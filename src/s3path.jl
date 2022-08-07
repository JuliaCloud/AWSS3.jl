# Amazon states that version IDs are UTF-8 encoded, URL-ready, opaque strings no longer than
# 1024 bytes
# – https://docs.aws.amazon.com/AmazonS3/latest/userguide/versioning-workflows.html#version-ids
#
# In practise version IDs seems to be much narrower in scope:
# https://github.com/JuliaCloud/AWSS3.jl/pull/199#issuecomment-901995960
#
# An unversioned object can be accessed using the "null" version ID. For details see:
# https://github.com/JuliaCloud/AWSS3.jl/issues/241
const VERSION_ID_REGEX = r"^(?:[0-9a-zA-Z\._]{32}|null)$"

struct S3Path{A<:AbstractS3PathConfig} <: AbstractPath
    segments::Tuple{Vararg{String}}
    root::String
    drive::String
    isdirectory::Bool
    version::Union{String,Nothing}
    config::A

    # Inner constructor performs no data checking and is only meant for direct use by
    # deserialization.
    function S3Path{A}(
        segments, root, drive, isdirectory, version, config::A
    ) where {A<:AbstractS3PathConfig}
        return new(segments, root, drive, isdirectory, version, config)
    end
end

function S3Path(
    segments, root, drive, isdirectory, version::AbstractString, config::A
) where {A<:AbstractS3PathConfig}
    # Validate the `version` string provided is valid. Having this check during construction
    # allows us to fail early instead of having to wait to make an API call to fail.
    if !occursin(VERSION_ID_REGEX, version)
        throw(ArgumentError("`version` string is invalid: $(repr(version))"))
    end

    return S3Path{A}(segments, root, drive, isdirectory, version, config)
end

function S3Path(
    segments, root, drive, isdirectory, version::Nothing, config::A
) where {A<:AbstractS3PathConfig}
    return S3Path{A}(segments, root, drive, isdirectory, version, config)
end

"""
    S3Path()
    S3Path(str; version::$(AbstractS3Version)=nothing, config::$(AbstractS3PathConfig)=nothing)

Construct a new AWS S3 path type which should be of the form
`"s3://<bucket>/prefix/to/my/object"`.

NOTES:

- Directories are required to have a trailing `/` due to how S3
  distinguishes files from folders, as internally they're just
  keys to objects.
- Objects `p"s3://bucket/a"` and `p"s3://bucket/a/b"` can co-exist.
  If both of these objects exist listing the keys for `p"s3://bucket/a"` returns
  `[p"s3://bucket/a"]` while `p"s3://bucket/a/"` returns `[p"s3://bucket/a/b"]`.
- The drive property will return `"s3://<bucket>"`
- On top of the standard path properties (e.g., `segments`, `root`, `drive`,
  `separator`), `S3Path`s also support `bucket` and `key` properties for your
  convenience.
- If `version` argument is `nothing`, will return latest version of object. Version
  can be provided via either kwarg `version` or as suffix `"?versionId=<object_version>"`
  of `str`, e.g., `"s3://<bucket>/prefix/to/my/object?versionId=<object_version>"`.
- If `config` is left at its default value of `nothing`, then the
  latest `global_aws_config()` will be used in any operations involving the
  path. To "freeze" the config at construction time, explicitly pass an
  `AbstractAWSConfig` to the `config` keyword argument.
"""
S3Path() = S3Path((), "/", "", true, nothing, nothing)

S3Path(path::S3Path) = path

# below definition needed by FilePathsBase
S3Path{A}() where {A<:AbstractS3PathConfig} = S3Path()

function S3Path(
    bucket::AbstractString,
    key::AbstractString;
    isdirectory::Bool=endswith(key, "/"),
    version::AbstractS3Version=nothing,
    config::AbstractS3PathConfig=nothing,
)
    return S3Path(
        Tuple(filter!(!isempty, split(key, "/"))),
        "/",
        strip(startswith(bucket, "s3://") ? bucket : "s3://$bucket", '/'),
        isdirectory,
        version,
        config,
    )
end

function S3Path(
    bucket::AbstractString,
    key::AbstractPath;
    isdirectory::Bool=false,
    version::AbstractS3Version=nothing,
    config::AbstractS3PathConfig=nothing,
)
    return S3Path(
        key.segments, "/", normalize_bucket_name(bucket), isdirectory, version, config
    )
end

# To avoid a breaking change.
function S3Path(
    str::AbstractString;
    isdirectory::Union{Bool,Nothing}=nothing,
    version::AbstractS3Version=nothing,
    config::AbstractS3PathConfig=nothing,
)
    result = tryparse(S3Path, str; config=config)
    result !== nothing || throw(ArgumentError("Invalid s3 path string: $str"))
    ver = if version !== nothing
        if result.version !== nothing && result.version != version
            throw(ArgumentError("Conflicting object versions in `version` and `str`"))
        end
        version
    else
        result.version
    end

    # Replace the parsed isdirectory field with an explicit passed in argument.
    is_dir = isdirectory === nothing ? result.isdirectory : isdirectory

    # Warning: We need to use the full constructor because reconstructing with the bucket
    # and key results in inconsistent `root` fields.
    return S3Path(result.segments, result.root, result.drive, is_dir, ver, result.config)
end

# Parses a URI in the S3 scheme as an S3Path combining the conventions documented in:
# - https://docs.aws.amazon.com/AmazonS3/latest/userguide/access-bucket-intro.html#accessing-a-bucket-using-S3-format
# - https://docs.aws.amazon.com/AmazonS3/latest/userguide/RetMetaOfObjVersion.html
function Base.tryparse(
    ::Type{<:S3Path}, str::AbstractString; config::AbstractS3PathConfig=nothing
)
    uri = URI(str)
    uri.scheme == "s3" || return nothing

    drive = "s3://$(uri.host)"
    root = isempty(uri.path) ? "" : "/"
    isdirectory = isempty(uri.path) || endswith(uri.path, '/')
    path = Tuple(split(uri.path, '/'; keepempty=false))
    version = get(queryparams(uri), "versionId", nothing)

    return S3Path(path, root, drive, isdirectory, version, config)
end

function normalize_bucket_name(bucket)
    return strip(startswith(bucket, "s3://") ? bucket : "s3://$bucket", '/')
end

function Base.print(io::IO, fp::S3Path)
    print(io, fp.anchor, fp.key)
    fp.version !== nothing && print(io, "?versionId=", fp.version)
    return nothing
end

function Base.:(==)(a::S3Path, b::S3Path)
    return (
        a.segments == b.segments &&
        a.root == b.root &&
        a.drive == b.drive &&
        a.isdirectory == b.isdirectory &&
        a.version == b.version
    )
end

function Base.getproperty(fp::S3Path, attr::Symbol)
    if attr === :anchor
        return fp.drive * fp.root
    elseif attr === :separator
        return "/"
    elseif attr === :bucket
        return split(fp.drive, "//")[2]
    elseif attr === :key
        if isempty(fp.segments)
            return ""
        end

        return join(fp.segments, '/') * (fp.isdirectory ? "/" : "")
    else
        return getfield(fp, attr)
    end
end

# We need to special case join and parents so that we propagate
# directories correctly (see type docstring for details)
function FilePathsBase.join(prefix::S3Path, pieces::AbstractString...)
    isempty(pieces) && return prefix

    segments = String[prefix.segments...]
    isdirectory = endswith(last(pieces), "/")

    for p in pieces
        append!(segments, filter!(!isempty, split(p, "/")))
    end

    return S3Path(
        tuple(segments...),
        "/",
        prefix.drive,
        isdirectory,
        nothing, # Version is per-object, so we should not propagate it from the prefix
        prefix.config,
    )
end

function FilePathsBase.parents(fp::S3Path)
    if hasparent(fp)
        return map(0:(length(fp.segments) - 1)) do i
            S3Path(fp.segments[1:i], fp.root, fp.drive, true, nothing, fp.config)
        end
    elseif fp.segments == tuple(".") || isempty(fp.segments)
        return [fp]
    else
        return [isempty(fp.root) ? Path(fp, tuple(".")) : Path(fp, ())]
    end
end

"""
    get_config(fp::S3Path)

Returns the AWS configuration used by the path `fp`.  This can be stored within the path itself, but if not
it will be fetched with `AWS.global_aws_config()`.
"""
get_config(fp::S3Path) = @something(fp.config, global_aws_config())

function FilePathsBase.exists(fp::S3Path)
    return s3_exists(get_config(fp), fp.bucket, fp.key; version=fp.version)
end

Base.isfile(fp::S3Path) = !fp.isdirectory && exists(fp)
function Base.isdir(fp::S3Path)
    fp.isdirectory || return false
    if isempty(fp.segments)  # special handling of buckets themselves
        try
            @mock S3.list_objects_v2(
                fp.bucket, Dict("max-keys" => "0"); aws_config=get_config(fp)
            )
            return true
        catch e
            if ecode(e) == "NoSuchBucket"
                return false
            else
                rethrow()
            end
        end
    else
        exists(fp)
    end
end

function FilePathsBase.walkpath(fp::S3Path; kwargs...)
    # Select objects with that prefix
    objects = s3_list_objects(get_config(fp), fp.bucket, fp.key; delimiter="")
    root = joinpath(fp, "/")

    # Construct a new Channel using a recursive internal `_walkpath!` function
    return Channel(; ctype=typeof(fp), csize=128) do chnl
        _walkpath!(root, root, Iterators.Stateful(objects), chnl; kwargs...)
    end
end

function _walkpath!(
    root::S3Path, prefix::S3Path, objects, chnl; topdown=true, onerror=throw, kwargs...
)
    @assert root.isdirectory
    @assert prefix.isdirectory

    while true
        try
            # Start by inspecting the next element
            obj = Base.peek(objects)

            # Early exit condition if we've exhausted the iterator or just the current prefix.
            obj === nothing && return nothing

            # Extract the non-root part of the key
            k = chop(obj["Key"]; head=length(root.key), tail=0)

            fp = joinpath(root, k)
            _parents = parents(fp)

            # If the filepath matches our prefix then pop it off and continue
            # Cause we would have already processed it before recursing
            child = if prefix.segments == fp.segments
                popfirst!(objects)
                continue
                # If the filpath is a direct descendant of our prefix then check if it
                # is a directory too
            elseif last(_parents) == prefix
                popfirst!(objects)
                # If our current path is a prefix for the next path then we can assume that
                # the current path should be a directory without needing to call `isdir`
                next = Base.peek(objects)
                is_dir =
                    (next !== nothing && startswith(next["Key"], fp.key)) || isdir(fp)
                # Reconstruct our next object and explicitly specify whether it is a
                # directory.
                S3Path(
                    fp.bucket,
                    fp.key;
                    isdirectory=is_dir,
                    config=fp.config,
                    version=fp.version,
                )
                # If our filepath is a distance descendant of the prefix then start filling in
                # the intermediate paths
            elseif prefix in _parents
                i = findfirst(==(prefix), _parents)
                _parents[i + 1]
                # Otherwise we've established that the current filepath isn't a descendant
                # of the prefix and we should exit
            else
                return nothing
            end

            # If we aren't dealing with the root and we're doing topdown iteration then
            # insert the child into the results channel
            !isempty(k) && topdown && put!(chnl, child)

            # Apply our recursive call for the children as necessary
            # NOTE: We're relying on the `isdirectory` field rather than calling `isdir`
            # which will call out to AWS as a fallback.
            if child.isdirectory
                _walkpath!(
                    root, child, objects, chnl; topdown=topdown, onerror=onerror, kwargs...
                )
            end

            # If we aren't dealing with the root and we're doing bottom up iteration then
            # insert the child ion the result channel here
            !isempty(k) && !topdown && put!(chnl, child)
        catch e
            isa(e, Base.IOError) ? onerror(e) : rethrow()
        end
    end
end

"""
    stat(fp::S3Path)

Return the status struct for the S3 path analogously to `stat` for local directories.

Note that this cannot be used on a directory.  This is because S3 is a pure key-value store and internally does
not have a concept of directories.  In some cases, a directory may actually be an empty file, in which case
you should use `s3_get_meta`.
"""
function Base.stat(fp::S3Path)
    # Currently AWSS3 would require a s3_get_acl call to fetch
    # ownership and permission settings
    m = Mode(; user=(READ + WRITE), group=(READ + WRITE), other=(READ + WRITE))
    u = FilePathsBase.User()
    g = FilePathsBase.Group()
    blksize = 4096
    blocks = 0
    s = 0
    last_modified = DateTime(0)

    if isfile(fp)
        resp = s3_get_meta(get_config(fp), fp.bucket, fp.key; version=fp.version)

        # Example: "Thu, 03 Jan 2019 21:09:17 GMT"
        last_modified = DateTime(
            get_robust_case(resp, "Last-Modified")[1:(end - 4)], dateformat"e, d u Y H:M:S"
        )
        s = parse(Int, get_robust_case(resp, "Content-Length"))
        blocks = ceil(Int, s / 4096)
    end

    return Status(0, 0, m, 0, u, g, 0, s, blksize, blocks, last_modified, last_modified)
end

"""
    diskusage(fp::S3Path)

Compute the *total* size of all contents of a directory.  Note that there is no direct functionality
for this in the S3 API so it may be slow.
"""
function FilePathsBase.diskusage(fp::S3Path)
    return if isfile(fp)
        stat(fp).size
    else
        s3_directory_stat(get_config(fp), fp.bucket, fp.key)[1]
    end
end

"""
    lastmodified(fp::S3Path)

Returns a `DateTime` corresponding to the latest time at which the object (or, in the case of a
directory, any contained object) was modified.
"""
function lastmodified(fp::S3Path)
    return if isfile(fp)
        stat(fp).mtime
    else
        s3_directory_stat(get_config(fp), fp.bucket, fp.key)[2]
    end
end

# Need API for accessing object ACL permissions for this to work
FilePathsBase.isexecutable(fp::S3Path) = false
Base.isreadable(fp::S3Path) = true
Base.iswritable(fp::S3Path) = true
Base.ismount(fp::S3Path) = false

"""
    mkdir(fp::S3Path; recursive=false, exist_ok=false)

Create an empty directory at the S3 path `fp`.  If `recursive`, this will create any previously non-existent
directories which would contain `fp`.  An error will be thrown if an object exists at `fp` unless `exist_ok`.

Note that empty directories in S3 are actually 0-byte objects with the naming convention of a directory.

This will *not* create a bucket.
"""
function Base.mkdir(fp::S3Path; recursive=false, exist_ok=false)
    fp.isdirectory || throw(ArgumentError("S3Path folders must end with '/': $fp"))

    if exists(fp)
        !exist_ok && error("$fp already exists.")
    else
        if hasparent(fp) && !exists(parent(fp))
            if recursive
                # don't try to create buckets this way, minio at least really doesn't like it
                isempty(parent(fp).segments) ||
                    mkdir(parent(fp); recursive=recursive, exist_ok=exist_ok)
            else
                error(
                    "The parent of $fp does not exist. " *
                    "Pass `recursive=true` to create it.",
                )
            end
        end

        write(fp, "")
    end

    return fp
end

function Base.rm(fp::S3Path; recursive=false, kwargs...)
    if isdir(fp)
        files = readpath(fp)

        if recursive
            for f in files
                rm(f; recursive=recursive, kwargs...)
            end
        elseif length(files) > 0
            error("S3 path $fp is not empty. Use `recursive=true` to delete.")
        end
    end

    @debug "delete: $fp"
    return s3_delete(get_config(fp), fp.bucket, fp.key; version=fp.version)
end

# We need to special case sync with S3Paths because of how directories
# are handled again.
# NOTE: This method signature only makes sense with FilePathsBase 0.6.2, but
# 1) It'd be odd for other packages to restrict FilePathsBase to a patch release
# 2) Seems cleaner to have it fallback and error rather than having
# slightly inconsistent handling of edge cases between the two versions.
function FilePathsBase.sync(
    f::Function, src::AbstractPath, dst::S3Path; delete=false, overwrite=true
)
    # Throw an error if the source path doesn't exist at all
    exists(src) || throw(ArgumentError("Unable to sync from non-existent $src"))

    # If the top level source is just a file then try to just sync that
    # without calling walkpath
    if isfile(src)
        # If the destination exists then we should make sure it is a file and check
        # if we should copy the source over.
        if exists(dst)
            isfile(dst) || throw(ArgumentError("Unable to sync file $src to non-file $dst"))
            if overwrite && f(src, dst)
                cp(src, dst; force=true)
            end
        else
            cp(src, dst)
        end
    elseif isdir(src)
        if exists(dst)
            isdir(dst) ||
                throw(ArgumentError("Unable to sync directory $src to non-directory $dst"))
            # Create an index of all of the source files
            src_paths = collect(walkpath(src))

            #! format: off
            # https://github.com/domluna/JuliaFormatter.jl/issues/458
            index = Dict(
                Tuple(setdiff(p.segments, src.segments)) => i
                for (i, p) in enumerate(src_paths)
            )
            #! format: on

            for dst_path in walkpath(dst)
                k = Tuple(setdiff(dst_path.segments, dst.segments))

                if haskey(index, k)
                    src_path = src_paths[pop!(index, k)]
                    if overwrite && f(src_path, dst_path)
                        cp(src_path, dst_path; force=true)
                    end
                elseif delete
                    rm(dst_path; recursive=true)
                end
            end

            # Finally, copy over files that don't exist at the destination
            # But we need to iterate through it in a way that respects the original
            # walkpath order otherwise we may end up trying to copy a file before its parents.
            index_pairs = collect(pairs(index))
            index_pairs = index_pairs[sortperm(index_pairs; by=last)]
            for (seg, i) in index_pairs
                new_dst = S3Path(
                    tuple(dst.segments..., seg...),
                    dst.root,
                    dst.drive,
                    isdir(src_paths[i]),
                    nothing,
                    dst.config,
                )

                cp(src_paths[i], new_dst; force=true)
            end
        else
            cp(src, dst)
        end
    else
        throw(ArgumentError("$src is neither a file or directory."))
    end
end

# for some reason, sometimes we get back a `Pair`
# other times a `AbstractDict`.
function _pair_or_dict_get(p::Pair, k)
    first(p) == k || return nothing
    return last(p)
end
_pair_or_dict_get(d::AbstractDict, k) = get(d, k, nothing)

function _retrieve_prefixes!(results, objects, prefix_key, chop_head)
    objects === nothing && return nothing

    rm_key = s -> chop(s; head=chop_head, tail=0)

    for p in objects
        prefix = _pair_or_dict_get(p, prefix_key)

        if prefix !== nothing
            push!(results, rm_key(prefix))
        end
    end

    return nothing
end

function _readdir_add_results!(results, response, key_length)
    sizehint!(results, length(results) + parse(Int, response["KeyCount"]))

    common_prefixes = get(response, "CommonPrefixes", nothing)
    _retrieve_prefixes!(results, common_prefixes, "Prefix", key_length)

    contents = get(response, "Contents", nothing)
    _retrieve_prefixes!(results, contents, "Key", key_length)

    return get(response, "NextContinuationToken", nothing)
end

function Base.readdir(fp::S3Path; join=false, sort=true)
    if isdir(fp)
        k = fp.key
        key_length = length(k)
        results = String[]
        token = ""
        while token !== nothing
            response = @repeat 4 try
                params = Dict("delimiter" => "/", "prefix" => k)

                if !isempty(token)
                    params["continuation-token"] = token
                end
                parse(S3.list_objects_v2(fp.bucket, params; aws_config=get_config(fp)))
            catch e
                #! format: off
                @delay_retry if ecode(e) in ["NoSuchBucket"] end
                #! format: on
            end
            token = _readdir_add_results!(results, response, key_length)
        end

        # Filter out any empty object names which are valid in S3
        filter!(!isempty, results)

        # Sort results if sort=true
        sort && sort!(results)

        # Return results, possibly joined with the root path if join=true
        return join ? joinpath.(fp, results) : results
    else
        throw(ArgumentError("\"$fp\" is not a directory"))
    end
end

"""
    read(fp::S3Path; byte_range=nothing)

Fetch data from the S3 path as a `Vector{UInt8}`.  A subset of the object can be specified with
`byte_range` which should be a contiguous integer range, e.g. `1:4`.
"""
function Base.read(fp::S3Path; byte_range=nothing)
    return Vector{UInt8}(
        s3_get(
            get_config(fp),
            fp.bucket,
            fp.key;
            raw=true,
            byte_range=byte_range,
            version=fp.version,
        ),
    )
end

function Base.write(fp::S3Path, content::String; kwargs...)
    return Base.write(fp, Vector{UInt8}(content); kwargs...)
end

function Base.write(
    fp::S3Path,
    content::Vector{UInt8};
    part_size_mb=50,
    multipart::Bool=true,
    other_kwargs...,
)
    # avoid HTTPClientError('An HTTP Client raised an unhandled exception: string longer than 2147483647 bytes')
    MAX_HTTP_BYTES = 2147483647
    if fp.version !== nothing
        throw(ArgumentError("Can't write to a specific object version ($(fp.version))"))
    end

    if !multipart || length(content) < MAX_HTTP_BYTES
        return s3_put(get_config(fp), fp.bucket, fp.key, content)
    else
        io = IOBuffer(content)
        return s3_multipart_upload(
            get_config(fp), fp.bucket, fp.key, io, part_size_mb; other_kwargs...
        )
    end
end

function FilePathsBase.mktmpdir(parent::S3Path)
    fp = parent / string(uuid4(), "/")
    return mkdir(fp)
end

const S3PATH_ARROW_NAME = Symbol("JuliaLang.AWSS3.S3Path")
ArrowTypes.arrowname(::Type{<:S3Path}) = S3PATH_ARROW_NAME
ArrowTypes.ArrowType(::Type{<:S3Path}) = String
ArrowTypes.JuliaType(::Val{S3PATH_ARROW_NAME}, ::Any) = S3Path{Nothing}
ArrowTypes.fromarrow(::Type{<:S3Path}, uri_string) = S3Path(uri_string)

function ArrowTypes.toarrow(path::S3Path)
    if !isnothing(path.config)
        throw(ArgumentError("`S3Path` config must be `nothing` to serialize to Arrow"))
    end
    return string(path)
end
