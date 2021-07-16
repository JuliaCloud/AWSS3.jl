struct S3Path{A<:AbstractAWSConfig} <: AbstractPath
    segments::Tuple{Vararg{String}}
    root::String
    drive::String
    isdirectory::Bool
    config::A
    version::S3PathVersion
end

# constructor that converts but does not require type parameter
function S3Path(segments, root::AbstractString, drive::AbstractString, isdirectory::Bool,
                config::AbstractAWSConfig, version::S3PathVersion=nothing)
    S3Path{typeof(config)}(segments, root, drive, isdirectory, config, version)
end

"""
    S3Path()
    S3Path(str; config::AbstractAWSConfig=aws_config(), version=nothing)

Construct a new AWS S3 path type which should be of the form
"s3://<bucket>/prefix/to/my/object".

NOTES:

- Directories are required to have a trailing "/" due to how S3
  distinguishes files from folders, as internally they're just
  keys to objects.
- Objects p"s3://bucket/a" and p"s3://bucket/a/b" can co-exist.
  If both of these objects exist listing the keys for p"s3://bucket/a" returns
  [p"s3://bucket/a"] while p"s3://bucket/a/" returns [p"s3://bucket/a/b"].
- The drive property will return "s3://<bucket>"
- On top of the standard path properties (e.g., `segments`, `root`, `drive`,
  `separator`), `S3Path`s also support `bucket` and `key` properties for your
  convenience.
- If `version` argument is `nothing`, will return latest version of object.
"""
function S3Path()
    config = global_aws_config()
    account_id = aws_account_number(config)
    region = config.region

    return S3Path(
        (),
        "/",
        "s3://$account_id-$region",
        true,
        config,
        nothing,
    )
end
# below definition needed by FilePathsBase
S3Path{A}() where {A<:AbstractAWSConfig} = S3Path()

function S3Path(
    bucket::AbstractString,
    key::AbstractString;
    isdirectory::Bool=false,
    config::AbstractAWSConfig=global_aws_config(),
    version::S3PathVersion=nothing,
)
    return S3Path(
        Tuple(filter!(!isempty, split(key, "/"))),
        "/",
        strip(startswith(bucket, "s3://") ? bucket : "s3://$bucket", '/'),
        isdirectory,
        config,
        version,
    )
end

function S3Path(
    bucket::AbstractString,
    key::AbstractPath;
    isdirectory::Bool=false,
    config::AbstractAWSConfig=global_aws_config(),
    version::S3PathVersion=nothing,
)
    return S3Path(
        key.segments,
        "/",
        normalize_bucket_name(bucket),
        isdirectory,
        config,
        version,
    )
end

# To avoid a breaking change.
function S3Path(str::AbstractString; config::AbstractAWSConfig=global_aws_config(), version=nothing)
    result = tryparse(S3Path, str; config=config, version=version)
    result !== nothing || throw(ArgumentError("Invalid s3 path string: $str"))
    return result
end

# if config=nothing, will not try to talk to AWS until after string is confirmed to be an s3 path
function Base.tryparse(::Type{<:S3Path}, str::AbstractString; config::Union{Nothing,AbstractAWSConfig}=nothing, version::S3PathVersion=nothing)
    str = String(str)
    startswith(str, "s3://") || return nothing
    # we do this here so that the `@p_str` macro only tries to call AWS if it actually has an S3 path
    (config ≡ nothing) && (config = global_aws_config())
    root = ""
    path = ()
    isdirectory = true

    tokenized = split(str, "/")
    bucket = strip(tokenized[3], '/')
    drive = "s3://$bucket"

    if length(tokenized) > 3
        root = "/"
        # If the last tokenized element is an empty string then we've parsed a directory
        isdirectory = isempty(last(tokenized))
        path = Tuple(filter!(!isempty, tokenized[4:end]))
    end

    return S3Path(path, root, drive, isdirectory, config, version)
end

function normalize_bucket_name(bucket)
    return strip(startswith(bucket, "s3://") ? bucket : "s3://$bucket", '/')
end

Base.print(io::IO, fp::S3Path) = print(io, fp.anchor * fp.key)

function Base.:(==)(a::S3Path, b::S3Path)
    return a.segments == b.segments &&
        a.root == b.root &&
        a.drive == b.drive &&
        a.isdirectory == b.isdirectory
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
        prefix.config,
        nothing, # Version is per-object, so would not propogate from the prefix
    )
end

function FilePathsBase.parents(fp::S3Path)
    if hasparent(fp)
        return map(0:length(fp.segments)-1) do i
            S3Path(fp.segments[1:i], fp.root, fp.drive, true, fp.config)
        end
    elseif fp.segments == tuple(".") || isempty(fp.segments)
        return [fp]
    else
        return [isempty(fp.root) ? Path(fp, tuple(".")) : Path(fp, ())]
    end
end

FilePathsBase.exists(fp::S3Path) = s3_exists(fp.config, fp.bucket, fp.key; version=fp.version)
Base.isfile(fp::S3Path) = !fp.isdirectory && exists(fp)
function Base.isdir(fp::S3Path)
    if isempty(fp.segments)
        key = ""
    elseif fp.isdirectory
        key = fp.key
    else
        return false
    end

    objects = s3_list_objects(fp.config, fp.bucket, key; max_items=1)

    # `objects` is a `Channel`, so we call iterate to see if there are any objects that
    # match our directory key.
    # NOTE: `iterate` should handle waiting on a value to become available or return `nothing`
    # if the channel is closed without inserting anything.
    return iterate(objects) !== nothing
end

function Base.stat(fp::S3Path)
    # Currently AWSS3 would require a s3_get_acl call to fetch
    # ownership and permission settings
    m = Mode(user=(READ + WRITE), group=(READ + WRITE), other=(READ + WRITE))
    u = FilePathsBase.User()
    g = FilePathsBase.Group()
    blksize = 4096
    blocks = 0
    s = 0
    last_modified = DateTime(0)

    if exists(fp)
        resp = s3_get_meta(fp.config, fp.bucket, fp.key; version=fp.version)
        # Example: "Thu, 03 Jan 2019 21:09:17 GMT"
        last_modified = DateTime(
            resp["Last-Modified"][1:end-4],
            dateformat"e, d u Y H:M:S",
        )
        s = parse(Int, resp["Content-Length"])
        blocks = ceil(Int, s / 4096)
    end

    return Status(0, 0, m, 0, u, g, 0, s, blksize, blocks, last_modified, last_modified)
end

# Need API for accessing object ACL permissions for this to work
FilePathsBase.isexecutable(fp::S3Path) = false
Base.isreadable(fp::S3Path) = true
Base.iswritable(fp::S3Path) = true
Base.ismount(fp::S3Path) = false

function Base.mkdir(fp::S3Path; recursive=false, exist_ok=false)
    fp.isdirectory || throw(ArgumentError("S3Path folders must end with '/': $fp"))

    if exists(fp)
        !exist_ok && error("$fp already exists.")
    else
        if hasparent(fp) && !exists(parent(fp))
            if recursive
                mkdir(parent(fp); recursive=recursive, exist_ok=exist_ok)
            else
                error(
                    "The parent of $fp does not exist. " *
                    "Pass `recursive=true` to create it."
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
            error("S3 path $object is not empty. Use `recursive=true` to delete.")
        end
    end

    @debug "delete: $fp"
    s3_delete(fp.config, fp.bucket, fp.key; version=fp.version)
end

# We need to special case sync with S3Paths because of how directories
# are handled again.
# NOTE: This method signature only makes sense with FilePathsBase 0.6.2, but
# 1) It'd be odd for other packages to restrict FilePathsBase to a patch release
# 2) Seems cleaner to have it fallback and error rather than having
# slightly inconsistent handling of edge cases between the two versions.
function FilePathsBase.sync(f::Function, src::AbstractPath, dst::S3Path; delete=false, overwrite=true)
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
            isdir(dst) || throw(ArgumentError("Unable to sync directory $src to non-directory $dst"))
            # Create an index of all of the source files
            src_paths = collect(walkpath(src))
            index = Dict(
                Tuple(setdiff(p.segments, src.segments)) => i
                for (i, p) in enumerate(src_paths)
            )
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

    rm_key = s -> chop(s, head=chop_head, tail=0)

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

    _retrieve_prefixes!(results, get(response, "CommonPrefixes", nothing), "Prefix", key_length)
    _retrieve_prefixes!(results, get(response, "Contents", nothing), "Key", key_length)

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
                S3.list_objects_v2(fp.bucket, params; aws_config=fp.config)
            catch e
                @delay_retry if ecode(e) in ["NoSuchBucket"] end
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

function Base.read(fp::S3Path; byte_range=nothing)
    return Vector{UInt8}(s3_get(fp.config, fp.bucket, fp.key; raw=true, byte_range=byte_range, version=fp.version))
end

Base.write(fp::S3Path, content::String; kwargs...) = Base.write(fp, Vector{UInt8}(content); kwargs...)

function Base.write(fp::S3Path, content::Vector{UInt8}; part_size_mb=50, multipart::Bool=false, other_kwargs...)
    # avoid HTTPClientError('An HTTP Client raised an unhandled exception: string longer than 2147483647 bytes')
    MAX_HTTP_BYTES = 2147483647
    isnothing(fp.version) || throw(ArgumentError("Can't write to a specific object version ($(fp.version))"))
    if !multipart || length(content) < MAX_HTTP_BYTES
        return s3_put(fp.config, fp.bucket, fp.key, content)
    else
        io = IOBuffer(content)
        return s3_multipart_upload(fp.config, fp.bucket, fp.key, io, part_size_mb=part_size_mb; other_kwargs...)
    end
end

function FilePathsBase.mktmpdir(parent::S3Path)
    fp = parent / string(uuid4(), "/")
    return mkdir(fp)
end
