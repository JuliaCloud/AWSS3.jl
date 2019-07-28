struct S3Path <: AbstractPath
    segments::Tuple{Vararg{String}}
    root::String
    drive::String
    isdirectory::Bool
    config::AWSConfig
end

"""
    S3Path()
    S3Path(str; config::AWSConfig=aws_config())

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
"""
function S3Path()
    config = aws_config()
    account_id = aws_account_number(config)
    region = config[:region]

    return S3Path(
        (),
        "/",
        "s3://$account_id-$region",
        true,
        config,
    )
end

function S3Path(str::AbstractString; config::AWSConfig=aws_config())
    str = String(str)
    startswith(str, "s3://") || throw(ArgumentError("$str doesn't start with s://"))
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

    return S3Path(path, root, drive, isdirectory, config)
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
        return join(fp.segments, '/') * (fp.isdirectory ? "/" : "")
    else
        return getfield(fp, attr)
    end
end

# We need to special case join and parents so that we propagate
# directories correctly (see type docstring for details)
function Base.join(prefix::S3Path, pieces::AbstractString...)
    isempty(pieces) && return prefix

    segments = String[prefix.segments...]
    isdirectory = endswith(last(pieces), "/")

    for p in pieces
        append!(segments, filter!(!isempty, split(p, "/")))
    end

    return S3Path(
        tuple(segments...),
        prefix.root,
        prefix.drive,
        isdirectory,
        prefix.config,
    )
end

function FilePathsBase.parents(fp::S3Path)
    if hasparent(fp)
        return map(1:length(fp.segments)-1) do i
            S3Path(fp.segments[1:i], fp.root, fp.drive, true, fp.config)
        end
    elseif fp.segments == tuple(".") || isempty(fp.segments)
        return [fp]
    else
        return [isempty(fp.root) ? Path(fp, tuple(".")) : Path(fp, ())]
    end
end

FilePathsBase.ispathtype(::Type{S3Path}, str::AbstractString) = startswith(str, "s3://")
FilePathsBase.exists(fp::S3Path) = s3_exists(fp.config, fp.bucket, fp.key)
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
    return !isempty(objects)
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
        resp = s3_get_meta(fp.config, fp.bucket, fp.key)
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
    s3_delete(fp.config, fp.bucket, fp.key)
end

# We need to special case sync with S3Paths because of how directories
# are handled again.
function FilePathsBase.sync(src::AbstractPath, dst::S3Path; delete=false)
    # Create an index of all of the source files
    index = Dict(Tuple(setdiff(p.segments, src.segments)) => p for p in walkpath(src))

    if exists(dst)
        for p in walkpath(dst)
            k = Tuple(setdiff(p.segments, dst.segments))

            if haskey(index, k)
                if modified(index[k]) > modified(p)
                    cp(index[k], p; force=true)
                end

                delete!(index, k)
            elseif delete
                rm(p; recursive=true)
            end
        end

        # Finally, copy over files that don't exist at the destination
        for (seg, p) in index
            new_dst = S3Path(
                tuple(dst.segments..., seg...),
                dst.root,
                dst.drive,
                isdir(p),
                dst.config,
            )

            cp(p, new_dst; force=true)
        end
    else
        cp(src, dst)
    end
end

function Base.readdir(fp::S3Path)
    if isdir(fp)
        k = fp.key
        # Only list the files and "dirs" within this S3 "dir"
        objects = s3_list_objects(fp.config, fp.bucket, k; delimiter="")

        # Only list the basename and not the full key
        basenames = unique!([s3_get_name(k, string(o["Key"])) for o in objects])

        # Lexographically sort the results
        return sort!(filter!(!isempty, basenames))
    else
        throw(ArgumentError("\"$fp\" is not a directory"))
    end
end

Base.read(fp::S3Path) = s3_get(fp.config, fp.bucket, fp.key)

function Base.write(fp::S3Path, content::Union{String, Vector{UInt8}})
    s3_put(fp.config, fp.bucket, fp.key, content)
end

function FilePathsBase.mktmpdir(parent::S3Path)
    fp = parent / string(uuid4(), "/")
    return mkdir(fp)
end

# Given a full key return just the file or directory name w/o the prefix or suffix keys
# (e.g., s3_get_name("my/common/prefix/", "my/common/prefix/to/some/file")) -> "to/"
function s3_get_name(prefix::String, s::String)
    subkey = lstrip(replace(s, prefix => ""), '/')
    tokenized = split(subkey, "/"; limit=2)
    return length(tokenized) == 2 ? first(tokenized) * "/" : first(tokenized)
end
