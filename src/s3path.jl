struct S3Path <: AbstractPath
    segments::Tuple{Vararg{String}}
    root::String
    drive::String
    dir::Bool
    config::AWSConfig
end

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
    root = ""
    path = ()
    dir = true

    @assert startswith(str, "s3://")
    tokenized = split(str, "/")
    bucket = strip(tokenized[3], '/')
    drive = "s3://$bucket"

    if length(tokenized) > 3
        root = "/"
        # If the last tokenized element is an empty string then we've parsed a directory
        dir = isempty(last(tokenized))
        path = tuple(filter!(!isempty, tokenized[4:end])...)
    end

    return S3Path(path, root, drive, dir, config)
end

Base.print(io::IO, fp::S3Path) = print(io, fp.anchor * fp.key)

function Base.:(==)(a::S3Path, b::S3Path)
    return a.segments == b.segments &&
        a.root == b.root &&
        a.drive == b.drive
end

function Base.getproperty(fp::S3Path, attr::Symbol)
    if isdefined(fp, attr)
        return getfield(fp, attr)
    elseif attr === :anchor
        return fp.drive * fp.root
    elseif attr === :separator
        return "/"
    elseif attr === :bucket
        return split(fp.drive, "//")[2]
    elseif attr === :key
        return fp.dir ? join(fp.segments, '/') * "/" : join(fp.segments, '/')
    else
        # Call getfield even though we know it'll error
        # so the message is consistent.
        return getfield(fp, attr)
    end
end

# We need to special case join and parents so that we propagate
# directories correctly
function Base.join(prefix::S3Path, pieces::AbstractString...)
    segments = String[prefix.segments...]
    dir = endswith(last(pieces), "/")

    for p in pieces
        push!(segments, filter!(!isempty, split(p, "/"))...)
    end

    return S3Path(
        tuple(segments...),
        prefix.root,
        prefix.drive,
        dir,
        prefix.config,
    )
end

function parents(fp::T) where {T <: AbstractPath}
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
Base.isfile(fp::S3Path) = !fp.dir && exists(fp)
function FilePathsBase.isdir(fp::S3Path)
    # Note: objects "s3://bucket/a" and "s3://bucket/a/b" can co-exist. If both of these
    # objects exist listing the keys for "s3://bucket/a" returns ["s3://bucket/a"] while
    # "s3://bucket/a/" returns ["s3://bucket/a/b"].
    if isempty(fp.segments)
        key = ""
    elseif fp.dir
        key = fp.key
    else
        return false
    end

    objects = s3_list_objects(fp.config, fp.bucket, key; max_items=1)
    return !isempty(objects)
end

Base.real(fp::S3Path) = fp
function FilePathsBase.stat(fp::S3Path)
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

# TODO: FilePathsBase should default to calling stat?
FilePathsBase.lstat(fp::S3Path) = stat(fp)
FilePathsBase.mode(fp::S3Path) = stat(fp).mode
Base.size(fp::S3Path) = stat(fp).size
FilePathsBase.created(fp::S3Path) = stat(fp).ctime
FilePathsBase.modified(fp::S3Path) = stat(fp).mtime
Base.islink(fp::S3Path) = false
Base.issocket(fp::S3Path) = false
Base.isfifo(fp::S3Path) = false
Base.ischardev(fp::S3Path) = false
Base.isblockdev(fp::S3Path) = false
Base.ismount(fp::S3Path) = false
# Need API for accessing object ACL permissions for this to work
FilePathsBase.isexecutable(fp::S3Path) = false
Base.isreadable(fp::S3Path) = true
Base.iswritable(fp::S3Path) = true

function Base.mkdir(fp::S3Path; recursive=false, exist_ok=false)
    fp.dir || throw(ArgumentError("S3Path folders must end with '/': $fp"))

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

function Base.readdir(fp::S3Path)
    if isdir(fp)
        k = fp.key
        # Only list the files and "dirs" within this S3 "dir"
        objects = s3_list_objects(fp.config, fp.bucket, k; delimiter="")

        # Only list the basename and not the full key
        basenames = Set(s3_get_name(k, string(o["Key"])) for o in objects)

        # Lexographically sort the results
        return sort!(filter!(!isempty, collect(basenames)))
    else
        throw(ArgumentError("\"$fp\" is not a directory"))
    end
end

Base.read(fp::S3Path) = s3_get(fp.config, fp.bucket, fp.key)
Base.read(fp::S3Path, ::Type{String}) = String(read(fp))
function Base.write(fp::S3Path, content::Union{String, Vector{UInt8}})
    s3_put(fp.config, fp.bucket, fp.key, content)
end

function FilePathsBase.mktmpdir(parent::S3Path)
    fp = parent / string(uuid4(), "/")
    mkdir(fp)
    return fp
end

# Given a full key return just the file or directory name w/o the prefix or suffix keys
# (e.g., s3_get_name("my/common/prefix/", "my/common/prefix/to/some/file")) -> "to/"
function s3_get_name(prefix::String, s::String)
    subkey = lstrip(replace(s, prefix => ""), '/')
    tokenized = split(subkey, "/"; limit=2)
    return length(tokenized) == 2 ? first(tokenized) * "/" : first(tokenized)
end
