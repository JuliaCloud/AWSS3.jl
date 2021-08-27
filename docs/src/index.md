```@meta
CurrentModule = AWSS3
```

# AWSS3.jl

AWSS3.jl is a Julia package for interacting with key-value data storage services [AWS S3](https://aws.amazon.com/s3/)
and [min.io](https://min.io/).  It operates through HTTP calls to a REST API service.  It is based
on the package [AWS.jl](https://github.com/JuliaCloud/AWS.jl) which provides a direct wrapper to
low-level API calls but provides a great deal of additional convenient functionality.


## Quick Start
```julia
using AWSS3
using AWS # for `global_aws_config`

aws = global_aws_config(; region="us-east-2") # pass keyword arguments to change defaults

s3_create_bucket(aws, "my.bucket")
s3_enable_versioning(aws, "my.bucket")

s3_put(aws, "my.bucket", "key", "Hello!")
println(s3_get(aws, "my.bucket", "key"))  # prints "Hello!"
println(s3_get(aws, "my.bucket", "key", byte_range=1:2))  # prints only "He"
```

## `S3Path`
This package provides the `S3Path` object which implements the
[FilePathsBase](https://github.com/rofinn/FilePathsBase.jl) interface, thus providing a
filesystem-like abstraction for interacting with S3.  In particular, this allows for interacting
with S3 using the [filesystem interface](https://docs.julialang.org/en/v1/base/file/) provided by
Julia's `Base`.  This makes it possible to (mostly) write code which works the same way for S3 as it
does for the local filesystem.

```julia
julia> using AWSS3, AWS, FilePathsBase;

# global_aws_config() is also the default if no `config` argument is passed
julia> p = S3Path("s3://bucket-name/dir1/", config=global_aws_config());

julia> readdir(p)
1-element Vector{SubString{String}}:
 "demo.txt"

julia> file = joinpath(p, "demo.txt")
p"s3://bucket-name/dir1/demo.txt"

julia> stat(file)
Status(
  device = 0,
  inode = 0,
  mode = -rw-rw-rw-,
  nlink = 0,
  uid = 1000 (username),
  gid = 1000 (username),
  rdev = 0,
  size = 34 (34.0),
  blksize = 4096 (4.0K),
  blocks = 1,
  mtime = 2021-01-30T18:53:02,
  ctime = 2021-01-30T18:53:02,
)

julia> String(read(file))  # fetch the file into memory
"this is a file for testing S3Path\n"

julia> String(read(file, byte_range=1:4))  # fetch a specific byte range of the file
"this"

julia> rm(file)  # delete the file
UInt8[]
```

!!! warning

    S3 is a pure [key-value store](https://en.wikipedia.org/wiki/Key%E2%80%93value_database),
    **NOT** a filesystem.  Therefore, though S3 has, over time, gained features which oftne mimic a
    filesystem interface, in some cases it can behave very differently.  In particular "empty
    directories" are, in actuality, 0-byte files and can have some unexpected behavior, e.g. there
    is no `stat(dir)` like in a true filesystem.

## Min.io
Min.io is fully compatible with the S3 API and therefore this package can be used to interact with
it.  To use Min.io requires a dedicated AWS configuration object, see the
[Minio.jl](https://gitlab.com/ExpandingMan/Minio.jl) package.  This package also contains some
convenience functions for easily setting up a server for experimentation and testing with the
min.io/S3 interface.
