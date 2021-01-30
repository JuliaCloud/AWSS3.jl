# AWSS3

AWS S3 Interface for Julia

![CI](https://github.com/JuliaCloud/AWSS3.jl/workflows/CI/badge.svg)
[![Code Style: Blue](https://img.shields.io/badge/code%20style-blue-4495d1.svg)](https://github.com/invenia/BlueStyle)
[![ColPrac: Contributor's Guide on Collaborative Practices for Community Packages](https://img.shields.io/badge/ColPrac-Contributor's%20Guide-blueviolet)](https://github.com/SciML/ColPrac)
[Documentation](https://juliacloud.github.io/AWSCore.jl/build/AWSS3.html)

```julia
using AWSS3
using AWS # for `global_aws_config`

aws = global_aws_config(; region="us-east-2") # pass keyword arguments to change defaults

s3_create_bucket(aws, "my.bucket")
s3_enable_versioning(aws, "my.bucket")

s3_put(aws, "my.bucket", "key", "Hello!")
println(s3_get(aws, "my.bucket", "key"))
```

## `S3Path`
This package exports the `S3Path` object.  This is an `AbstractPath` object as defined by
[FilePathsBase.jl](https://github.com/rofinn/FilePathsBase.jl), allowing users to use
Julia's `Base` [file system interface](https://docs.julialang.org/en/v1/base/file/) to
obtain information from S3 buckets.  See the below example.
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

julia> rm(file)  # delete the file
UInt8[]
```
