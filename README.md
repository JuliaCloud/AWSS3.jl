# AWSS3

AWS S3 Interface for Julia

[![CI](https://github.com/JuliaCloud/AWSS3.jl/actions/workflows/CI.yaml/badge.svg?branch=master)](https://github.com/JuliaCloud/AWSS3.jl/actions/workflows/CI.yaml?query=branch%3Amaster)
[![Code Style: Blue](https://img.shields.io/badge/code%20style-blue-4495d1.svg)](https://github.com/invenia/BlueStyle)
[![ColPrac: Contributor's Guide on Collaborative Practices for Community Packages](https://img.shields.io/badge/ColPrac-Contributor's%20Guide-blueviolet)](https://github.com/SciML/ColPrac)

**Installation**: at the Julia REPL, `using Pkg; Pkg.add("AWSS3")`

**Documentation**: [![][docs-stable-img]][docs-stable-url] [![][docs-latest-img]][docs-latest-url]

[docs-latest-img]: https://img.shields.io/badge/docs-latest-blue.svg
[docs-latest-url]: http://juliacloud.github.io/AWSS3.jl/dev/

[docs-stable-img]: https://img.shields.io/badge/docs-stable-blue.svg
[docs-stable-url]: http://juliacloud.github.io/AWSS3.jl/stable/

## Example
```julia
using AWSS3
using AWS # for `global_aws_config`

aws = global_aws_config(; region="us-east-2") # pass keyword arguments to change defaults

s3_create_bucket(aws, "my.bucket")

# if the config is omitted it will try to infer it as usual from AWS.jl
s3_delete_bucket("my.bucket")

p = S3Path("s3://my.bucket/test1.txt")  # provides an filesystem-like interface
write(p, "some data")

read(p, byte_range=1:4)  # returns b"some"

response = write(p, "other data"; returns=:response) # returns the raw `AWS.Response` on writing to S3
parsed_response = write(p, "other data"; returns=:parsed) # returns the parsed `AWS.Response` (default)
versioned_path = write(p, "other data"; returns=:path) # returns the `S3Path` written to S3, including the version ID
```

