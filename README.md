# AWSS3

AWS S3 Interface for Julia

![CI](https://github.com/JuliaCloud/AWSS3.jl/workflows/CI/badge.svg)
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
```

## Testing

Some of the tests involve using a temporary AWS S3 bucket. For these tests to succeed you'll
need to set your current AWS profile to use a role which allows for `s3:*` access to the `arn:aws:s3:::ocaws.jl.test.*` resource.

If you do not have AWS access or lack the required permission you can use the
`AWSS3_TESTSETS` environmental variable to control which testsets run:

```bash
AWSS3_TESTSETS=MinIO julia --project -e 'using Pkg; Pkg.test()'
```