# AWSS3

AWS S3 Interface for Julia

![CI](https://github.com/JuliaCloud/AWSS3.jl/workflows/CI/badge.svg)
[![Code Style: Blue](https://img.shields.io/badge/code%20style-blue-4495d1.svg)](https://github.com/invenia/BlueStyle)
[![ColPrac: Contributor's Guide on Collaborative Practices for Community Packages](https://img.shields.io/badge/ColPrac-Contributor's%20Guide-blueviolet)](https://github.com/SciML/ColPrac)
[Documentation](https://juliacloud.github.io/AWSCore.jl/build/AWSS3.html)

```julia
using AWSS3
using AWS # for global_aws_config

aws = global_aws_config(; region="us-east-2") # pass keyword arguments to change defaults

s3_create_bucket(aws, "my.bucket")
s3_enable_versioning(aws, "my.bucket")

s3_put(aws, "my.bucket", "key", "Hello!")
println(s3_get(aws, "my.bucket", "key"))
```
