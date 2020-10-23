# AWSS3

AWS S3 Interface for Julia

[![Build Status](https://travis-ci.org/JuliaCloud/AWSS3.jl.svg?branch=master)](https://travis-ci.org/JuliaCloud/AWSS3.jl)
[![Code Style: Blue](https://img.shields.io/badge/code%20style-blue-4495d1.svg)](https://github.com/invenia/BlueStyle)
[![ColPrac: Contributor's Guide on Collaborative Practices for Community Packages](https://img.shields.io/badge/ColPrac-Contributor's%20Guide-blueviolet)](https://github.com/SciML/ColPrac)
[Documentation](https://juliacloud.github.io/AWSCore.jl/build/AWSS3.html)

```julia
using AWSS3

aws = AWSCore.aws_config()

s3_create_bucket(aws, "my.bucket")
s3_enable_versioning(aws, "my.bucket")

s3_put(aws, "my.bucket", "key", "Hello!")
println(s3_get(aws, "my.bucket", "key"))
```
