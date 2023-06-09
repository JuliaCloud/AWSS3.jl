# AWSS3.jl v0.11.0 Release Notes

## Breaking changes

- The `s3_exists`, `isdir(::S3Path)`, and `isfile(::S3Path)` calls now specify the `delimiter` to be `"/"` instead of `""` to support IAM policies which allow limited access to specified prefixes (see this [example](https://github.com/JuliaCloud/AWSS3.jl/pull/289#discussion_r1224636214)). Users who previously used the IAM policies conditional `{"Condition":{"StringEquals":{"s3:delimiter":[""]}}}` with AWSS3.jl v0.10 will need to update their IAM policy to be `{"s3:delimiter":["/"]}` with AWSS3.jl v0.11.0. To maintain compatibility with both versions of AWSS3.jl use the policy `{"s3:delimiter":["","/"]}`. Any policies not using the conditional `s3:delimiter` are unaffected ([#289]).

## Non-breaking changes

- The `s3_exists` and `isdir(::S3Path)` calls no longer encounter HTTP 403 (Access Denied) errors when attempting to list resources which requiring an `s3:prefix` to be specified ([#289]).

[#289]: https://github.com/JuliaCloud/AWSS3.jl/pull/289
