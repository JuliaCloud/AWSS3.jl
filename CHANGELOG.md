# AWSS3.jl v0.11 Release Notes

## Breaking changes

- v0.11.0: The `s3_exists`, `isdir(::S3Path)`, and `isfile(::S3Path)` calls now specify the `delimiter` to be `"/"` instead of `""` to support IAM policies which allow limited access to specified prefixes (see this [example](https://github.com/JuliaCloud/AWSS3.jl/pull/289#discussion_r1224636214)). Users who previously used the IAM policies conditional `{"Condition":{"StringEquals":{"s3:delimiter":[""]}}}` with AWSS3.jl v0.10 will need to update their IAM policy to be `{"s3:delimiter":["/"]}` with AWSS3.jl v0.11.0. To maintain compatibility with both versions of AWSS3.jl use the policy `{"s3:delimiter":["","/"]}`. Any policies not using the conditional `s3:delimiter` are unaffected ([#289]).

## Non-breaking changes

- v0.11.0: The `s3_exists` and `isdir(::S3Path)` calls no longer encounter HTTP 403 (Access Denied) errors when attempting to list resources which requiring an `s3:prefix` to be specified ([#289]).
- v0.11.1: The new keyword argument `returns` for `Base.write(fp::S3Path, ...)` determines the output returned from `write`, which can now be the raw `AWS.Response` (`returns=:response`) or the `S3Path` (`returns=:path`); this latter option returns an `S3Path` populated with the version ID of the written object (when versioning is enabled on the bucket) ([#293]).
- v0.11.2: New constructor for unversioned `S3Path` and version: `S3Path(path::S3Path; version=...)` ([#297]).

[#289]: https://github.com/JuliaCloud/AWSS3.jl/pull/289
[#293]: https://github.com/JuliaCloud/AWSS3.jl/pull/293
[#297]: https://github.com/JuliaCloud/AWSS3.jl/pull/297
