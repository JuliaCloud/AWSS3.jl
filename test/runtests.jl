using AWS
using AWS.AWSServices: s3
using AWSS3
using Test
using Dates
using Retry
using HTTP
using FilePathsBase
using FilePathsBase: /, join
using FilePathsBase.TestPaths
using UUIDs: uuid4

aws = AWSConfig()

function s3_nuke_bucket(bucket_name)
    for v in s3_list_versions(aws, bucket_name)
        s3_delete(aws, bucket_name, v["Key"]; version = v["VersionId"])
    end

    s3_delete_bucket(aws, bucket_name)
end

@testset "AWSS3.jl" begin
    include("s3path.jl")
    include("awss3.jl")
end
