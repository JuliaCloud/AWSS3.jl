# Start minio with
# > docker run -p 9000:9000 minio/minio server /data{1..12}
# first.

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

# https://github.com/JuliaCloud/AWS.jl#modifying-functionality
struct MinioConfig <: AbstractAWSConfig
    endpoint::String
    region::String
    creds
 end
 AWS.region(c::MinioConfig) = c.region
 AWS.credentials(c::MinioConfig) = c.creds

 AWS.aws_account_number(c::MinioConfig) = 1234

 struct SimpleCredentials
    access_key_id::String
    secret_key::String
    token::String
end

AWS.check_credentials(c::SimpleCredentials) = c

function AWS.generate_service_url(aws::MinioConfig, service::String, resource::String)
    service == "s3" || throw(ArgumentError("Can only handle s3 requests to Minio"))
    return string(aws.endpoint, resource)
end

aws = AWS.global_aws_config(MinioConfig("http://127.0.0.1:9000", "a_region", SimpleCredentials("minioadmin", "minioadmin", "")))

@testset "AWSS3.jl" begin
    include("s3path.jl")
    include("awss3.jl")
end
