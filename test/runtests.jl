using AWS
using AWS.AWSExceptions: AWSException
using AWSS3
using Arrow
using Dates
using FilePathsBase
using FilePathsBase: /, join
using FilePathsBase.TestPaths
using FilePathsBase.TestPaths: test
using HTTP
using JSON3
using Minio
using Mocking
using OrderedCollections: LittleDict
using Retry
using Test
using UUIDs: uuid4

Mocking.activate()

@service S3 use_response_type = true

include("utils.jl")

# Load the test functions
include("s3path.jl") # creates `awss3_tests(config)`
include("awss3.jl") # creates `s3path_tests(config)`

@testset "AWSS3.jl" begin
    # We can run most tests locally under MinIO without requring AWS credentials
    @testset "Minio" begin
        minio_server() do config
            awss3_tests(config)
            s3path_tests(config)
        end
    end

    @testset "S3" begin
        config = AWSConfig()
        awss3_tests(config)
        s3path_tests(config)
    end
end
