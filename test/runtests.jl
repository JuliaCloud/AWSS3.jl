using AWS
using AWS.AWSExceptions: AWSException
using AWSS3
using Arrow
using Dates
using FilePathsBase
using FilePathsBase.TestPaths
using FilePathsBase: /, join
using JSON3
using Mocking
using Retry
using Test
using UUIDs: uuid4

Mocking.activate()

@service S3 use_response_type = true

is_aws(config) = config isa AWSConfig

# Load the test functions
include("s3path.jl") # creates `awss3_tests(config)`
include("awss3.jl") # creates `s3path_tests(config)`

@testset "AWSS3.jl" begin
    if VERSION >= v"1.5"
        using Minio
        AWS.aws_account_number(::Minio.MinioConfig) = "123"

        # We run most tests under Minio. This can be done locally by those
        # without access to the s3 bucket under which CI is performed.
        # We then run all tests with s3 directly.

        port = 9005
        minio_server = Minio.Server([mktempdir()]; address="localhost:$port")

        try
            run(minio_server; wait=false)
            sleep(0.5)  # give the server just a bit of time, though it is amazingly fast to start
            config = global_aws_config(
                MinioConfig(
                    "http://localhost:$port"; username="minioadmin", password="minioadmin"
                ),
            )
            @testset "Minio" begin
                awss3_tests(config)
                s3path_tests(config)
            end
        finally
            # Make sure we kill the server even if a test failed.
            kill(minio_server)
        end
    end

    # Set `AWSConfig` as the default for the following tests
    aws = global_aws_config(AWSConfig())
    @testset "S3" begin
        awss3_tests(aws)
        s3path_tests(aws)
    end
end
