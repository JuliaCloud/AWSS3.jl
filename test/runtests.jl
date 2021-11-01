using AWS
using AWS.AWSExceptions: AWSException
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
using JSON3

const AWSS3_TESTSETS = split(get(ENV, "AWSS3_TESTSETS", "MinIO,S3"), ',')

is_aws(config) = config isa AWSConfig

# Load the test functions
include("s3path.jl") # creates `awss3_tests(config)`
include("awss3.jl") # creates `s3path_tests(config)`

@testset "AWSS3.jl" begin
    @testset "MinIO" begin
        if "MinIO" in AWSS3_TESTSETS && VERSION >= v"1.5"
            using Minio
            AWS.aws_account_number(::Minio.MinioConfig) = "123"

            # We run most tests under Minio. This can be done locally by those
            # without access to the s3 bucket under which CI is performed.
            # We then run all tests with s3 directly.

            port = 9005
            minio_server = Minio.Server([mktempdir()]; address="localhost:$port")

            minio_config = MinioConfig(
                "http://localhost:$port"; username="minioadmin", password="minioadmin"
            )

            try
                run(minio_server; wait=false)
                sleep(0.5)  # give the server just a bit of time, though it is amazingly fast to start

                config = global_aws_config(minio_config)
                awss3_tests(config)
                s3path_tests(config)
            finally
                # Make sure we kill the server even if a test failed.
                kill(minio_server)
            end
        elseif VERSION < v"1.5"
            @warn "Skipping MinIO tests as they can only be run on Julia ≥ 1.5"
        else
            @warn "Skipping MinIO tests"
        end
    end

    @testset "S3" begin
        if "S3" in AWSS3_TESTSETS
            # Set `AWSConfig` as the default for the following tests
            config = global_aws_config(AWSConfig())

            awss3_tests(config)
            s3path_tests(config)
        else
            @warn "Skipping S3 tests"
        end
    end
end
