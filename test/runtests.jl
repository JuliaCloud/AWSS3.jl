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
using JSON3

is_aws(config) = config isa AWSConfig

# Load the test functions
include("s3path.jl")
include("awss3.jl")

if VERSION >= v"1.5"
    using Minio
    AWS.aws_account_number(::Minio.MinioConfig) = "123"

    # We run most tests under Minio. This can be done locally by those
    # without access to the s3 bucket under which CI is performed.
    # We then run all tests with s3 directly.

    # We use multiple directories so that Minio can support versioning.
    root = mktempdir()
    dirs = [mkdir(joinpath(root, string(i))) for i in 1:12]
    port = 9005
    minio_server = Minio.Server(dirs; address="localhost:$port")

    try
        run(minio_server, wait=false)
        config = global_aws_config(MinioConfig("http://localhost:$port"; username="minioadmin", password="minioadmin"))
        @testset "Minio tests" begin
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
@testset "AWSS3.jl" begin
    awss3_tests(aws)
    s3path_tests(aws)
end
