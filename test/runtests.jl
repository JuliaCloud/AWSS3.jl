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

if VERSION >= v"1.5"
    using Minio
    AWS.aws_account_number(::Minio.MinioConfig) = "123"

    # We run most tests under Minio. This can be done locally by those
    # without access to the s3 bucket under which CI is performed.
    # We then run all tests with s3 directly.

    # We use multiple directories so that Minio can support versioning.
    dirs = [mktempdir() for _ =1:12]
    port = 9005
    minio_server = Minio.Server(dirs; address="localhost:$port")

    # We use this boolean flag to skip some tests under Minio
    minio = true
    try
        run(minio_server, wait=false)
        global aws = global_aws_config(MinioConfig("http://localhost:$port"; username="minioadmin", password="minioadmin"))
        @testset "Minio tests" begin
            include("s3path.jl")
            include("awss3.jl")
        end
    catch
        # Make sure we kill the server even if a test failed.
        kill(minio_server)
        rethrow()
    end
end

aws = global_aws_config(AWSConfig(; region = "us-east-1"))
minio = false # make sure we run all tests
@testset "AWSS3.jl" begin
    include("s3path.jl")
    include("awss3.jl")
end
