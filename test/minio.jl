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

using Minio

dirs = [mktempdir() for _ =1:12]
minio_server = Minio.Server(dirs; address="localhost:9002")  # create a server which views the current directory
run(minio_server, wait=false)

aws = global_aws_config(MinioConfig("http://localhost:9002"))
AWS.aws_account_number(::Minio.MinioConfig) = "123"

@testset "AWSS3.jl" begin
    include("s3path.jl")
    include("awss3.jl")
end
