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

aws = AWSConfig(; region = "us-east-1")

@testset "AWSS3.jl" begin
    include("s3path.jl")
    include("awss3.jl")
end
