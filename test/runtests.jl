using AWSS3
using Test
using Dates
using AWSCore
using Retry
using HTTP
using FilePathsBase
using FilePathsBase: /, join
using FilePathsBase.TestPaths
using UUIDs: uuid4

AWSCore.set_debug_level(0)

@testset "AWSS3.jl" begin
    include("s3path.jl")
    include("awss3.jl")
end
