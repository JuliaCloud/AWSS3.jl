using AWSS3, Minio, FilePathsBase

const DIR = mktempdir()
const PORT = 9000

const MINIO_SERVER = Minio.Server([DIR]; address="localhost:$PORT")

const CONFIG = MinioConfig("http://localhost:$PORT")

#===================================================================================================
sandbox.jl

    This file sets up a simple min.io bucket with a few files that is useful for testing,
    developing, and just generally screwing around.

    Include and do `r = init()`
===================================================================================================#


runserver() = run(MINIO_SERVER; wait=false)

makebucket() = s3_create_bucket(CONFIG, "test-bucket")

rootpath() = S3Path("s3://test-bucket/"; config=CONFIG)

# assumes bucket made
function makefiles()
    r = rootpath()
    write(joinpath(r, "testfile.txt"), "what up?")
    write(joinpath(r, "testdir/"), "")
    write(
        joinpath(r, "testdir/testfile2.txt"),
        "we are 3 cool guys looking for other cool guys",
    )
    write(joinpath(r, "testdir_empty."), "to hang out in our party mansion")
    write(joinpath(r, "testdir_empty/"), "")
    return r
end

function init()
    runserver()
    sleep(0.5)
    makebucket()
    return makefiles()
end
