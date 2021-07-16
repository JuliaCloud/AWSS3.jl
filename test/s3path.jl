bucket_name = "ocaws.jl.test." * lowercase(Dates.format(now(Dates.UTC), "yyyymmddTHHMMSSZ"))
s3_create_bucket(aws, bucket_name)
root = Path("s3://$bucket_name/pathset-root/")

ps = PathSet(
    root,
    root / "foo/",
    root / "foo" / "baz.txt",
    root / "bar/",
    root / "bar" / "qux/",
    root / "bar" / "qux" / "quux.tar.gz",
    root / "fred/",
    root / "fred" / "plugh",
    false
)

function test_s3_constructors(ps::PathSet)
    @test S3Path(bucket_name, "pathset-root/foo/baz.txt") == ps.baz
    @test S3Path(bucket_name, p"pathset-root/foo/baz.txt") == ps.baz
    @test S3Path(bucket_name, p"/pathset-root/foo/baz.txt") == ps.baz
    @test S3Path("s3://$bucket_name", p"/pathset-root/foo/baz.txt") == ps.baz
    @test S3Path(bucket_name, "pathset-root/bar/qux"; isdirectory=true) == ps.qux
    @test S3Path(bucket_name, "pathset-root/bar/qux/"; isdirectory=true) == ps.qux
    @test S3Path(bucket_name, p"pathset-root/bar/qux"; isdirectory=true) == ps.qux
    @test S3Path(bucket_name, p"/pathset-root/bar/qux"; isdirectory=true) == ps.qux
end

function test_s3_parents(ps::PathSet)
    @testset "parents" begin
        @test parent(ps.foo) == ps.root
        @test parent(ps.qux) == ps.bar
        @test dirname(ps.foo) == ps.root

        @test hasparent(ps.qux)
        _parents = parents(ps.qux)
        @test _parents[end] == ps.bar
        @test _parents[end - 1] == ps.root
        @test _parents[1] == Path(ps.root; segments=())
    end
end

function test_s3_join(ps::PathSet)
    @testset "join" begin
        @test join(ps.root, "bar/") == ps.bar
        @test ps.root / "foo" / "baz.txt" == ps.baz
        @test ps.root / "foobaz.txt" == ps.root / "foo" * "baz.txt"
    end
end

function test_s3_normalize(ps::PathSet)
    @testset "norm" begin
        @test normalize(ps.bar / ".." / "foo/") == ps.foo
        @test normalize(ps.bar / ".." / "foo") != ps.foo
        @test normalize(ps.bar / "./") == ps.bar
        @test normalize(ps.bar / "../") == ps.root
    end
end

function test_s3_mkdir(p::PathSet)
    @testset "mkdir" begin
        garply = p.root / "corge" / "grault" / "garply/"
        mkdir(garply; recursive=true)
        @test exists(garply)
        rm(p.root / "corge/"; recursive=true)
        @test !exists(garply)
    end
end

function test_s3_readpath(p::PathSet)
    @testset "readpath" begin
        @test readdir(p.root) == ["bar/", "foo/", "fred/"]
        @test readdir(p.qux) == ["quux.tar.gz"]
        @test readpath(p.root) == [p.bar, p.foo, p.fred]
        @test readpath(p.qux) == [p.quux]
    end
end

function test_s3_cp(p::PathSet)
    @testset "cp" begin
        # In case the folder objects were deleted in a previous test
        mkdir.([p.foo, p.qux, p.fred]; recursive=true, exist_ok=true)
        @test exists(p.foo)
        cp(p.foo, p.qux / "foo/"; force=true)
        @test exists(p.qux / "foo" / "baz.txt")
        rm(p.qux / "foo/"; recursive=true)
    end
end

function test_s3_mv(p::PathSet)
    @testset "mv" begin
        # In case the folder objects were deleted in a previous test
        mkdir.([p.foo, p.qux, p.fred]; recursive=true, exist_ok=true)
        garply = p.root / "corge" / "grault" / "garply/"
        mkdir(garply; recursive=true, exist_ok=true)
        @test exists(garply)
        mv(p.root / "corge/", p.foo / "corge/"; force=true)
        @test exists(p.foo / "corge" / "grault" / "garply/")
        rm(p.foo / "corge/"; recursive=true)
    end
end

function test_s3_sync(p::PathSet)
    @testset "sync" begin
        # In case the folder objects were deleted in a previous test
        mkdir.([p.foo, p.qux, p.fred]; recursive=true, exist_ok=true)
        # Base cp case
        sync(p.foo, ps.qux / "foo/")
        @test exists(p.qux / "foo" / "baz.txt")

        # Test that the copied baz file has a newer modified time
        baz_t = modified(p.qux / "foo" / "baz.txt")
        @test modified(p.baz) < baz_t

        # Don't cp unchanged files when a new file is added
        # NOTE: sleep before we make a new file, so it's clear tha the
        # modified time has changed.
        sleep(1)
        write(p.foo / "test.txt", "New File")
        sync(p.foo, ps.qux / "foo/")
        @test exists(p.qux / "foo" / "test.txt")
        @test read(p.qux / "foo" / "test.txt") == b"New File"
        @test read(p.qux / "foo" / "test.txt", String) == "New File"
        @test modified(p.qux / "foo" / "baz.txt") == baz_t
        @test modified(p.qux / "foo" / "test.txt") > baz_t

        # Test not deleting a file on sync
        rm(p.foo / "test.txt")
        sync(p.foo, ps.qux / "foo/")
        @test exists(p.qux / "foo" / "test.txt")

        # Test passing delete flag
        sync(p.foo, p.qux / "foo/"; delete=true)
        @test !exists(p.qux / "foo" / "test.txt")
        rm(p.qux / "foo/"; recursive=true)
    end
end

function test_s3_properties(ps::PathSet)
    @testset "s3_properties" begin
        fp1 = p"s3://mybucket/path/to/some/object"
        fp2 = p"s3://mybucket/path/to/some/prefix/"
        @test fp1.bucket == "mybucket"
        @test fp1.key == "path/to/some/object"
        @test fp2.bucket == "mybucket"
        @test fp2.key == "path/to/some/prefix/"
    end
end

function test_s3_folders_and_files(ps::PathSet)
    @testset "s3_folders_and_files" begin
        # Minio has slightly different semantics than s3 in that it does
        # not support having prefixes that clash with files
        # (https://github.com/minio/minio/issues/9865)
        # Thus in these tests, we run certain tests only on s3.
        minio = ps.root.config isa MinioConfig

        # In case the ps.root doesn't exist
        mkdir(ps.root; recursive=true, exist_ok=true)

        # Test that the trailing slash matters
        @test p"s3://mybucket/path/to/some/prefix/" != p"s3://mybucket/path/to/some/prefix"

        # Test that we can have empty directory names
        # I'm not sure if we want to support this in the future, but it may require more
        # overloading of AbstractPath methods to support properly.
        @test_broken p"s3://mybucket/path/to/some/prefix" != p"s3://mybucket/path//to/some/prefix"

        write(ps.root / "foobar", "I'm an object")
        if !minio
            mkdir(ps.root / "foobar/")
            write(ps.root / "foobar" / "car.txt", "I'm a different object")
        end

        @test read(ps.root / "foobar") == b"I'm an object"
        @test read(ps.root / "foobar", String) == "I'm an object"
        @test_throws ArgumentError readpath(ps.root / "foobar")
        if !minio
            @test readpath(ps.root / "foobar/") == [ps.root / "foobar" / "car.txt"]
            @test read(ps.root / "foobar" / "car.txt", String) == "I'm a different object"
        end
    end
end

function test_large_write(ps::PathSet)
    teststr = repeat("This is a test string!", round(Int, 2e5));
    @testset "large write/read" begin
        write(ps.quux, teststr; part_size=1, multipart=true)
        @test read(ps.quux, String) == teststr
    end
end

@testset "$(typeof(ps.root))" begin
    testsets = [
        test_s3_constructors,
        test_registration,
        test_show,
        test_parse,
        test_convert,
        test_components,
        test_indexing,
        test_iteration,
        test_s3_parents,
        test_descendants_and_ascendants,
        test_s3_join,
        test_splitext,
        test_basename,
        test_filename,
        test_extensions,
        test_isempty,
        test_s3_normalize,
        # test_canonicalize, # real doesn't make sense for S3Paths
        test_relative,
        test_absolute,
        test_isdir,
        test_isfile,
        test_stat,
        test_filesize,
        test_modified,
        test_created,
        test_cd,
        test_s3_readpath,
        test_walkpath,
        test_read,
        test_large_write,
        test_write,
        test_s3_mkdir,
        # These tests seem to fail due to an eventual consistency issue?
        test_s3_cp,
        test_s3_mv,
        test_s3_sync,
        test_symlink,
        test_touch,
        test_tmpname,
        test_tmpdir,
        test_mktmp,
        test_mktmpdir,
        test_download,
        test_issocket,
        # These will also all work for our custom path type,
        # but many implementations won't support them.
        test_isfifo,
        test_ischardev,
        test_isblockdev,
        test_ismount,
        test_isexecutable,
        test_isreadable,
        test_iswritable,
        # test_chown,   # chmod & chown don't make sense for S3Paths
        # test_chmod,
        test_s3_properties,
        test_s3_folders_and_files,
    ]

    # Run all of the automated tests
    test(ps, testsets)
end

@testset "readdir" begin
    function initialize()
        """
        Hierarchy:

        bucket-name
        |-- test_01.txt
        |-- emptydir/
        |-- subdir1/
        |   |-- test_02.txt
        |   |-- test_03.txt
        |   |-- subdir2/
        |       |-- test_04.txt
        |       |-- subdir3/
        """
        s3_put(bucket_name, "test_01.txt", "test01")
        s3_put(bucket_name, "emptydir/", "")
        s3_put(bucket_name, "subdir1/", "")
        s3_put(bucket_name, "subdir1/test_02.txt", "test02")
        s3_put(bucket_name, "subdir1/test_03.txt", "test03")
        s3_put(bucket_name, "subdir1/subdir2/", "")
        s3_put(bucket_name, "subdir1/subdir2/test_04.txt", "test04")
        s3_put(bucket_name, "subdir1/subdir2/subdir3/", "")
    end

    function verify_files(path::S3Path)
        @test readdir(path) == ["emptydir/", "subdir1/", "test_01.txt"]
        @test readdir(path; join=true) == [path / "emptydir/", path / "subdir1/", path / "test_01.txt"]
        @test readdir(path / "emptydir/") == []
        @test readdir(path / "emptydir/"; join=true) == []
        @test readdir(path / "subdir1/") == ["subdir2/", "test_02.txt", "test_03.txt"]
        @test readdir(path / "subdir1/"; join=true) == [path / "subdir1/" / "subdir2/", path / "subdir1/" / "test_02.txt", path / "subdir1/" / "test_03.txt"]
        @test readdir(path / "subdir1/subdir2/") == ["subdir3/", "test_04.txt"]
        @test readdir(path / "subdir1/subdir2/"; join=true) == [path / "subdir1/subdir2/" / "subdir3/", path / "subdir1/subdir2/" / "test_04.txt"]
        @test readdir(path / "subdir1/subdir2/subdir3/") == []
        @test readdir(path / "subdir1/subdir2/subdir3/"; join=true) == []
    end

    function verify_files(path::AbstractPath)
        @test readdir(path) == ["emptydir", "subdir1", "test_01.txt"]
        VERSION >= v"1.4.0" && @test readdir(path; join=true) == [path / "emptydir", path / "subdir1", path / "test_01.txt"]
        @test readdir(path / "emptydir/") == []
        VERSION >= v"1.4.0" && @test readdir(path / "emptydir/"; join=true) == []
        @test readdir(path / "subdir1/") == ["subdir2", "test_02.txt", "test_03.txt"]
        VERSION >= v"1.4.0" && @test readdir(path / "subdir1/"; join=true) == [path / "subdir1" / "subdir2", path / "subdir1" / "test_02.txt", path / "subdir1/" / "subdir1/test_03.txt"]
        @test readdir(path / "subdir1/subdir2/") == ["subdir3", "test_04.txt"]
        VERSION >= v"1.4.0" && @test readdir(path / "subdir1/subdir2/"; join=true) == [path / "subdir1/subdir2/" / "subdir3", path / "subdir1/subdir2/" / "test_04.txt"]
        @test readdir(path / "subdir1/subdir2/subdir3/") == []
        VERSION >= v"1.4.0" && @test readdir(path / "subdir1/subdir2/subdir3/"; join=true) == []
    end

    initialize()

    @testset "S3" begin
        verify_files(S3Path("s3://$bucket_name/"))
        @test_throws ArgumentError("Invalid s3 path string: $bucket_name") S3Path(bucket_name)
    end

    @test_skip @testset "Local" begin
        temp_path = Path(tempdir() * string(uuid4()))
        mkdir(temp_path)

        sync(S3Path("s3://$bucket_name/"), temp_path)
        verify_files(temp_path)

        rm(temp_path, force=true, recursive=true)
    end

    @testset "join" begin
        @test (  # test trailing slash on prefix does not matter for join
            p"s3://foo/bar" / "baz" ==
            p"s3://foo/bar/" / "baz" ==
            p"s3://foo/bar/baz"
        )
        @test (  # test trailing slash on root-only prefix in particular does not matter
            p"s3://foo" / "bar" / "baz" ==
            p"s3://foo/" / "bar" / "baz" ==
            p"s3://foo/bar/baz"
        )
        # test extra leading and trailing slashes do not matter
        @test p"s3://foo/" / "bar/" / "/baz" == p"s3://foo/bar/baz"
        # test joining `/` and string concatentation `*` play nice as expected
        @test p"s3://foo" * "/" / "bar" == p"s3://foo" / "/" * "bar" == p"s3://foo" / "bar"
        @test p"s3://foo" / "bar" * "baz" == p"s3://foo/bar" * "baz"  == p"s3://foo" / "barbaz"
        # test trailing slash on final piece is included
        @test p"s3://foo/bar" / "baz/" == p"s3://foo/bar/baz/"
    end

    @testset "readdir" begin
        path = S3Path("s3://$(bucket_name)/A/A/B.txt"; config = aws)
        write(path, "test!")
        results = readdir(S3Path("s3://$(bucket_name)/A/"; config = aws))

        @test results == ["A/"]
    end
end

@testset "JSON roundtripping" begin
    json_path = S3Path("s3://$(bucket_name)/test_json"; config=aws)
    my_dict = Dict("key" => "value", "key2" => 5.0)
    # here we use the "application/json" MIME type to trigger the heuristic parsing into a `LittleDict`
    # that will hit a `MethodError` at the `Vector{UInt8}` constructor of `read(::S3Path)` if `raw=true`
    # was not passed to `s3_get` in that method.
    s3_put(aws, bucket_name, "test_json", JSON3.write(my_dict), "application/json")
    json_bytes = read(json_path)
    @test JSON3.read(json_bytes, Dict) == my_dict
    rm(json_path)
end

# Minio 
if !(aws isa MinioConfig)
    AWSS3.s3_nuke_bucket(aws, bucket_name)
end
