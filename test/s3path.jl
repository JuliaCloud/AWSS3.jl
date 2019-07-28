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

function test_s3_join(ps::PathSet)
    @testset "join" begin
        @test join(ps.root, "bar/") == ps.bar
        @test ps.root / "foo" / "baz.txt" == ps.baz
        @test ps.root / "foobaz.txt" == ps.root / "foo" * "baz.txt"
    end
end

function test_s3_norm(ps::PathSet)
    @testset "norm" begin
        @test norm(ps.bar / ".." / "foo/") == ps.foo
        @test norm(ps.bar / ".." / "foo") != ps.foo
        @test norm(ps.bar / "./") == ps.bar
        @test norm(ps.bar / "../") == ps.root
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
        # In case the ps.root doesn't exist
        mkdir(ps.root; recursive=true, exist_ok=true)

        # Test that the trailing slash matters
        @test p"s3://mybucket/path/to/some/prefix/" != p"s3://mybucket/path/to/some/prefix"

        # Test that we can have empty directory names
        # I'm not sure if we want to support this in the future, but it may require more
        # overloading of AbstractPath methods to support properly.
        @test_broken p"s3://mybucket/path/to/some/prefix" != p"s3://mybucket/path//to/some/prefix"

        write(ps.root / "foobar", "I'm an object")
        mkdir(ps.root / "foobar/")
        write(ps.root / "foobar" / "car.txt", "I'm a different object")

        @test read(ps.root / "foobar", String) == "I'm an object"
        @test_throws ArgumentError readpath(ps.root / "foobar")
        @test readpath(ps.root / "foobar/") == [ps.root / "foobar" / "car.txt"]
    end
end

@testset "$(typeof(ps.root))" begin
    testsets = [
        test_constructor,
        test_registration,
        test_show,
        test_parse,
        test_convert,
        test_components,
        test_parents,
        test_s3_join,
        test_basename,
        test_filename,
        test_extensions,
        test_isempty,
        test_s3_norm,
        # test_real, # real doesn't make sense for S3Paths
        test_relative,
        test_abs,
        test_isdir,
        test_isfile,
        test_stat,
        test_size,
        test_modified,
        test_created,
        test_cd,
        test_s3_readpath,
        test_walkpath,
        test_read,
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
