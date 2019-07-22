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

@testset "$(typeof(ps.root))" begin
    testsets = [
        test_constructor,
        test_registration,
        test_show,
        test_parse,
        test_convert,
        test_components,
        test_parents,
        test_join,
        test_basename,
        test_filename,
        test_extensions,
        test_isempty,
        test_norm,
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
    ]

    # Run all of the automated tests
    test(ps, testsets)

    # TODO: Copy over specific tests that can't be tested reliably from the general case.
end
