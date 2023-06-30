function test_s3_constructors(ps::PathSet)
    bucket_name = ps.root.bucket
    @test S3Path(bucket_name, "pathset-root/foo/baz.txt") == ps.baz
    @test S3Path(bucket_name, p"pathset-root/foo/baz.txt") == ps.baz
    @test S3Path(bucket_name, p"/pathset-root/foo/baz.txt") == ps.baz
    @test S3Path("s3://$bucket_name", p"/pathset-root/foo/baz.txt") == ps.baz
    @test S3Path(bucket_name, "pathset-root/bar/qux"; isdirectory=true) == ps.qux
    @test S3Path(bucket_name, "pathset-root/bar/qux/"; isdirectory=true) == ps.qux
    @test S3Path(bucket_name, p"pathset-root/bar/qux"; isdirectory=true) == ps.qux
    @test S3Path(bucket_name, p"/pathset-root/bar/qux"; isdirectory=true) == ps.qux
    @test S3Path("s3://$bucket_name/pathset-root/bar/qux"; isdirectory=true) == ps.qux
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

function test_s3_download(base_config::AbstractAWSConfig)
    config = assume_testset_role("ReadWriteObject"; base_config)

    # Requires that the global AWS configuration is set so that `S3Path`s created within
    # the tests have the correct permissions (e.g. `download(::S3Path, "s3://...")`)
    return p -> with_aws_config(config) do
        test_download(p)
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

function test_s3_walkpath(p::PathSet)
    @testset "walkpath - S3" begin
        # Test that we still return parent prefixes even when no "directory" objects
        # have been created by a `mkdir`, retaining consistency with `readdir`.
        _root = p.root / "s3_walkpath/"

        _foo = _root / "foo/"
        _baz = _foo / "baz.txt"
        _bar = _root / "bar/"
        _qux = _bar / "qux/"
        _quux = _qux / "quux.tar.gz"

        # Only write the leaf files
        write(_baz, read(p.baz))
        write(_quux, read(p.quux))

        topdown = [_bar, _qux, _quux, _foo, _baz]
        bottomup = [_quux, _qux, _bar, _baz, _foo]

        @test collect(walkpath(_root; topdown=true)) == topdown
        @test collect(walkpath(_root; topdown=false)) == bottomup

        rm(_root; recursive=true)
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

function test_s3_sync(ps::PathSet)
    return p -> @testset "sync" begin
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

function test_s3_properties(base_config::AbstractAWSConfig)
    return ps -> @testset "s3_properties" begin
        config = assume_testset_role("ReadWriteObject"; base_config)

        fp1 = S3Path("s3://mybucket/path/to/some/object"; config)
        fp2 = S3Path("s3://mybucket/path/to/some/prefix/"; config)
        @test fp1.bucket == "mybucket"
        @test fp1.key == "path/to/some/object"
        @test fp2.bucket == "mybucket"
        @test fp2.key == "path/to/some/prefix/"
        @test fp2.version === nothing

        try
            fp3 = S3Path(ps.root.bucket, "/another/testdir/"; config)
            strs = ["what up", "what else up", "what up again"]
            write(fp3 / "testfile1.txt", strs[1])
            write(fp3 / "testfile2.txt", strs[2])
            write(fp3 / "inner" / "testfile3.txt", strs[3])
            @test AWSS3.diskusage(fp3) == sum(ncodeunits.(strs))

            # we deliberately pick an older file to compare to so we
            # can be confident timestamps are different
            @test AWSS3.lastmodified(fp3) > AWSS3.lastmodified(ps.foo)
        finally
            rm(S3Path(ps.root.bucket, "/another/"; config); recursive=true)  # otherwise subsequent tests may fail
        end
    end
end

function test_s3_folders_and_files(ps::PathSet)
    config = ps.root.config
    @testset "s3_folders_and_files" begin
        # Minio has slightly different semantics than s3 in that it does
        # not support having prefixes that clash with files
        # (https://github.com/minio/minio/issues/9865)
        # Thus in these tests, we run certain tests only on s3.

        # In case the ps.root doesn't exist
        mkdir(ps.root; recursive=true, exist_ok=true)

        # Test that the trailing slash matters
        @test p"s3://mybucket/path/to/some/prefix/" != p"s3://mybucket/path/to/some/prefix"

        # Test that we can have empty directory names
        # I'm not sure if we want to support this in the future, but it may require more
        # overloading of AbstractPath methods to support properly.
        @test_broken p"s3://mybucket/path/to/some/prefix" !=
            p"s3://mybucket/path//to/some/prefix"

        write(ps.root / "foobar", "I'm an object")
        if is_aws(config)
            mkdir(ps.root / "foobar/")
            write(ps.root / "foobar" / "car.txt", "I'm a different object")
        end

        @test read(ps.root / "foobar") == b"I'm an object"
        @test read(ps.root / "foobar", String) == "I'm an object"
        @test_throws ArgumentError readpath(ps.root / "foobar")
        if is_aws(config)
            @test readpath(ps.root / "foobar/") == [ps.root / "foobar" / "car.txt"]
            @test read(ps.root / "foobar" / "car.txt", String) == "I'm a different object"
        end
    end
end

function test_multipart_write(ps::PathSet)
    teststr = repeat("This is a test string!", round(Int, 2e5))
    @testset "multipart write/read" begin
        result = write(ps.quux, teststr; part_size_mb=1, multipart=true)
        @test read(ps.quux, String) == teststr
        @test result == UInt8[]
    end

    @testset "multipart write/read, return path" begin
        result = write(ps.quux, teststr; part_size_mb=1, multipart=true, returns=:path)
        @test read(ps.quux, String) == teststr
        @test isa(result, S3Path)
    end

    @testset "multipart write/read, return response" begin
        result = write(ps.quux, teststr; part_size_mb=1, multipart=true, returns=:response)
        @test read(ps.quux, String) == teststr
        @test isa(result, AWS.Response)
    end
end

function test_write_returns(ps::PathSet)
    @testset "write returns" begin
        teststr = "Test string"
        @test write(ps.quux, teststr) == UInt8[]
        @test write(ps.quux, teststr; returns=:parsed) == UInt8[]
        @test write(ps.quux, teststr; returns=:response) isa AWS.Response
        @test write(ps.quux, teststr; returns=:path) isa S3Path
        @test_throws ArgumentError write(ps.quux, teststr; returns=:unsupported_return_type)
    end
end

function initialize(config, bucket_name)
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
    s3_put(config, bucket_name, "test_01.txt", "test01")
    s3_put(config, bucket_name, "emptydir/", "")
    s3_put(config, bucket_name, "subdir1/", "")
    s3_put(config, bucket_name, "subdir1/test_02.txt", "test02")
    s3_put(config, bucket_name, "subdir1/test_03.txt", "test03")
    s3_put(config, bucket_name, "subdir1/subdir2/", "")
    s3_put(config, bucket_name, "subdir1/subdir2/test_04.txt", "test04")
    return s3_put(config, bucket_name, "subdir1/subdir2/subdir3/", "")
end

function verify_files(path::S3Path)
    @test readdir(path) == ["emptydir/", "subdir1/", "test_01.txt"]
    @test readdir(path; join=true) ==
        [path / "emptydir/", path / "subdir1/", path / "test_01.txt"]
    @test readdir(path / "emptydir/") == []
    @test readdir(path / "emptydir/"; join=true) == []
    @test readdir(path / "subdir1/") == ["subdir2/", "test_02.txt", "test_03.txt"]
    @test readdir(path / "subdir1/"; join=true) == [
        path / "subdir1/" / "subdir2/",
        path / "subdir1/" / "test_02.txt",
        path / "subdir1/" / "test_03.txt",
    ]
    @test readdir(path / "subdir1/subdir2/") == ["subdir3/", "test_04.txt"]
    @test readdir(path / "subdir1/subdir2/"; join=true) == [
        path / "subdir1/subdir2/" / "subdir3/", path / "subdir1/subdir2/" / "test_04.txt"
    ]
    @test readdir(path / "subdir1/subdir2/subdir3/") == []
    @test readdir(path / "subdir1/subdir2/subdir3/"; join=true) == []
end

function verify_files(path::AbstractPath)
    @test readdir(path) == ["emptydir", "subdir1", "test_01.txt"]
    @test readdir(path; join=true) ==
        [path / "emptydir", path / "subdir1", path / "test_01.txt"]
    @test readdir(path / "emptydir/") == []
    @test readdir(path / "emptydir/"; join=true) == []
    @test readdir(path / "subdir1/") == ["subdir2", "test_02.txt", "test_03.txt"]
    @test readdir(path / "subdir1/"; join=true) == [
        path / "subdir1" / "subdir2",
        path / "subdir1" / "test_02.txt",
        path / "subdir1/" / "subdir1/test_03.txt",
    ]
    @test readdir(path / "subdir1/subdir2/") == ["subdir3", "test_04.txt"]
    @test readdir(path / "subdir1/subdir2/"; join=true) ==
        [path / "subdir1/subdir2/" / "subdir3", path / "subdir1/subdir2/" / "test_04.txt"]
    @test readdir(path / "subdir1/subdir2/subdir3/") == []
    @test readdir(path / "subdir1/subdir2/subdir3/"; join=true) == []
end

# This is the main entrypoint for the S3Path tests
function s3path_tests(base_config)
    bucket_name = gen_bucket_name()

    let
        config = assume_testset_role("CreateBucket"; base_config)
        s3_create_bucket(config, bucket_name)
    end

    root = let
        config = assume_testset_role("ReadWriteObject"; base_config)
        S3Path("s3://$bucket_name/pathset-root/"; config)
    end

    ps = PathSet(
        root,
        root / "foo/",
        root / "foo" / "baz.txt",
        root / "bar/",
        root / "bar" / "qux/",
        root / "bar" / "qux" / "quux.tar.gz",
        root / "fred/",
        root / "fred" / "plugh",
        false,
    )

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
            test_multipart_write,
            test_write,
            test_write_returns,
            test_s3_mkdir,
            # These tests seem to fail due to an eventual consistency issue?
            test_s3_cp,
            test_s3_mv,
            test_s3_sync(ps),
            test_symlink,
            test_touch,
            test_tmpname,
            test_tmpdir,
            test_mktmp,
            test_mktmpdir,
            test_s3_download(base_config),
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
            test_s3_properties(base_config),
            test_s3_folders_and_files,
        ]

        # Run all of the automated tests
        #
        # Note: `FilePathsBase.TestPaths.test` internally calls an `initialize` function
        # which requires AWS permissions in order to write some files to S3. Due to this
        # setup and how `test` passes in `ps` to each test it makes it hard to have each
        # testset specify their required permissions separately. Currently, we embed the
        # configuration in the paths themselves but it may make more sense to set the
        # config globally temporarily via `with_aws_config`.
        test(ps, testsets)
    end

    @testset "readdir" begin
        config = assume_testset_role("ReadWriteObject"; base_config)
        initialize(config, bucket_name)

        @testset "S3" begin
            verify_files(S3Path("s3://$bucket_name/"; config))
            @test_throws ArgumentError("Invalid s3 path string: $bucket_name") S3Path(
                bucket_name
            )
        end

        @test_skip @testset "Local" begin
            temp_path = Path(tempdir() * string(uuid4()))
            mkdir(temp_path)

            sync(S3Path("s3://$bucket_name/"; config), temp_path)
            verify_files(temp_path)

            rm(temp_path; force=true, recursive=true)
        end

        @testset "join" begin
            @test (  # test trailing slash on prefix does not matter for join
                p"s3://foo/bar" / "baz" == p"s3://foo/bar/" / "baz" == p"s3://foo/bar/baz"
            )
            @test (  # test trailing slash on root-only prefix in particular does not matter
                p"s3://foo" / "bar" / "baz" ==
                p"s3://foo/" / "bar" / "baz" ==
                p"s3://foo/bar/baz"
            )
            # test extra leading and trailing slashes do not matter
            @test p"s3://foo/" / "bar/" / "/baz" == p"s3://foo/bar/baz"
            # test joining `/` and string concatentation `*` play nice as expected
            @test p"s3://foo" * "/" / "bar" ==
                p"s3://foo" / "/" * "bar" ==
                p"s3://foo" / "bar"
            @test p"s3://foo" / "bar" * "baz" ==
                p"s3://foo/bar" * "baz" ==
                p"s3://foo" / "barbaz"
            # test trailing slash on final piece is included
            @test p"s3://foo/bar" / "baz/" == p"s3://foo/bar/baz/"
        end

        @testset "readdir" begin
            path = S3Path("s3://$(bucket_name)/A/A/B.txt"; config)
            write(path, "test!")
            results = readdir(S3Path("s3://$(bucket_name)/A/"; config))

            @test results == ["A/"]
        end
    end

    @testset "isdir" begin
        config = assume_testset_role("ReadObject"; base_config)

        function _generate_exception(code)
            return AWSException(
                code, "", nothing, AWS.HTTP.Exceptions.StatusError(404, "", "", ""), nothing
            )
        end

        @testset "top level bucket" begin
            @testset "success" begin
                @test isdir(S3Path("s3://$(bucket_name)"; config))
                @test isdir(S3Path("s3://$(bucket_name)/"; config))
            end

            @testset "NoSuchBucket" begin
                test_exception = _generate_exception("NoSuchBucket")
                patch = @patch function AWSS3.S3.list_objects_v2(args...; kwargs...)
                    throw(test_exception)
                end

                apply(patch) do
                    @test !isdir(S3Path("s3://$(bucket_name)"; config))
                    @test !isdir(S3Path("s3://$(bucket_name)/"; config))
                end
            end

            @testset "Other Exception" begin
                test_exception = _generate_exception("TestException")
                patch = @patch function AWSS3.S3.list_objects_v2(args...; kwargs...)
                    throw(test_exception)
                end

                apply(patch) do
                    @test_throws AWSException isdir(S3Path("s3://$(bucket_name)"; config))
                    @test_throws AWSException isdir(S3Path("s3://$(bucket_name)/"; config))
                end
            end
        end

        # Based upon this example: https://repost.aws/knowledge-center/iam-s3-user-specific-folder
        #
        # MinIO isn't currently setup with the restrictive prefix required to make the tests
        # fail with "AccessDenied".
        is_aws(base_config) && @testset "Restricted Prefix" begin
            setup_config = assume_testset_role("ReadWriteObject"; base_config)
            s3_put(
                setup_config,
                bucket_name,
                "prefix/denied/secrets/top-secret",
                "for british eyes only",
            )
            s3_put(setup_config, bucket_name, "prefix/granted/file", "hello")

            config = assume_testset_role("RestrictedPrefixTestset"; base_config)
            @test isdir(S3Path("s3://$(bucket_name)/prefix/granted/"; config))
            @test isdir(S3Path("s3://$(bucket_name)/prefix/"; config))
            @test isdir(S3Path("s3://$(bucket_name)"; config))

            @test_throws_msg ["AccessDenied", "403"] begin
                isdir(S3Path("s3://$(bucket_name)/prefix/denied/"; config))
            end

            # The above call fails as we use `"prefix" => "prefix/denied/"`. However,
            # this restricted role can still determine that the "denied" directory
            # exists with some carefully crafted queries.
            params = Dict("prefix" => "prefix/", "delimiter" => "/")
            r = S3.list_objects_v2(bucket_name, params; aws_config=config)
            prefixes = [x["Prefix"] for x in parse(r)["CommonPrefixes"]]
            @test "prefix/denied/" in prefixes

            @test_throws_msg ["AccessDenied", "403"] begin
                !isdir(S3Path("s3://$(bucket_name)/prefix/dne/"; config))
            end

            @test_throws_msg ["AccessDenied", "403"] begin
                !isdir(S3Path("s3://$(bucket_name)/prefix/denied/secrets/"; config))
            end
        end
    end

    @testset "JSON roundtripping" begin
        config = assume_testset_role("ReadWriteObject"; base_config)

        json_path = S3Path("s3://$(bucket_name)/test_json"; config)
        my_dict = Dict("key" => "value", "key2" => 5.0)
        # here we use the "application/json" MIME type to trigger the heuristic parsing into a `LittleDict`
        # that will hit a `MethodError` at the `Vector{UInt8}` constructor of `read(::S3Path)` if `raw=true`
        # was not passed to `s3_get` in that method.
        result = s3_put(
            config, bucket_name, "test_json", JSON3.write(my_dict), "application/json"
        )
        @test result == UInt8[]
        json_bytes = read(json_path)
        @test JSON3.read(json_bytes, Dict) == my_dict
        rm(json_path)
    end

    @testset "Arrow <-> S3Path (de)serialization" begin
        ver = String('A':'Z') * String('0':'5')
        paths = Union{Missing,S3Path}[
            missing,
            S3Path("s3://$(bucket_name)/a"),
            S3Path("s3://$(bucket_name)/b?versionId=$ver"),
            # format trick: using this comment to force use of multiple lines
        ]
        tbl = Arrow.Table(Arrow.tobuffer((; paths=paths)))
        @test all(isequal.(tbl.paths, paths))

        # Cannot serialize `S3Path`s with embedded `AWSConfig`s.
        push!(paths, S3Path("s3://$(bucket_name)/c"; config=base_config))
        @test_throws ArgumentError Arrow.tobuffer((; paths))
    end

    @testset "tryparse" begin
        # The global `AWSConfig` is just used for comparison and isn't used for access
        cfg = global_aws_config()
        ver = String('A':'Z') * String('0':'5')

        @test S3Path("s3://my_bucket/prefix/path") ==
            S3Path(("prefix", "path"), "/", "s3://my_bucket", false, nothing, cfg)

        @test S3Path("s3://my_bucket/prefix/path/") ==
            S3Path(("prefix", "path"), "/", "s3://my_bucket", true, nothing, cfg)

        @test S3Path("s3://my_bucket/") ==
            S3Path((), "/", "s3://my_bucket", true, nothing, cfg)

        @test S3Path("s3://my_bucket") ==
            S3Path((), "", "s3://my_bucket", true, nothing, cfg)

        @test S3Path("s3://my_bucket/prefix/path?versionId=$ver") ==
            S3Path(("prefix", "path"), "/", "s3://my_bucket", false, ver, cfg)

        @test S3Path("s3://my_bucket/prefix/path/?versionId=$ver") ==
            S3Path(("prefix", "path"), "/", "s3://my_bucket", true, ver, cfg)

        @test S3Path("s3://my_bucket/?versionId=$ver") ==
            S3Path((), "/", "s3://my_bucket", true, ver, cfg)

        @test S3Path("s3://my_bucket?versionId=$ver") ==
            S3Path((), "", "s3://my_bucket", true, ver, cfg)

        @test S3Path("s3://my_bucket/prefix/path/?versionId=$ver&radtimes=foo") ==
            S3Path(("prefix", "path"), "/", "s3://my_bucket", true, ver, cfg)

        @test S3Path("s3://my_bucket/prefix/path/?radtimes=foo&versionId=$ver") ==
            S3Path(("prefix", "path"), "/", "s3://my_bucket", true, ver, cfg)

        @test S3Path("s3://my_bucket/prefix/path?versionId=null") ==
            S3Path(("prefix", "path"), "/", "s3://my_bucket", false, "null", cfg)

        # Test to mark inconsistent root string behaviour when reconstructing parsed paths.
        parsed = tryparse(S3Path, "s3://my_bucket")
        @test_broken parsed == S3Path(
            parsed.bucket, parsed.key; version=parsed.version, config=parsed.config
        )

        @test_throws ArgumentError S3Path("s3://my_bucket/?versionId=")
        @test_throws ArgumentError S3Path("s3://my_bucket/?versionId=xyz")
    end

    @testset "version is empty" begin
        @test_throws ArgumentError S3Path("my_bucket", "path"; version="")
        @test_throws ArgumentError S3Path("s3://my_bucket/"; version="")
    end

    # `s3_list_versions` gives `SignatureDoesNotMatch` exceptions on Minio
    if is_aws(base_config)
        @testset "S3Path versioning" begin
            config = assume_testset_role("S3PathVersioningTestset"; base_config)

            s3_enable_versioning(config, bucket_name)
            key = "test_versions"
            r1 = s3_put(config, bucket_name, key, "data.v1"; parse_response=false)
            r2 = s3_put(config, bucket_name, key, "data.v2"; parse_response=false)
            rv1 = HTTP.header(r1.headers, "x-amz-version-id", nothing)
            rv2 = HTTP.header(r2.headers, "x-amz-version-id", nothing)

            # `s3_list_versions` returns versions in the order newest to oldest
            listed_versions = s3_list_versions(config, bucket_name, key)
            versions = [d["VersionId"] for d in reverse!(listed_versions)]

            v1, v2 = first(versions), last(versions)
            @test v1 == rv1
            @test v2 == rv2
            @test read(S3Path(bucket_name, key; config, version=v1), String) == "data.v1"
            @test read(S3Path(bucket_name, key; config, version=v2), String) == "data.v2"
            @test read(S3Path(bucket_name, key; config, version=v2), String) ==
                read(S3Path(bucket_name, key; config), String)
            @test read(S3Path(bucket_name, key; config, version=v2), String) ==
                read(S3Path(bucket_name, key; config, version=nothing), String)

            unversioned_path = S3Path(bucket_name, key; config)
            versioned_path = S3Path(bucket_name, key; config, version=v2)
            @test versioned_path.version == v2
            @test unversioned_path.version === nothing
            @test exists(versioned_path)
            @test exists(unversioned_path)

            dne = "feVMBvDgNiKSpMS17fKNJK3GV05bl8ir"
            dne_versioned_path = S3Path(bucket_name, key; config, version=dne)
            @test !exists(dne_versioned_path)

            versioned_path_v1 = S3Path("s3://$bucket_name/$key"; version=v1)
            versioned_path_v2 = S3Path("s3://$bucket_name/$key"; version=v2)
            @test versioned_path_v1.version == v1
            @test versioned_path_v1 != unversioned_path
            @test versioned_path_v1 != versioned_path_v2

            versioned_path_v1_from_url = S3Path("s3://$bucket_name/$key?versionId=$v1")
            @test versioned_path_v1_from_url.key == key
            @test versioned_path_v1_from_url.version == v1
            @test S3Path("s3://$bucket_name/$key?versionId=$v1"; version=v1).version == v1
            @test_throws ArgumentError begin
                S3Path("s3://$bucket_name/$key?versionId=$v1"; version=v2)
            end

            str_v1 = string(versioned_path_v1)
            roundtripped_v1 = S3Path(str_v1; config)
            @test isequal(versioned_path_v1, roundtripped_v1)
            @test str_v1 == "s3://" * bucket_name * "/" * key * "?versionId=" * v1

            @test isa(stat(versioned_path), Status)
            @test_throws ArgumentError write(versioned_path, "new_content")

            rm(versioned_path)
            @test !exists(versioned_path)
            @test length(s3_list_versions(config, bucket_name, key)) == 1

            fp = S3Path(bucket_name, "test_versions_deleteall"; config)
            foreach(_ -> write(fp, "foo"), 1:6)
            @test length(s3_list_versions(fp.config, fp.bucket, fp.key)) == 6
            s3_nuke_object(fp)
            @test length(s3_list_versions(fp.config, fp.bucket, fp.key)) == 0
            @test !exists(fp)
        end

        @testset "S3Path null version" begin
            config = assume_testset_role("S3PathNullVersionTestset"; base_config)

            b = gen_bucket_name("awss3.jl.test.null.")
            k = "object"

            function versioning_enabled(config, bucket)
                d = parse(S3.get_bucket_versioning(bucket; aws_config=config))
                return get(d, "Status", "Disabled") == "Enabled"
            end

            function list_version_ids(args...)
                return [d["VersionId"] for d in reverse!(s3_list_versions(args...))]
            end

            try
                # Create a new bucket that we know does not have versioning enabled
                s3_create_bucket(config, b)
                @test !versioning_enabled(config, b)

                # Create an object which will have versionId set to "null"
                r1 = s3_put(config, b, k, "original"; parse_response=false)
                rv1 = HTTP.header(r1.headers, "x-amz-version-id", nothing)
                @test isnothing(rv1)

                versions = list_version_ids(config, b, k)
                @test length(versions) == 1
                @test versions[1] == "null"
                @test read(S3Path(b, k; config, version=versions[1])) == b"original"

                s3_enable_versioning(config, b)
                @test versioning_enabled(config, b)

                # Overwrite the original object with a new version
                r2 = s3_put(config, b, k, "new and improved!"; parse_response=false)
                rv2 = HTTP.header(r2.headers, "x-amz-version-id", nothing)
                @test !isnothing(rv2)

                versions = list_version_ids(config, b, k)
                @test length(versions) == 2
                @test versions[1] == "null"
                @test versions[2] != "null"
                @test versions[2] == rv2
                @test read(S3Path(b, k; config, version=versions[1])) == b"original"
                @test read(S3Path(b, k; config, version=versions[2])) ==
                    b"new and improved!"
            finally
                AWSS3.s3_nuke_bucket(config, b)
            end
        end
    end

    # <https://github.com/JuliaCloud/AWSS3.jl/issues/168>
    @testset "Default `S3Path` does not freeze config" begin
        path = S3Path("s3://$(bucket_name)/test_str.txt")
        @test path.config === nothing
        @test AWSS3.get_config(path) !== nothing
    end

    @testset "No-op constructor" begin
        path = S3Path("s3://$(bucket_name)/test_str.txt")
        path2 = S3Path(path)
        @test path == path2
    end

    # MinIO does not care about regions, so this test doesn't work there
    if is_aws(base_config)
        @testset "Global config is not frozen at construction time" begin
            config = assume_testset_role("ReadWriteObject"; base_config)

            with_aws_config(config) do
                # Setup: create a file holding a string `abc`
                path = S3Path("s3://$(bucket_name)/test_str.txt")
                write(path, "abc")
                @test read(path, String) == "abc"  # Have access to read file

                alt_region = config.region == "us-east-2" ? "us-east-1" : "us-east-2"
                alt_config = AWSConfig(; region=alt_region) # this is the wrong region!

                with_aws_config(alt_config) do
                    @test_throws AWS.AWSException read(path, String)
                end

                # Now it works, without recreating `path`
                @test read(path, String) == "abc"
                rm(path)
            end
        end
    end

    # Broken on MinIO
    if is_aws(base_config)
        config = assume_testset_role("NukeBucket"; base_config)
        AWSS3.s3_nuke_bucket(config, bucket_name)
    end
end
