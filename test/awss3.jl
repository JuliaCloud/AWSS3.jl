function awss3_tests(base_config)
    bucket_name = gen_bucket_name()

    @testset "Robust key selection" begin
        lower_dict = Dict("foo-bar" => 1)
        upper_dict = Dict("Foo-Bar" => 1)
        @test AWSS3.get_robust_case(lower_dict, "Foo-Bar") == 1
        @test AWSS3.get_robust_case(upper_dict, "Foo-Bar") == 1
        @test_throws KeyError("Foo-Bar") AWSS3.get_robust_case(Dict(), "Foo-Bar")
    end

    @testset "Create Bucket" begin
        config = assume_testset_role("CreateBucketTestset"; base_config)
        s3_create_bucket(config, bucket_name)
        @test bucket_name in s3_list_buckets(config)
        is_aws(config) && s3_enable_versioning(config, bucket_name)
        sleep(1)
    end

    @testset "Bucket Tagging" begin
        config = assume_testset_role("BucketTaggingTestset"; base_config)
        @test isempty(s3_get_tags(config, bucket_name))
        tags = Dict("A" => "1", "B" => "2", "C" => "3")
        s3_put_tags(config, bucket_name, tags)
        @test s3_get_tags(config, bucket_name) == tags
        s3_delete_tags(config, bucket_name)
        @test isempty(s3_get_tags(config, bucket_name))
    end

    @testset "Create Objects" begin
        config = assume_testset_role("CreateObjectsTestset"; base_config)
        global_aws_config(config)

        s3_put(config, bucket_name, "key1", "data1.v1")
        s3_put(bucket_name, "key2", "data2.v1"; tags=Dict("Key" => "Value"))
        s3_put(config, bucket_name, "key3", "data3.v1")
        s3_put(config, bucket_name, "key3", "data3.v2")
        s3_put(config, bucket_name, "key3", "data3.v3"; metadata=Dict("foo" => "bar"))
        s3_put(config, bucket_name, "key4", "data3.v4"; acl="bucket-owner-full-control")
        s3_put_tags(config, bucket_name, "key3", Dict("Left" => "Right"))

        @test isempty(s3_get_tags(config, bucket_name, "key1"))
        @test s3_get_tags(config, bucket_name, "key2")["Key"] == "Value"
        @test s3_get_tags(config, bucket_name, "key3")["Left"] == "Right"
        s3_delete_tags(config, bucket_name, "key2")
        @test isempty(s3_get_tags(config, bucket_name, "key2"))

        @test s3_get(config, bucket_name, "key1") == b"data1.v1"
        @test s3_get(config, bucket_name, "key2") == b"data2.v1"
        @test s3_get(bucket_name, "key3") == b"data3.v3"
        @test s3_get(bucket_name, "key4") == b"data3.v4"

        try
            s3_get(config, bucket_name, "key5")
            @test false
        catch e
            e isa AWSException || rethrow()

            # Will see a 403 status if we lack the `s3:ListBucket` permission.
            @test e.cause.status == 404
        end

        @test s3_get_meta(bucket_name, "key3")["x-amz-meta-foo"] == "bar"

        @test isa(
            s3_put(config, bucket_name, "key6", "data"; parse_response=false), AWS.Response
        )
    end

    @testset "ASync Get" begin
        config = assume_testset_role("ReadObject"; base_config)
        @sync begin
            for i in 1:2
                @async begin
                    @test s3_get(bucket_name, "key3") == b"data3.v3"
                end
            end
        end
    end

    @testset "Raw Return - XML" begin
        config = assume_testset_role("ReadWriteObject"; base_config)
        xml = "<?xml version='1.0'?><Doc><Text>Hello</Text></Doc>"
        @test s3_put(config, bucket_name, "file.xml", xml, "text/xml") == UInt8[]
        @test String(s3_get(config, bucket_name, "file.xml"; raw=true)) == xml
        @test s3_get(config, bucket_name, "file.xml")["Text"] == "Hello"
    end

    @testset "Get byte range" begin
        config = assume_testset_role("ReadWriteObject"; base_config)
        teststr = "123456789"
        s3_put(config, bucket_name, "byte_range", teststr)
        range = 3:6
        @test String(s3_get(config, bucket_name, "byte_range"; byte_range=range)) ==
            teststr[range]
    end

    @testset "Object Copy" begin
        config = assume_testset_role("ReadWriteObject"; base_config)
        result = s3_copy(
            config, bucket_name, "key1"; to_bucket=bucket_name, to_path="key1.copy"
        )
        @test result isa AbstractDict
        @test s3_get(config, bucket_name, "key1.copy") == b"data1.v1"

        result = s3_copy(
            config,
            bucket_name,
            "key1";
            to_bucket=bucket_name,
            to_path="key1.copy",
            parse_response=false,
        )
        @test result isa AWS.Response

        if is_aws(base_config)
            @test !isnothing(HTTP.header(result.headers, "x-amz-version-id", nothing))
        end
    end

    @testset "Object exists" begin
        config = assume_testset_role("ReadObject"; base_config)
        for key in ["key1", "key2", "key3", "key1.copy"]
            @test s3_exists(config, bucket_name, key)
        end
    end

    @testset "List Objects" begin
        config = assume_testset_role("ReadObject"; base_config)
        for key in ["key1", "key2", "key3", "key1.copy"]
            @test key in [o["Key"] for o in s3_list_objects(config, bucket_name)]
        end
    end

    @testset "Object Delete" begin
        config = assume_testset_role("ReadWriteObject"; base_config)
        s3_delete(config, bucket_name, "key1.copy")
        @test !("key1.copy" in [o["Key"] for o in s3_list_objects(config, bucket_name)])
    end

    @testset "Check Metadata" begin
        config = assume_testset_role("ReadObject"; base_config)
        meta = s3_get_meta(config, bucket_name, "key1")
        @test meta["ETag"] == "\"68bc8898af64159b72f349b391a7ae35\""
    end

    # https://github.com/samoconnor/AWSS3.jl/issues/24
    @testset "default Content-Type" begin
        config = assume_testset_role("ReadWriteObject"; base_config)
        ctype(key) = s3_get_meta(config, bucket_name, key)["Content-Type"]

        for k in ["file.foo", "file", "file_html", "file.d/html", "foobar.html/file.htm"]
            is_aws(config) && k == "file" && continue
            s3_put(config, bucket_name, k, "x")
            @test ctype(k) == "application/octet-stream"
        end

        for (k, t) in [
            ("foo/bar/file.html", "text/html"),
            ("x.y.z.js", "application/javascript"),
            ("downalods/foo.pdf", "application/pdf"),
            ("data/foo.csv", "text/csv"),
            ("this.is.a.file.txt", "text/plain"),
            ("my.log", "text/plain"),
            ("big.dat", "application/octet-stream"),
            ("some.tar.gz", "application/octet-stream"),
            ("data.bz2", "application/octet-stream"),
        ]
            s3_put(config, bucket_name, k, "x")
            @test ctype(k) == t
        end
    end

    @testset "Multi-Part Upload" begin
        config = assume_testset_role("MultipartUploadTestset"; base_config)
        MIN_S3_CHUNK_SIZE = 5 * 1024 * 1024 # 5 MB
        key_name = "multi-part-key"
        upload = s3_begin_multipart_upload(config, bucket_name, key_name)
        tags = Vector{String}()

        for part_number in 1:5
            push!(
                tags,
                s3_upload_part(config, upload, part_number, rand(UInt8, MIN_S3_CHUNK_SIZE)),
            )
        end

        result = s3_complete_multipart_upload(config, upload, tags)
        @test s3_exists(config, bucket_name, key_name)
        @test isa(result, LittleDict)
    end

    @testset "Multi-Part Upload, return unparsed path" begin
        config = assume_testset_role("MultipartUploadTestset"; base_config)
        MIN_S3_CHUNK_SIZE = 5 * 1024 * 1024 # 5 MB
        key_name = "multi-part-key"
        upload = s3_begin_multipart_upload(config, bucket_name, key_name)
        tags = Vector{String}()

        for part_number in 1:5
            push!(
                tags,
                s3_upload_part(config, upload, part_number, rand(UInt8, MIN_S3_CHUNK_SIZE)),
            )
        end

        result = s3_complete_multipart_upload(config, upload, tags; parse_response=false)
        @test s3_exists(config, bucket_name, key_name)
        @test isa(result, AWS.Response)
    end

    # these tests are needed because lack of functionality of the underlying AWS API makes certain
    # seemingly inane tasks incredibly tricky: for example checking if an "object" (file or
    # directory) exists is very subtle
    @testset "path naming edge cases" begin
        config = assume_testset_role("ReadWriteObject"; base_config)

        # this seemingly arbitrary operation is needed because of the insanely tricky way we
        # need to check for directories
        s3_put(config, bucket_name, "testdir.", "") # create an empty file called `testdir.`
        s3_put(config, bucket_name, "testdir/", "") # create an empty file called `testdir/` which AWS will treat as an "empty directory"
        @test s3_exists(config, bucket_name, "testdir/")
        @test isdir(S3Path(bucket_name, "testdir/"; config))
        @test !isfile(S3Path(bucket_name, "testdir/"; config))
        @test s3_exists(config, bucket_name, "testdir.")
        @test isfile(S3Path(bucket_name, "testdir."; config))
        @test !isdir(S3Path(bucket_name, "testdir."; config))
        @test !s3_exists(config, bucket_name, "testdir")

        s3_put(config, bucket_name, "testdir/testfile.txt", "what up")
        @test s3_exists(config, bucket_name, "testdir/testfile.txt")
        @test isfile(S3Path(bucket_name, "testdir/testfile.txt"; config))
        # make sure the directory still "exists" even though there's a key in there now
        @test s3_exists(config, bucket_name, "testdir/")
        @test isdir(S3Path(bucket_name, "testdir/"; config))
        @test !isfile(S3Path(bucket_name, "testdir/"; config))

        # but it is still a directory and not an object
        @test !s3_exists(config, bucket_name, "testdir")
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
        @test s3_exists(config, bucket_name, "prefix/granted/file")
        @test !s3_exists(config, bucket_name, "prefix/granted/dne")
        @test_throws_msg ["AccessDenied", "403"] begin
            s3_exists(config, bucket_name, "prefix/denied/top-secret")
        end
        @test s3_exists(config, bucket_name, "prefix/granted/")
        @test s3_exists(config, bucket_name, "prefix/")

        # Ensure that `s3_list_objects` works with restricted prefixes
        @test length(collect(s3_list_objects(config, bucket_name, "prefix/granted/"))) == 1
        @test length(collect(s3_list_objects(config, bucket_name, "prefix/"))) == 0

        # Validate that we have permissions to list the root without encountering an access error.
        # Ideally we just want `@test_no_throws s3_list_objects(config, bucket_name)`.
        @test length(collect(s3_list_objects(config, bucket_name))) >= 0
    end

    @testset "Version is empty" begin
        config = assume_testset_role("ReadWriteObject"; base_config)

        # Create the file to ensure we're only testing `version`
        k = "version_empty.txt"
        s3_put(config, bucket_name, k, "v1")
        s3_put(config, bucket_name, k, "v2")

        if is_aws(config)
            @test_throws AWSException s3_get(config, bucket_name, k; version="")
            @test_throws AWSException s3_get_meta(config, bucket_name, k; version="")
            @test_throws AWSException s3_exists(config, bucket_name, k; version="")
            @test_throws AWSException s3_delete(config, bucket_name, k; version="")
        else
            # Using an empty string as the version returns the latest version
            @test s3_get(config, bucket_name, k; version="") == "v2"
            @test s3_get_meta(config, bucket_name, k; version="") isa AbstractDict
            @test s3_exists(config, bucket_name, k; version="")
            @test s3_delete(config, bucket_name, k; version="") == UInt8[]
        end
    end

    @testset "Version is nothing" begin
        config = assume_testset_role("ReadWriteObject"; base_config)

        # Create the file to ensure we're only testing `version`
        k = "version_nothing.txt"
        s3_put(config, bucket_name, k, "v1")
        s3_put(config, bucket_name, k, "v2")

        # Using an empty string as the version returns the latest version
        @test s3_get(config, bucket_name, k; version=nothing) == "v2"
        @test s3_get_meta(config, bucket_name, k; version=nothing) isa AbstractDict
        @test s3_exists(config, bucket_name, k; version=nothing)
        @test s3_delete(config, bucket_name, k; version=nothing) == UInt8[]
    end

    is_aws(base_config) && @testset "Sign URL" begin
        config = assume_testset_role("SignUrlTestset"; base_config)
        for v in ["v2", "v4"]
            url = s3_sign_url(config, bucket_name, "key1"; signature_version=v)
            curl_output = ""

            @repeat 3 try
                curl_output = read(`curl -s -o - $url`, String)
            catch e
                @delay_retry if true
                end
            end

            @test curl_output == "data1.v1"

            fn = "/tmp/jl_qws_test_key1"
            if isfile(fn)
                rm(fn)
            end

            @repeat 3 try
                s3_get_file(config, bucket_name, "key1", fn)
            catch e
                sleep(1)
                @retry if true
                end
            end

            @test read(fn, String) == "data1.v1"
            rm(fn)
        end
    end

    is_aws(base_config) && @testset "Check Object Versions" begin
        config = assume_testset_role("ReadObjectVersion"; base_config)
        versions = s3_list_versions(config, bucket_name, "key3")
        @test length(versions) == 3
        @test (
            s3_get(config, bucket_name, "key3"; version=versions[3]["VersionId"]) ==
            b"data3.v1"
        )
        @test (
            s3_get(config, bucket_name, "key3"; version=versions[2]["VersionId"]) ==
            b"data3.v2"
        )
        @test (
            s3_get(config, bucket_name, "key3"; version=versions[1]["VersionId"]) ==
            b"data3.v3"
        )

        tmp_file = joinpath(tempdir(), "jl_qws_test_key3")
        s3_get_file(config, bucket_name, "key3", tmp_file; version=versions[2]["VersionId"])
        @test read(tmp_file) == b"data3.v2"
    end

    is_aws(base_config) && @testset "Purge Versions" begin
        config = assume_testset_role("PurgeVersionsTestset"; base_config)
        s3_purge_versions(config, bucket_name, "key3")
        versions = s3_list_versions(config, bucket_name, "key3")
        @test length(versions) == 1
        @test s3_get(config, bucket_name, "key3") == b"data3.v3"
    end

    is_aws(base_config) && @testset "Delete All Versions" begin
        config = assume_testset_role("NukeObjectTestset"; base_config)
        key_to_delete = "NukeObjectTestset_key"
        # Test that object that starts with the same prefix as `key_to_delete` is
        # not _also_ deleted
        key_not_to_delete = "NukeObjectTestset_key/rad"

        function _s3_object_versions(config, bucket, key)
            return filter!(x -> x["Key"] == key, s3_list_versions(config, bucket, key))
        end

        s3_put(config, bucket_name, key_to_delete, "foo.v1")
        s3_put(config, bucket_name, key_to_delete, "foo.v2")
        s3_put(config, bucket_name, key_to_delete, "foo.v3")
        s3_put(config, bucket_name, key_not_to_delete, "rad.v1")
        s3_put(config, bucket_name, key_not_to_delete, "rad.v2")

        @test length(_s3_object_versions(config, bucket_name, key_to_delete)) == 3
        @test length(_s3_object_versions(config, bucket_name, key_not_to_delete)) == 2

        s3_nuke_object(config, bucket_name, key_to_delete)
        @test length(_s3_object_versions(config, bucket_name, key_to_delete)) == 0

        # Test that _only_ specific path was deleted---not paths at the same prefix
        @test length(_s3_object_versions(config, bucket_name, key_not_to_delete)) == 2
    end

    if is_aws(base_config)
        @testset "Empty and Delete Bucket" begin
            config = assume_testset_role("EmptyAndDeleteBucketTestset"; base_config)
            AWSS3.s3_nuke_bucket(config, bucket_name)
            @test !in(bucket_name, s3_list_buckets(config))
        end

        @testset "Delete Non-Existent Bucket" begin
            config = assume_testset_role("DeleteNonExistentBucketTestset"; base_config)
            @test_throws AWS.AWSException s3_delete_bucket(config, bucket_name)
        end
    end
end
