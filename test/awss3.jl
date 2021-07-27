bucket_name = "ocaws.jl.test." * lowercase(Dates.format(now(Dates.UTC), "yyyymmddTHHMMSSZ"))

@testset "Create Bucket" begin
    s3_create_bucket(aws, bucket_name)
    @test bucket_name in s3_list_buckets(aws)
    s3_enable_versioning(aws, bucket_name)
    sleep(1)
end

@testset "Bucket Tagging" begin
    @test isempty(s3_get_tags(aws, bucket_name))
    tags = Dict("A" => "1", "B" => "2", "C" => "3")
    s3_put_tags(aws, bucket_name, tags)
    @test s3_get_tags(aws, bucket_name) == tags
    s3_delete_tags(aws, bucket_name)
    @test isempty(s3_get_tags(aws, bucket_name))
end

@testset "Create Objects" begin
    s3_put(aws, bucket_name, "key1", "data1.v1")
    s3_put(bucket_name, "key2", "data2.v1", tags = Dict("Key" => "Value"))
    s3_put(aws, bucket_name, "key3", "data3.v1")
    s3_put(aws, bucket_name, "key3", "data3.v2")
    s3_put(aws, bucket_name, "key3", "data3.v3"; metadata = Dict("foo" => "bar"))
    s3_put(aws, bucket_name, "key4", "data3.v4"; acl="bucket-owner-full-control")
    s3_put_tags(aws, bucket_name, "key3", Dict("Left" => "Right"))

    @test isempty(s3_get_tags(aws, bucket_name, "key1"))
    @test s3_get_tags(aws, bucket_name, "key2")["Key"] == "Value"
    @test s3_get_tags(aws, bucket_name, "key3")["Left"] == "Right"
    s3_delete_tags(aws, bucket_name, "key2")
    @test isempty(s3_get_tags(aws, bucket_name, "key2"))

    @test s3_get(aws, bucket_name, "key1") == b"data1.v1"
    @test s3_get(aws, bucket_name, "key2") == b"data2.v1"
    @test s3_get(bucket_name, "key3") == b"data3.v3"
    @test s3_get(bucket_name, "key4") == b"data3.v4"
    @test s3_get_meta(bucket_name, "key3")["x-amz-meta-foo"] == "bar"
end

@testset "ASync Get" begin
    @sync begin
        for i in 1:2
            @async begin
                @test s3_get(bucket_name, "key3") == b"data3.v3"
            end
        end
    end
end

@testset "Raw Return - XML" begin
    xml = "<?xml version='1.0'?><Doc><Text>Hello</Text></Doc>"
    s3_put(aws, bucket_name, "file.xml", xml, "text/xml")
    @test String(s3_get(aws, bucket_name, "file.xml", raw=true)) == xml
    @test s3_get(aws, bucket_name, "file.xml")["Text"] == "Hello"
end

@testset "Get byte range" begin
    teststr = "123456789"
    s3_put(aws, bucket_name, "byte_range", teststr)
    range = 3:6
    @test String(s3_get(aws, bucket_name, "byte_range"; byte_range=range)) == teststr[range]
end

@testset "Object Copy" begin
    s3_copy(bucket_name, "key1"; to_bucket=bucket_name, to_path="key1.copy")
    @test s3_get(aws, bucket_name, "key1.copy") == b"data1.v1"
end

minio || @testset "Sign URL" begin
    for v in ["v2", "v4"]
        url = s3_sign_url(aws, bucket_name, "key1"; signature_version=v)
        curl_output = ""

        @repeat 3 try
            curl_output = read(`curl -s -o - $url`, String)
        catch e
            @delay_retry if true end
        end

        @test curl_output == "data1.v1"

        fn = "/tmp/jl_qws_test_key1"
        if isfile(fn)
            rm(fn)
        end

        @repeat 3 try
            s3_get_file(aws, bucket_name, "key1", fn)
        catch e
            sleep(1)
            @retry if true end
        end

        @test read(fn, String) == "data1.v1"
        rm(fn)
    end
end

@testset "Object exists" begin
    for key in ["key1", "key2", "key3", "key1.copy"]
        @test s3_exists(bucket_name, key)
    end
end

@testset "List Objects" begin
    for key in ["key1", "key2", "key3", "key1.copy"]
        @test key in [o["Key"] for o in s3_list_objects(aws, bucket_name)]
    end
end

@testset "Object Delete" begin
    s3_delete(aws, bucket_name, "key1.copy")
    @test !("key1.copy" in [o["Key"] for o in s3_list_objects(aws, bucket_name)])
end

@testset "Check Metadata" begin
    meta = s3_get_meta(aws, bucket_name, "key1")
    @test meta["ETag"] == "\"68bc8898af64159b72f349b391a7ae35\""
end

minio || @testset "Check Object Versions" begin
    versions = s3_list_versions(aws, bucket_name, "key3")
    @test length(versions) == 3
    @test (s3_get(aws, bucket_name, "key3"; version=versions[3]["VersionId"]) == b"data3.v1")
    @test (s3_get(aws, bucket_name, "key3"; version=versions[2]["VersionId"]) == b"data3.v2")
    @test (s3_get(aws, bucket_name, "key3"; version=versions[1]["VersionId"]) == b"data3.v3")
end

minio || @testset "Purge Versions" begin
    s3_purge_versions(aws, bucket_name, "key3")
    versions = s3_list_versions(aws, bucket_name, "key3")
    @test length(versions) == 1
    @test s3_get(aws, bucket_name, "key3") == b"data3.v3"
end

@testset "default Content-Type" begin
    # https://github.com/samoconnor/AWSS3.jl/issues/24
    ctype(key) = s3_get_meta(bucket_name, key)["Content-Type"]

    for k in [
        "file.foo",
        "file",
        "file_html",
        "file/html",
        "foobar.html/file.htm"]
        minio && k == "file" && continue
        s3_put(aws, bucket_name, k, "x")
        @test ctype(k) == "application/octet-stream"
    end

    for (k, t) in [
        ("foo/bar/file.html",  "text/html"),
        ("x.y.z.js",           "application/javascript"),
        ("downalods/foo.pdf",  "application/pdf"),
        ("data/foo.csv",       "text/csv"),
        ("this.is.a.file.txt", "text/plain"),
        ("my.log",             "text/plain"),
        ("big.dat",            "application/octet-stream"),
        ("some.tar.gz",        "application/octet-stream"),
        ("data.bz2",           "application/octet-stream")]

        s3_put(aws, bucket_name, k, "x")
        @test ctype(k) == t
    end
end

@testset "Multi-Part Upload" begin
    MIN_S3_CHUNK_SIZE = 5 * 1024 * 1024 # 5 MB
    key_name = "multi-part-key"
    upload = s3_begin_multipart_upload(aws, bucket_name, key_name)
    tags = Vector{String}()

    for part_number in 1:5
        push!(tags, s3_upload_part(aws, upload, part_number, rand(UInt8, MIN_S3_CHUNK_SIZE)))
    end

    s3_complete_multipart_upload(aws, upload, tags)
    @test s3_exists(bucket_name, key_name)
end

if !minio
    @testset "Empty and Delete Bucket" begin
        AWSS3.s3_nuke_bucket(aws, bucket_name)
        @test !in(bucket_name, s3_list_buckets(aws))
    end

    @testset "Delete Non-Existant Bucket" begin
        @test_throws AWS.AWSException s3_delete_bucket(aws, bucket_name)
    end
end
