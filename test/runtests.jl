#==============================================================================#
# AWSS3/test/runtests.jl
#
# Copyright OC Technology Pty Ltd 2014 - All rights reserved
#==============================================================================#


using AWSS3
using Test
using Dates
using AWSCore
using Retry
using HTTP

AWSCore.set_debug_level(1)

function test_without_catch(f)

    @protected try
        f()
    catch e
        @ignore if isa(e, Test.Error)
            e = e.err
            rethrow(e)
        end
    end
end



#-------------------------------------------------------------------------------
# Load credentials...
#-------------------------------------------------------------------------------

aws = AWSCore.default_aws_config()
aws[:region] = "ap-southeast-2"



#-------------------------------------------------------------------------------
# S3 tests
#-------------------------------------------------------------------------------

# Delete old test files...

for b in s3_list_buckets()

    if occursin(r"^ocaws.jl.test", b)
        @protected try
            println("Cleaning up old test bucket: " * b)
            @sync for v in s3_list_versions(aws, b)
                @async s3_delete(aws, b, v["Key"]; version = v["VersionId"])
            end
            s3_delete_bucket(aws, b)
        catch e
            @ignore if isa(e, AWSCore.AWSException) &&
                       e.code == "NoSuchBucket" end
        end
    end
end

HTTP.ConnectionPool.showpool(stdout)

# Temporary bucket name...

bucket_name = "ocaws.jl.test." * lowercase(Dates.format(now(Dates.UTC),
                                                        "yyyymmddTHHMMSSZ"))


# Test exception code for deleting non existent bucket...

@protected try

    s3_delete_bucket(aws, bucket_name)

catch e
     @ignore if isa(e, AWSCore.AWSException) &&
                e.code == "NoSuchBucket" end
end


# Create bucket...

s3_create_bucket(aws, bucket_name)


@repeat 4 try

    # Turn on object versioning for this bucket...

    s3_enable_versioning(aws, bucket_name)

    # Check that the new bucket is returned in the list of buckets...

    @test bucket_name in s3_list_buckets(aws)

    test_without_catch() do
        # Check that our test keys do not exist yet...
        @test !s3_exists(aws, bucket_name, "key 1")
        @test !s3_exists(aws, bucket_name, "key2")
        @test !s3_exists(aws, bucket_name, "key3")
    end

catch e
    @delay_retry if isa(e, AWSCore.AWSException) &&
                    e.code == "NoSuchBucket" end
end


# Bucket tagging...

@test isempty(s3_get_tags(aws, bucket_name))
tags = Dict("A" => "1", "B" => "2", "C" => "3")
s3_put_tags(aws, bucket_name, tags)
@test s3_get_tags(aws, bucket_name) == tags
s3_delete_tags(aws, bucket_name)
@test isempty(s3_get_tags(aws, bucket_name))

# Create test objects...

s3_put(aws, bucket_name, "key 1", "data1.v1")
s3_put(bucket_name, "key2", "data2.v1", tags = Dict("Key" => "Value"))
s3_put(aws, bucket_name, "key3", "data3.v1")
s3_put(aws, bucket_name, "key3", "data3.v2")
s3_put(aws, bucket_name, "key3", "data3.v3"; metadata = Dict("foo" => "bar"))
s3_put_tags(aws, bucket_name, "key3", Dict("Left" => "Right"))

@test isempty(s3_get_tags(aws, bucket_name, "key 1"))
@test s3_get_tags(aws, bucket_name, "key2")["Key"] == "Value"
@test s3_get_tags(aws, bucket_name, "key3")["Left"] == "Right"
s3_delete_tags(aws, bucket_name, "key2")
@test isempty(s3_get_tags(aws, bucket_name, "key2"))


# Check that test objects have expected content...

@test s3_get(aws, bucket_name, "key 1") == b"data1.v1"
@test s3_get(aws, bucket_name, "key2") == b"data2.v1"
@test s3_get(bucket_name, "key3") == b"data3.v3"
@test s3_get_meta(bucket_name, "key3")["x-amz-meta-foo"] == "bar"

@testset "test coroutine reading" begin
    @sync begin
        for i in 1:2
            @async begin
                @test s3_get(bucket_name, "key3") == b"data3.v3"
                println("success ID: $i")
            end
        end
    end
end

# Check raw return of XML object...
xml = "<?xml version='1.0'?><Doc><Text>Hello</Text></Doc>"
s3_put(aws, bucket_name, "file.xml", xml, "text/xml")
@test String(s3_get(aws, bucket_name, "file.xml", raw=true)) == xml
@test s3_get(aws, bucket_name, "file.xml")["Text"] == "Hello"

# Check object copy function...

s3_copy(bucket_name, "key 1";
        to_bucket = bucket_name, to_path = "key 1.copy")

@test s3_get(aws, bucket_name, "key 1.copy") == b"data1.v1"


url = s3_sign_url(aws, bucket_name, "key 1")
curl_output = ""
@repeat 3 try
    global curl_output = read(`curl -s -o - $url`, String)
catch e
    @delay_retry if true end
end
@test curl_output == "data1.v1"

fn = "/tmp/jl_qws_test_key1"
if isfile(fn)
    rm(fn)
end
@repeat 3 try
    s3_get_file(aws, bucket_name, "key 1", fn)
catch e
    sleep(1)
    @retry if true end
end
@test read(fn, String) == "data1.v1"
rm(fn)


# Check exists and list objects functions...

for key in ["key 1", "key2", "key3", "key 1.copy"]
    @test s3_exists(bucket_name, key)
    @test key in [o["Key"] for o in s3_list_objects(aws, bucket_name)]
end

# Check delete...

s3_delete(aws, bucket_name, "key 1.copy")

@test !("key 1.copy" in [o["Key"] for o in s3_list_objects(aws, bucket_name)])

# Check metadata...

meta = s3_get_meta(aws, bucket_name, "key 1")
@test meta["ETag"] == "\"68bc8898af64159b72f349b391a7ae35\""


# Check versioned object content...

versions = s3_list_versions(aws, bucket_name, "key3")
@test length(versions) == 3
@test (s3_get(aws, bucket_name, "key3"; version = versions[3]["VersionId"])
      == b"data3.v1")
@test (s3_get(aws, bucket_name, "key3"; version = versions[2]["VersionId"])
      == b"data3.v2")
@test (s3_get(aws, bucket_name, "key3"; version = versions[1]["VersionId"])
      == b"data3.v3")


@testset "default Content-Type" begin
# https://github.com/samoconnor/AWSS3.jl/issues/24

    ctype(key) = s3_get_meta(bucket_name, key)["Content-Type"]

    for k in [
        "file.foo",
        "file",
        "file_html",
        "file/html",
        "foobar.html/file.htm"]

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

# Check pruning of old versions...

s3_purge_versions(aws, bucket_name, "key3")
versions = s3_list_versions(aws, bucket_name, "key3")
@test length(versions) == 1
@test s3_get(aws, bucket_name, "key3") == b"data3.v3"


HTTP.ConnectionPool.showpool(stdout)

# Create objects...

max = 1000
sz = 10000
objs = [rand(UInt8(65):UInt8(75), sz) for i in 1:max]

asyncmap(x->AWSS3.s3(aws, "PUT", bucket_name;
                          path = "obj$(x[1])", content = x[2]),
         enumerate(objs);
         ntasks=30)
HTTP.ConnectionPool.showpool(stdout)

asyncmap(x->begin
    o = AWSS3.s3(aws, "GET", bucket_name; path = "obj$(x[1])")

    @test o == x[2]
end,
enumerate(objs);
ntasks=30)

HTTP.ConnectionPool.showpool(stdout)



#==============================================================================#
# End of file.
#==============================================================================#
