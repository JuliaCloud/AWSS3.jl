#==============================================================================#
# AWSS3.jl
#
# S3 API. See http://docs.aws.amazon.com/AmazonS3/latest/API/APIRest.html
#
# Copyright Sam O'Connor 2014 - All rights reserved
#==============================================================================#


__precompile__()


module AWSS3

export s3_arn, s3_put, s3_get, s3_get_file, s3_exists, s3_delete, s3_copy,
       s3_create_bucket,
       s3_enable_versioning, s3_delete_bucket, s3_list_buckets,
       s3_list_objects, s3_list_versions, s3_get_meta, s3_purge_versions,
       s3_sign_url


using AWSCore
using SymDict
using Retry
using XMLDict
using LightXML

import Requests: format_query_str


s3_arn(resource) = "arn:aws:s3:::$resource"
s3_arn(bucket, path) = s3_arn("$bucket/$path")


# S3 REST API request.

function s3(aws, verb, bucket="";
            headers=Dict(),
            path="",
            query=Dict(),
            version="",
            content="",
            return_stream=false)

    # Build query string...
    if version != ""
        @assert isa(query, Associative)
        query["versionId"] = version
    end
    if isa(query, Associative)
        query = format_query_str(query)
    end

    # Build URL...
    resource = "/$path$(query == "" ? "" : "?$query")"
    url = aws_endpoint("s3", "", bucket) * resource

    # Build Request...
    request = @SymDict(service = "s3",
                       verb,
                       url,
                       resource,
                       headers,
                       content,
                       return_stream,
                       aws...)

    @repeat 2 try

        # Check bucket region cache...
        try request[:region] = aws[:bucket_region][bucket] end

        do_request(request)

    catch e

        # Update bucket region cache if needed...
        @retry if typeof(e) == AWSCore.AuthorizationHeaderMalformed &&
                  haskey(e.info, "Region")

            if AWSCore.debug_level > 0
                println("S3 region redirect $bucket -> $(e.info["Region"])")
            end
            if !haskey(aws, :bucket_region)
                aws[:bucket_region] = Dict()
            end
            aws[:bucket_region][bucket] = e.info["Region"]
        end
    end
end


# See http://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectGET.html

function s3_get(aws, bucket, path; version="")

    @repeat 4 try

        r = s3(aws, "GET", bucket; path = path, version = version)
        return data(r)

    catch e
        @delay_retry if e.code in ["NoSuchBucket", "NoSuchKey"] end
    end
end


function s3_get_file(aws, bucket::AbstractString, path, filename; version="")

    stream = s3(aws, "GET", bucket; path = path,
                                    version = version,
                                    return_stream = true)

    try
        open(filename, "w") do file
            while !eof(stream)
                write(file, readavailable(stream))
            end
        end
    finally
        close(stream)
    end
end


function s3_get_file(aws, buckets::Vector, path, filename; version="")

    i = start(buckets)

    @repeat length(buckets) try

        bucket, i = next(buckets, i)
        s3_get_file(aws, bucket, path, filename; version=version);

    catch e
        @retry if e.code in ["NoSuchKey", "AccessDenied"] end
    end
end


function s3_get_meta(aws, bucket, path; version="")

    res = s3(aws, "GET", bucket;
             path = path,
             headers = Dict("Range" => "bytes=0-0"),
             version = version)
    return res.headers
end


function s3_exists(aws, bucket, path; version="")

    @repeat 2 try

        s3(aws, "GET", bucket; path = path,
                               headers = Dict("Range" => "bytes=0-0"),
                               version = version)
        return true

    catch e
        @delay_retry if e.code in ["NoSuchBucket", "NoSuchKey", "AccessDenied"]
        end
        @ignore if e.code in ["NoSuchKey", "AccessDenied"]
            return false
        end
    end
end


# See http://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectDELETE.html

function s3_delete(aws, bucket, path; version="")

    s3(aws, "DELETE", bucket; path = path, version = version)
end


# See http://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectCOPY.html

function s3_copy(aws, bucket, path; to_bucket=bucket, to_path="")

    s3(aws, "PUT", to_bucket;
                   path = to_path,
                   headers = Dict("x-amz-copy-source" => "/$bucket/$path"))
end


# See http://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketPUT.html

function s3_create_bucket(aws, bucket)

    println("""Creating Bucket "$bucket"...""")

    @protected try

        if aws[:region] == "us-east-1"

            s3(aws, "PUT", bucket)

        else

            s3(aws, "PUT", bucket;
                headers = Dict("Content-Type" => "text/plain"),
                content = """
                <CreateBucketConfiguration
                             xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                    <LocationConstraint>$(aws[:region])</LocationConstraint>
                </CreateBucketConfiguration>""")
        end

    catch e
        @ignore if e.code == "BucketAlreadyOwnedByYou" end
    end
end


# See http://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketPUTVersioningStatus.html

function s3_enable_versioning(aws, bucket)

    s3(aws, "PUT", bucket;
       query = Dict("versioning" => ""),
       content = """
       <VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
           <Status>Enabled</Status>
       </VersioningConfiguration>""")
end


# See http://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketDELETE.html

s3_delete_bucket(aws, bucket) = s3(aws, "DELETE", bucket)


# See http://docs.aws.amazon.com/AmazonS3/latest/API/RESTServiceGET.html

function s3_list_buckets(aws)

    r = s3(aws,"GET", headers=Dict("Content-Type" => "application/json"))
    [b["Name"] for b in r["Buckets"]["Bucket"]]
end


# See http://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketGET.html

function s3_list_objects(aws, bucket, path = "")

    more = true
    objects = []
    marker = ""

    while more

        q = Dict()
        if path != ""
            q["delimiter"] = "/"
            q["prefix"] = path
        end
        if marker != ""
            q["key-marker"] = marker
        end

        @repeat 4 try

            r = s3(aws, "GET", bucket; query = q)

            more = r["IsTruncated"] == "true"
            for object in r["Contents"]
                push!(objects, xml_dict(object))
                marker = object["Key"]
            end

        catch e
            @delay_retry if e.code in ["NoSuchBucket"] end
        end
    end

    return objects
end


# See http://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketGETVersion.html

function s3_list_versions(aws, bucket, path="")

    more = true
    versions = []
    marker = ""

    while more

        query = Dict("versions" => "", "prefix" => path)
        if marker != ""
            query["key-marker"] = marker
        end

        r = s3(aws, "GET", bucket; query = query)
        more = r["IsTruncated"][1] == "true"
        for e in child_elements(root(r.x))
            if name(e) in ["Version", "DeleteMarker"]
                version = xml_dict(e)
                version["state"] = name(e)
                push!(versions, version)
                marker = version["Key"]
            end
        end
    end
    return versions
end


import Base.ismatch
ismatch(pattern::AbstractString,s::AbstractString) = ismatch(Regex(pattern), s)


function s3_purge_versions(aws, bucket, path="", pattern="")

    for v in s3_list_versions(aws, bucket, path)
        if pattern == "" || ismatch(pattern, v["Key"])
            if v["IsLatest"] != "true"
                s3_delete(aws, bucket, v["Key"]; version = v["VersionId"])
            end
        end
    end
end


# See http://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectPUT.html

function s3_put(aws, bucket, path, data::Union{AbstractString,Vector{UInt8}},
                                   data_type="")

    if data_type == ""
        data_type = "application/octet-stream"
        for (e, t) in [
            (".pdf",  "application/pdf"),
            (".csv",  "text/csv"),
            (".txt",  "text/plain"),
            (".log",  "text/plain"),
            (".dat",  "application/octet-stream"),
            (".gz",   "application/octet-stream"),
            (".bz2",  "application/octet-stream"),
        ]
            if ismatch(e * "\$", path)
                data_type = t
                break
            end
        end
    end

    s3(aws, "PUT", bucket;
       path=path,
       headers=Dict("Content-Type" => data_type),
       content=data)
end


import Nettle: digest


function s3_sign_url(aws, bucket, path, seconds = 3600)

    query = Dict("AWSAccessKeyId" =>  aws[:creds].access_key_id,
                 "x-amz-security-token" => get(aws, "token", ""),
                 "Expires" => string(round(Int, Dates.datetime2unix(now(Dates.UTC)) + seconds)),
                 "response-content-disposition" => "attachment")

    to_sign = "GET\n\n\n$(query["Expires"])\n" *
              "x-amz-security-token:$(query["x-amz-security-token"])\n" *
              "/$bucket/$path?response-content-disposition=attachment"

    key = aws[:creds].secret_key
    query["Signature"] = digest("sha1", key, to_sign) |> base64encode |> strip

    endpoint=aws_endpoint("s3", aws[:region], bucket)
    return "$endpoint/$path?$(format_query_str(query))"
end


end #module AWSS3

#==============================================================================#
# End of file.
#==============================================================================#
