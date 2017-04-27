#==============================================================================#
# AWSS3.jl
#
# S3 API. See http://docs.aws.amazon.com/AmazonS3/latest/API/APIRest.html
#
# Copyright OC Technology Pty Ltd 2014 - All rights reserved
#==============================================================================#


__precompile__()


module AWSS3

export s3_arn, s3_put, s3_get, s3_get_file, s3_exists, s3_delete, s3_copy,
       s3_create_bucket,
       s3_put_cors,
       s3_enable_versioning, s3_delete_bucket, s3_list_buckets,
       s3_list_objects, s3_list_versions, s3_get_meta, s3_purge_versions,
       s3_sign_url, s3_begin_multipart_upload, s3_upload_part,
       s3_complete_multipart_upload, s3_multipart_upload

import HttpCommon: Response
import Requests: mimetype

using AWSCore
using SymDict
using Retry
using XMLDict
using LightXML

import Requests: format_query_str

typealias SSDict Dict{String,String}


s3_arn(resource) = "arn:aws:s3:::$resource"
s3_arn(bucket, path) = s3_arn("$bucket/$path")


# S3 REST API request.

function s3(aws::AWSConfig,
            verb,
            bucket="";
            headers=SSDict(),
            path="",
            query=SSDict(),
            version="",
            content="",
            return_stream=false)

    # Build query string...
    if version != ""
        query["versionId"] = version
    end
    query_str = format_query_str(query)

    # Build URL...
    resource = "/$path$(query_str == "" ? "" : "?$query_str")"
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
        return do_request(request)

    catch e

        # Update bucket region cache if needed...
        @retry if typeof(e) == AWSCore.AuthorizationHeaderMalformed &&
                  haskey(e.info, "Region")

            if AWSCore.debug_level > 0
                println("S3 region redirect $bucket -> $(e.info["Region"])")
            end
            if !haskey(aws, :bucket_region)
                aws[:bucket_region] = SSDict()
            end
            aws[:bucket_region][bucket] = e.info["Region"]
        end
    end

    assert(false) # Unreachable.
end


# See http://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectGET.html

function s3_get(aws::AWSConfig, bucket, path; version="", retry=true)

    @repeat 4 try

        return s3(aws, "GET", bucket; path = path, version = version)

    catch e
        @delay_retry if retry && e.code in ["NoSuchBucket", "NoSuchKey"] end
    end
end


function s3_get_file(aws::AWSConfig, bucket, path, filename; version="")

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


function s3_get_file(aws::AWSConfig, buckets::Vector, path, filename; version="")

    i = start(buckets)

    @repeat length(buckets) try

        bucket, i = next(buckets, i)
        s3_get_file(aws, bucket, path, filename; version=version);

    catch e
        @retry if e.code in ["NoSuchKey", "AccessDenied"] end
    end
end


function s3_get_meta(aws::AWSConfig, bucket, path; version="")

    res = s3(aws, "HEAD", bucket; path = path, version = version)
    return res.headers
end


function s3_exists(aws::AWSConfig, bucket, path; version="")

    @repeat 2 try

        s3(aws, "GET", bucket; path = path,
                               headers = SSDict("Range" => "bytes=0-0"),
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

function s3_delete(aws::AWSConfig, bucket, path; version="")

    s3(aws, "DELETE", bucket; path = path, version = version)
end


# See http://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectCOPY.html

function s3_copy(aws::AWSConfig, bucket, path; to_bucket=bucket, to_path=path)

    s3(aws, "PUT", to_bucket;
                   path = to_path,
                   headers = SSDict("x-amz-copy-source" => "/$bucket/$path"))
end


# See http://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketPUT.html

function s3_create_bucket(aws::AWSConfig, bucket)

    println("""Creating Bucket "$bucket"...""")

    @protected try

        if aws[:region] == "us-east-1"

            s3(aws, "PUT", bucket)

        else

            s3(aws, "PUT", bucket;
                headers = SSDict("Content-Type" => "text/plain"),
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



# See http://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketPUTcors.html

function s3_put_cors(aws::AWSConfig, bucket, cors_config)
    s3(aws, "PUT", bucket, path = "?cors", content = cors_config)
end


# See http://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketPUTVersioningStatus.html

function s3_enable_versioning(aws::AWSConfig, bucket)

    s3(aws, "PUT", bucket;
       query = SSDict("versioning" => ""),
       content = """
       <VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
           <Status>Enabled</Status>
       </VersioningConfiguration>""")
end


# See http://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketDELETE.html

s3_delete_bucket(aws::AWSConfig, bucket) = s3(aws, "DELETE", bucket)


# See http://docs.aws.amazon.com/AmazonS3/latest/API/RESTServiceGET.html

function s3_list_buckets(aws::AWSConfig)

    r = s3(aws,"GET", headers=SSDict("Content-Type" => "application/json"))
    [b["Name"] for b in r["Buckets"]["Bucket"]]
end


# See http://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketGET.html

function s3_list_objects(aws::AWSConfig, bucket, path="")

    more = true
    objects = []
    marker = ""

    while more

        q = SSDict()
        if path != ""
            q["delimiter"] = "/"
            q["prefix"] = path
        end
        if marker != ""
            q["marker"] = marker
        end

        @repeat 4 try

            r = s3(aws, "GET", bucket; query = q)

            more = r["IsTruncated"] == "true"

            if haskey(r, "Contents")
                l = isa(r["Contents"], Vector) ? r["Contents"] : [r["Contents"]]
                for object in l
                    push!(objects, xml_dict(object))
                    marker = object["Key"]
                end
            end

        catch e
            @delay_retry if e.code in ["NoSuchBucket"] end
        end
    end

    return objects
end


# See http://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketGETVersion.html

function s3_list_versions(aws::AWSConfig, bucket, path="")

    more = true
    versions = []
    marker = ""

    while more

        query = SSDict("versions" => "", "prefix" => path)
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
ismatch(pattern::AbstractString, s::AbstractString) = ismatch(Regex(pattern), s)


function s3_purge_versions(aws::AWSConfig, bucket, path="", pattern="")

    for v in s3_list_versions(aws, bucket, path)
        if pattern == "" || ismatch(pattern, v["Key"])
            if v["IsLatest"] != "true"
                s3_delete(aws, bucket, v["Key"]; version = v["VersionId"])
            end
        end
    end
end


# See http://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectPUT.html

function s3_put(aws::AWSConfig, bucket, path, 
                data::Union{String,Vector{UInt8}},
                data_type="application/octet-stream", 
                content_encoding = "")

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
    
    local headers
    if content_encoding == ""
        headers = SSDict(   "Content-Type"  => data_type )
    else
        headers = SSDict(  "Content-Type"      => data_type,
                        "Content-Encoding"  => content_encoding)
    end

    s3(aws, "PUT", bucket;
       path=path,
       headers= headers,
       content=data)
end


import Nettle: digest

function s3_begin_multipart_upload(aws, bucket, path, data_type = "application/octet-stream")
  response = s3(aws, "POST", bucket;
                path=path, query="uploads")
  if typeof(response) != XMLDict.XMLDictElement
    response = parse_xml(bytestring(response))
  end
  response
end

function s3_upload_part(aws, env, part_number, part_data)
  path = env["Key"]
  bucket = env["Bucket"]
  md5 = base64encode(digest("md5", part_data))
  q = Dict("partNumber" => part_number,
           "uploadId" => env["UploadId"])
  response = s3(aws, "PUT", bucket;
                path=path, query=q,
                headers = Dict("Content-MD5" => md5),
                content = part_data)
  response.headers["ETag"]
end

function s3_complete_multipart_upload(aws, env, parts :: Array{String})
  doc = XMLDocument()
  root = create_root(doc, "CompleteMultipartUpload")
  for (i, etag) in enumerate(parts)
    xchild = new_child(root, "Part")
    xpartnumber = new_child(xchild, "PartNumber")
    xetag = new_child(xchild, "ETag")
    add_text(xpartnumber, string(i))
    add_text(xetag, etag)
  end
  q = Dict("uploadId" => env["UploadId"])
  bucket = env["Bucket"]
  path = env["Key"]
  response = s3(aws, "POST", bucket;
                path=path, query=q,
                content=string(doc))
  free(doc)
  response
end

function s3_multipart_upload(aws, bucket, path, data :: IOStream, chunk_size = 50)
  #convert the chunk size to megabytes
  chunk_size = chunk_size * 1024 * 1024
  env = s3_begin_multipart_upload(aws, bucket, path)
  tags = Array{ASCIIString}(0)
  part_data = Vector{UInt8}(chunk_size)
  while (n = readbytes!(data, part_data, chunk_size)) > 0
    if n < chunk_size
      part_data = part_data[1:n]
    end
    push!(tags, s3_upload_part(aws, env, length(tags) + 1, part_data))
  end
  s3_complete_multipart_upload(aws, env, tags)
end




function s3_sign_url(aws::AWSConfig, bucket, path, seconds = 3600)

    query = SSDict("AWSAccessKeyId" =>  aws[:creds].access_key_id,
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
