#==============================================================================#
# AWSS3.jl
#
# S3 API. See http://docs.aws.amazon.com/AmazonS3/latest/API/APIRest.html
#
# Copyright OC Technology Pty Ltd 2014 - All rights reserved
#==============================================================================#


__precompile__()


module AWSS3

export S3Path, s3_arn, s3_put, s3_get, s3_get_file, s3_exists, s3_delete, s3_copy,
       s3_create_bucket,
       s3_put_cors,
       s3_enable_versioning, s3_delete_bucket, s3_list_buckets,
       s3_list_objects, s3_list_keys, s3_list_versions,
       s3_get_meta, s3_purge_versions,
       s3_sign_url, s3_begin_multipart_upload, s3_upload_part,
       s3_complete_multipart_upload, s3_multipart_upload,
       s3_get_tags, s3_put_tags, s3_delete_tags

using AWSCore
using DataStructures
using FilePathsBase
using FilePathsBase: /, join
using HTTP
using SymDict
using Retry
using XMLDict
using EzXML
using Dates
using Base64
using UUIDs

const SSDict = Dict{String,String}

__init__() = FilePathsBase.register(S3Path)

"""
    s3_arn(resource)
    s3_arn(bucket,path)

[Amazon Resource Name](http://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html)
for S3 `resource` or `bucket` and `path`.
"""
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
            return_stream=false,
            return_raw=false,
            return_headers=false)

    # Build query string...
    if version != ""
        query["versionId"] = version
    end
    query_str = HTTP.escapeuri(query)

    resource = string("/", HTTP.escapepath(path),
                      query_str == "" ? "" : "?$query_str")

    # Build Request...
    request = @SymDict(service = "s3",
                       verb,
                       resource,
                       headers,
                       content,
                       return_stream,
                       return_raw,
                       aws...)

    @repeat 3 try

        # Check bucket region cache...
        if haskey(aws, :bucket_region) &&
           haskey(aws[:bucket_region], bucket)
            request[:region] = aws[:bucket_region][bucket]
        end

        # Build URL...
        if haskey(aws, :endpoint)
            if bucket == ""
                url = string(aws[:endpoint], resource)
            else
                url = string(aws[:endpoint], "/", bucket, resource)
            end
        else
            region = get(request, :region, "")
            url = string(get(aws, :protocol, "https"), "://",
                         bucket, bucket == "" ? "" : ".",
                         "s3",
                         region == "" ? "" : ".", region,
                         ".amazonaws.com",
                         resource)
        end
        request[:url] = url

        if return_headers
            response, headers = AWSCore.do_request(request; return_headers=return_headers)
        else
            response = AWSCore.do_request(request; return_headers=return_headers)
        end

        # Handle 301 Moved Permanently with missing Location header.
        # https://github.com/samoconnor/AWSS3.jl/issues/25
        if response isa XMLDict.XMLDictElement &&
           get(response, "Code", "") == "PermanentRedirect" &&
           haskey(response, "Endpoint")

            if AWSCore.debug_level > 0
                println("S3 endpoint redirect $bucket -> $(response["Endpoint"])")
            end
            request[:url] = string(get(aws, :protocol, "https"), "://",
                                   response["Endpoint"], resource)
            return AWSCore.do_request(request; return_headers=return_headers)
        end

        return (return_headers ? (response, headers) : response)

    catch e

        # Update bucket region cache if needed...
        @retry if ecode(e) == "AuthorizationHeaderMalformed" &&
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

    @assert false # Unreachable.
end


"""
    s3_get([::AWSConfig], bucket, path; <keyword arguments>)

[Get Object](http://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectGET.html)
from `path` in `bucket`.

# Optional Arguments
- `version=`: version of object to get.
- `retry=true`: try again on "NoSuchBucket", "NoSuchKey"
                (common if object was recently created).
- `raw=false`:  return response as `Vector{UInt8}`
                (by default return type depends on `Content-Type` header).
"""
function s3_get(aws::AWSConfig, bucket, path; version="",
                                              retry=true,
                                              raw=false)

    @repeat 4 try

        return s3(aws, "GET", bucket; path = path,
                                      version = version,
                                      return_raw = raw)

    catch e
        @delay_retry if retry && ecode(e) in ["NoSuchBucket", "NoSuchKey"] end
    end
end

s3_get(a...; b...) = s3_get(default_aws_config(), a...; b...)


"""
    s3_get_file([::AWSConfig], bucket, path, filename; [version=])

Like `s3_get` but streams result directly to `filename`.
"""
function s3_get_file(aws::AWSConfig, bucket, path, filename; version="")

    stream = s3(aws, "GET", bucket; path = path,
                                    version = version,
                                    return_stream = true)

    open(filename, "w") do file
        while !eof(stream)
            write(file, readavailable(stream))
        end
    end
end

s3_get_file(a...; b...) = s3_get_file(default_aws_config(), a...; b...)


function s3_get_file(aws::AWSConfig, buckets::Vector, path, filename; version="")

    i = start(buckets)

    @repeat length(buckets) try

        bucket, i = next(buckets, i)
        s3_get_file(aws, bucket, path, filename; version=version);

    catch e
        @retry if ecode(e) in ["NoSuchKey", "AccessDenied"] end
    end
end


"""
   s3_get_meta([::AWSConfig], bucket, path; [version=])

[HEAD Object](http://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectHEAD.html)

Retrieves metadata from an object without returning the object itself.
"""
function s3_get_meta(aws::AWSConfig, bucket, path; version="")

    s3(aws, "HEAD", bucket; path = path, version = version)
end

s3_get_meta(a...; b...) = s3_get_meta(default_aws_config(), a...; b...)


"""
    s3_exists([::AWSConfig], bucket, path [version=])

Is there an object in `bucket` at `path`?
"""
function s3_exists(aws::AWSConfig, bucket, path; version="")

    @repeat 2 try

        s3_get_meta(aws, bucket, path; version = version)

        return true

    catch e

        @delay_retry if ecode(e) in ["NoSuchBucket", "404",
                                   "NoSuchKey", "AccessDenied"]
        end
        @ignore if ecode(e) in ["404", "NoSuchKey", "AccessDenied"]
            return false
        end
    end
end

s3_exists(a...; b...) = s3_exists(default_aws_config(), a...; b...)


"""
    s3_delete([::AWSConfig], bucket, path; [version=]

[DELETE Object](http://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectDELETE.html)
"""
function s3_delete(aws::AWSConfig, bucket, path; version="")

    s3(aws, "DELETE", bucket; path = path, version = version)
end

s3_delete(a...; b...) = s3_delete(default_aws_config(), a...; b...)


"""
    s3_copy([::AWSConfig], bucket, path; to_bucket=bucket, to_path=path)

[PUT Object - Copy](http://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectCOPY.html)

# Optional Arguments
- `metadata::Dict=`; optional `x-amz-meta-` headers.
"""
function s3_copy(aws::AWSConfig, bucket, path;
                 acl::AbstractString="",
                 to_bucket=bucket, to_path=path, metadata::SSDict = SSDict())

    headers = SSDict("x-amz-copy-source" => "/$bucket/$path",
                     "x-amz-metadata-directive" => "REPLACE",
                     Pair["x-amz-meta-$k" => v for (k, v) in metadata]...)

    if !isempty(acl)
        headers["x-amz-acl"] = acl
    end

    s3(aws, "PUT", to_bucket; path = to_path, headers = headers)
end

s3_copy(a...; b...) = s3_copy(default_aws_config(), a...; b...)


"""
    s3_create_bucket([:AWSConfig], bucket)

[PUT Bucket](http://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketPUT.html)
"""
function s3_create_bucket(aws::AWSConfig, bucket)
    if AWSCore.debug_level > 0
        println("""Creating Bucket "$bucket"...""")
    end

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
        @ignore if ecode(e) == "BucketAlreadyOwnedByYou" end
    end
end

s3_create_bucket(a) = s3_create_bucket(default_aws_config(), a)


"""
    s3_put_cors([::AWSConfig], bucket, cors_config)

[PUT Bucket cors](http://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketPUTcors.html)

```
s3_put_cors("my_bucket", \"\"\"
    <?xml version="1.0" encoding="UTF-8"?>
    <CORSConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
        <CORSRule>
            <AllowedOrigin>http://my.domain.com</AllowedOrigin>
            <AllowedOrigin>http://my.other.domain.com</AllowedOrigin>
            <AllowedMethod>GET</AllowedMethod>
            <AllowedMethod>HEAD</AllowedMethod>
            <AllowedHeader>*</AllowedHeader>
            <ExposeHeader>Content-Range</ExposeHeader>
        </CORSRule>
    </CORSConfiguration>
\"\"\"
```
"""
function s3_put_cors(aws::AWSConfig, bucket, cors_config)
    s3(aws, "PUT", bucket, path = "?cors", content = cors_config)
end

s3_put_cors(a...) = s3_put_cors(default_aws_config(), a...)


"""
    s3_enable_versioning([::AWSConfig], bucket)

[Enable versioning for `bucket`](http://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketPUTVersioningStatus.html).
"""
function s3_enable_versioning(aws::AWSConfig, bucket)

    s3(aws, "PUT", bucket;
       query = SSDict("versioning" => ""),
       content = """
       <VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
           <Status>Enabled</Status>
       </VersioningConfiguration>""")
end

s3_enable_versioning(a) = s3_enable_versioning(default_aws_config(), a)


"""
    s3_put_tags([::AWSConfig], bucket, [path,] tags::Dict)

PUT `tags` on
[`bucket`](http://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketPUTtagging.html)
or
[object (`path`)](http://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectPUTtagging.html).

See also `tags=` option on [`s3_put`](@ref).
"""
function s3_put_tags(aws::AWSConfig, bucket, tags::SSDict)
    s3_put_tags(aws, bucket, "", tags)
end


function s3_put_tags(aws::AWSConfig, bucket, path, tags::SSDict)

    tags = Dict("Tagging" =>
           Dict("TagSet" =>
           Dict("Tag" =>
           [Dict("Key" => k, "Value" => v) for (k,v) in tags])))

    s3(aws, "PUT", bucket;
       path = path,
       query = SSDict("tagging" => ""),
       content = XMLDict.node_xml(tags))
end

s3_put_tags(a...) = s3_put_tags(default_aws_config(), a...)


"""
    s3_get_tags([::AWSConfig], bucket, [path])

Get tags from
[`bucket`](http://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketGETtagging.html)
or
[object (`path`)](http://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectGETtagging.html).
"""
function s3_get_tags(aws::AWSConfig, bucket, path="")

    @protected try

        tags = s3(aws, "GET", bucket; path = path, query = SSDict("tagging" => ""))
        if isempty(tags["TagSet"])
            return SSDict()
        end
        tags = tags["TagSet"]
        tags = isa(tags["Tag"], Vector) ? tags["Tag"] : [tags["Tag"]]

        return SSDict(x["Key"] => x["Value"] for x in tags)

    catch e
        @ignore if ecode(e) == "NoSuchTagSet"
            return SSDict()
        end
    end
end

s3_get_tags(a...) = s3_get_tags(default_aws_config(), a...)


"""
    s3_delete_tags([::AWSConfig], bucket, [path])

Delete tags from
[`bucket`](http://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketDELETEtagging.html)
or
[object (`path`)](http://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectDELETEtagging.html).
"""
function s3_delete_tags(aws::AWSConfig, bucket, path="")
    s3(aws, "DELETE", bucket; path = path, query = SSDict("tagging" => ""))
end

s3_delete_tags(a...) = s3_delete_tags(default_aws_config(), a...)


"""
    s3_delete_bucket([::AWSConfig], "bucket")

[DELETE Bucket](http://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketDELETE.html).
"""
s3_delete_bucket(aws::AWSConfig, bucket) = s3(aws, "DELETE", bucket)

s3_delete_bucket(a) = s3_delete_bucket(default_aws_config(), a)


"""
    s3_list_buckets([::AWSConfig])

[List of all buckets owned by the sender of the request](http://docs.aws.amazon.com/AmazonS3/latest/API/RESTServiceGET.html).
"""
function s3_list_buckets(aws::AWSConfig = default_aws_config())

    r = s3(aws,"GET", headers=SSDict("Content-Type" => "application/json"))
    buckets = r["Buckets"]
    if isempty(buckets)
        return []
    end
    buckets = buckets["Bucket"]
    [b["Name"] for b in (isa(buckets, Vector) ? buckets : [buckets])]
end


"""
    s3_list_objects([::AWSConfig], bucket, [path_prefix]; delimiter="/", max_items=1000)

[List Objects](http://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketGET.html)
in `bucket` with optional `path_prefix`.

Returns an iterator of `Dict`s with keys `Key`, `LastModified`, `ETag`, `Size`,
`Owner`, `StorageClass`.
"""
function s3_list_objects(aws::AWSConfig, bucket, path_prefix=""; delimiter="/", max_items=nothing)
    return Channel() do chnl
        more = true
        num_objects = 0
        marker = ""

        while more
            q = Dict{String, String}()
            if path_prefix != ""
                q["prefix"] = path_prefix
            end
            if delimiter != ""
                q["delimiter"] = delimiter
            end
            if marker != ""
                q["marker"] = marker
            end
            if max_items !== nothing
                # Note: AWS seems to only return up to 1000 items
                q["max-keys"] = string(max_items - num_objects)
            end

            @repeat 4 try
                # Request objects
                r = s3(aws, "GET", bucket; query = q)

                # Add each object from the response and update our object count / marker
                if haskey(r, "Contents")
                    l = isa(r["Contents"], Vector) ? r["Contents"] : [r["Contents"]]
                    for object in l
                        put!(chnl, xml_dict(object))
                        num_objects += 1
                        marker = object["Key"]
                    end
                # It's possible that the response doesn't have "Contents" and just has a prefix,
                # in which case we should just save the next marker and iterate.
                elseif haskey(r, "Prefix")
                    put!(chnl, Dict("Key" => r["Prefix"]))
                    num_objects +=1
                    marker = haskey(r, "NextMarker") ? r["NextMarker"] : r["Prefix"]
                end

                # Continue looping if the results were truncated and we haven't exceeded out max_items (if specified)
                more = r["IsTruncated"] == "true" && (max_items === nothing || num_objects < max_items)
            catch e
                @delay_retry if ecode(e) in ["NoSuchBucket"] end
            end
        end
    end
end

s3_list_objects(a...) = s3_list_objects(default_aws_config(), a...)


"""
    s3_list_keys([::AWSConfig], bucket, [path_prefix])

Like [`s3_list_objects`](@ref) but returns object keys as `Vector{String}`.
"""
function s3_list_keys(aws::AWSConfig, bucket, path_prefix="")

    (o["Key"] for o in s3_list_objects(aws::AWSConfig, bucket, path_prefix))
end

s3_list_keys(a...) = s3_list_keys(default_aws_config(), a...)



"""
    s3_list_versions([::AWSConfig], bucket, [path_prefix])

[List object versions](http://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketGETVersion.html) in `bucket` with optional `path_prefix`.
"""
function s3_list_versions(aws::AWSConfig, bucket, path_prefix="")

    more = true
    versions = []
    marker = ""

    while more

        query = SSDict("versions" => "", "prefix" => path_prefix)
        if marker != ""
            query["key-marker"] = marker
        end

        r = s3(aws, "GET", bucket; query = query)
        more = r["IsTruncated"] == "true"
        for e in eachelement(EzXML.root(r.x))
            if nodename(e) in ["Version", "DeleteMarker"]
                version = xml_dict(e)
                version["state"] = nodename(e)
                push!(versions, version)
                marker = version["Key"]
            end
        end
    end
    return versions
end

s3_list_versions(a...) = s3_list_versions(default_aws_config(), a...)



"""
    s3_purge_versions([::AWSConfig], bucket, [path [, pattern]])

[DELETE](http://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectDELETE.html)
all object versions except for the latest version.
"""
function s3_purge_versions(aws::AWSConfig, bucket, path="", pattern="")

    for v in s3_list_versions(aws, bucket, path)
        if pattern == "" || occursin(pattern, v["Key"])
            if v["IsLatest"] != "true"
                s3_delete(aws, bucket, v["Key"]; version = v["VersionId"])
            end
        end
    end
end

s3_purge_versions(a...) = s3_purge_versions(default_aws_config(), a...)

"""
    s3_put([::AWSConfig], bucket, path, data; <keyword arguments>

[PUT Object](http://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectPUT.html)
`data` at `path` in `bucket`.

# Optional Arguments
- `data_type=`; `Content-Type` header.
- `encoding=`; `Content-Encoding` header.
- `acl=`; 'x-amz-acl' header for setting access permissions with canned config.
    See [here](https://docs.aws.amazon.com/AmazonS3/latest/dev/acl-overview.html#canned-acl).
- `metadata::Dict=`; `x-amz-meta-` headers.
- `tags::Dict=`; `x-amz-tagging-` headers
                 (see also [`s3_put_tags`](@ref) and [`s3_get_tags`](@ref)).
"""
function s3_put(aws::AWSConfig,
                bucket, path, data::Union{String,Vector{UInt8}},
                data_type="", encoding="";
                acl::AbstractString="",
                metadata::SSDict = SSDict(),
                tags::SSDict = SSDict())

    if data_type == ""
        data_type = "application/octet-stream"
        ext = splitext(path)[2]
        for (e, t) in [
            (".html", "text/html"),
            (".js",   "application/javascript"),
            (".pdf",  "application/pdf"),
            (".csv",  "text/csv"),
            (".txt",  "text/plain"),
            (".log",  "text/plain"),
            (".dat",  "application/octet-stream"),
            (".gz",   "application/octet-stream"),
            (".bz2",  "application/octet-stream"),
        ]
            if ext == e
                data_type = t
                break
            end
        end
    end

    headers = SSDict("Content-Type" => data_type,
                     Pair["x-amz-meta-$k" => v for (k, v) in metadata]...)

    if !isempty(tags)
        headers["x-amz-tagging"] = HTTP.escapeuri(tags)
    end

    if !isempty(acl)
        headers["x-amz-acl"] = acl
    end

    if encoding != ""
        headers["Content-Encoding"] = encoding
    end

    s3(aws, "PUT", bucket;
       path = path,
       headers = headers,
       content = data)
end

s3_put(a...; b...) = s3_put(default_aws_config(), a...; b...)


function s3_begin_multipart_upload(aws::AWSConfig,
                                   bucket, path,
                                   data_type = "application/octet-stream")

    s3(aws, "POST", bucket; path=path, query = SSDict("uploads"=>""))
end


function s3_upload_part(aws::AWSConfig, upload, part_number, part_data)

    _, headers = s3(aws, "PUT", upload["Bucket"];
                  path = upload["Key"],
                  query = Dict("partNumber" => part_number,
                               "uploadId" => upload["UploadId"]),
                  content = part_data,
                  return_headers = true)

    return Dict(headers)["ETag"]
end


function s3_complete_multipart_upload(aws::AWSConfig,
                                      upload, parts::Vector{String})
    doc = XMLDocument()
    rootnode = setroot!(doc, ElementNode("CompleteMultipartUpload"))

    for (i, etag) in enumerate(parts)
        part = addelement!(rootnode, "Part")
        addelement!(part, "PartNumber", string(i))
        addelement!(part, "ETag", etag)
    end

    response = s3(aws, "POST", upload["Bucket"];
                  path = upload["Key"],
                  query = Dict("uploadId" => upload["UploadId"]),
                  content = string(doc))

    return response
end


function s3_multipart_upload(aws::AWSConfig, bucket, path, io::IOStream,
                             part_size_mb = 50)

    part_size = part_size_mb * 1024 * 1024

    upload = s3_begin_multipart_upload(aws, bucket, path)

    tags = Vector{String}()
    buf = Vector{UInt8}(undef, part_size)

    i = 0
    while (n = readbytes!(io, buf, part_size)) > 0
        if n < part_size
            resize!(buf, n)
        end
        push!(tags, s3_upload_part(aws, upload, (i += 1), buf))
    end

    s3_complete_multipart_upload(aws, upload, tags)
end

using MbedTLS

function _s3_sign_url_v2(
    aws::AWSConfig, bucket, path, seconds=3600;
    verb="GET", content_type="application/octet-stream", protocol="http",
)

    path = HTTP.escapepath(path)

    expires = round(Int, Dates.datetime2unix(now(Dates.UTC)) + seconds)

    query = SSDict("AWSAccessKeyId" =>  aws[:creds].access_key_id,
                   "x-amz-security-token" => aws[:creds].token,
                   "Expires" => string(expires),
                   "response-content-disposition" => "attachment")

    if verb != "PUT"
        content_type = ""
    end

    to_sign = "$verb\n\n$content_type\n$(query["Expires"])\n" *
              "x-amz-security-token:$(query["x-amz-security-token"])\n" *
              "/$bucket/$path?" *
              "response-content-disposition=attachment"

    key = aws[:creds].secret_key
    query["Signature"] = digest(MD_SHA1, to_sign, key) |> base64encode |> strip

    endpoint=string(protocol, "://",
                    bucket, ".s3.", aws[:region], ".amazonaws.com")
    return "$endpoint/$path?$(HTTP.escapeuri(query))"
end


function _s3_sign_url_v4(
    aws::AWSConfig, bucket, path, seconds=3600;
    verb="GET", content_type="application/octet-stream", protocol="http",
)

    path = HTTP.escapepath("/$bucket/$path")

    now_datetime = now(Dates.UTC)
    datetime_stamp = Dates.format(now_datetime, "YYYYmmddTHHMMSSZ")
    date_stamp = Dates.format(now_datetime, "YYYYmmdd")

    service = "s3"
    scheme = "AWS4"
    algorithm = "HMAC-SHA256"
    terminator = "aws4_request"

    scope = "$date_stamp/$(aws[:region])/$service/$terminator"
    host = if aws[:region] == "us-east-1"
        "s3.amazonaws.com"
    else
        "s3-$(aws[:region]).amazonaws.com"
    end

    headers = OrderedDict{String, String}("Host" => host)
    sort!(headers; by = name -> lowercase(name))
    canonical_header_names = join(map(name -> lowercase(name), headers |> keys |> collect), ";")

    query = OrderedDict{String, String}(
        "X-Amz-Expires" => string(seconds),
        "X-Amz-Algorithm" => "$scheme-$algorithm",
        "X-Amz-Credential" => "$(aws[:creds].access_key_id)/$scope",
        "X-Amz-Date" => datetime_stamp,
        "X-Amz-Security-Token" => aws[:creds].token,
        "X-Amz-SignedHeaders" => canonical_header_names
    )

    if !isempty(aws[:creds].token)
        query["X-Amz-Security-Token"] = aws[:creds].token
    end

    sort!(query; by = name -> lowercase(name))

    canonical_headers = join(map(header -> "$(lowercase(header.first)):$(lowercase(header.second))\n", collect(headers)))

    canonical_request = string(
        "$verb\n",
        "$path\n",
        "$(HTTP.escapeuri(query))\n",
        "$canonical_headers\n",
        "$canonical_header_names\n",
        "UNSIGNED-PAYLOAD"
    )

    string_to_sign = string(
        "$scheme-$algorithm\n",
        "$datetime_stamp\n",
        "$scope\n",
        digest(MD_SHA256, canonical_request) |> bytes2hex
    )

    key_secret = string(scheme, aws[:creds].secret_key)
    key_date = digest(MD_SHA256, date_stamp, key_secret)
    key_region = digest(MD_SHA256, aws[:region], key_date)
    key_service = digest(MD_SHA256, service, key_region)
    key_signing = digest(MD_SHA256, terminator, key_service)
    signature = digest(MD_SHA256, string_to_sign, key_signing)

    query["X-Amz-Signature"] = signature |> bytes2hex

    return string(protocol, "://", host, path, "?", HTTP.escapeuri(query))
end


"""
    s3_sign_url([::AWSConfig], bucket, path, [seconds=3600];
                [verb="GET"], [content_type="application/octet-stream"],
                [protocol="http"], [signature_version="v4"])

Create a
[pre-signed url](http://docs.aws.amazon.com/AmazonS3/latest/dev/ShareObjectPreSignedURL.html) for `bucket` and `path` (expires after for `seconds`).

To create an upload URL use `verb="PUT"` and set `content_type` to match
the type used in the `Content-Type` header of the PUT request.

For compatibility, the signature version 2 signing process can be used by setting
`signature_version="v2"` but it is [recommended](https://docs.aws.amazon.com/general/latest/gr/signature-version-2.html) that the default version 4 is used.

```
url = s3_sign_url("my_bucket", "my_file.txt"; verb="PUT")
Requests.put(URI(url), "Hello!")
```
```
url = s3_sign_url("my_bucket", "my_file.txt";
                  verb="PUT", content_type="text/plain")

Requests.put(URI(url), "Hello!";
             headers=Dict("Content-Type" => "text/plain"))
```
"""
function s3_sign_url(
    aws::AWSConfig, bucket, path, seconds=3600;
    verb="GET", content_type="application/octet-stream", protocol="http",
    signature_version="v4",
)

    if signature_version == "v2"
        _s3_sign_url_v2(
            aws, bucket, path, seconds;
            verb=verb, content_type=content_type, protocol=protocol,
        )
    elseif signature_version == "v4"
        _s3_sign_url_v4(
            aws, bucket, path, seconds;
            verb=verb, content_type=content_type, protocol=protocol,
        )
    else
        throw(ArgumentError("Unknown signature version $signature_version"))
    end
end

s3_sign_url(a...;b...) = s3_sign_url(default_aws_config(), a...;b...)


include("s3path.jl")

end #module AWSS3

#==============================================================================#
# End of file.
#==============================================================================#
