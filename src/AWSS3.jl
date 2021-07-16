#==============================================================================#
# AWSS3.jl
#
# S3 API. See http://docs.aws.amazon.com/AmazonS3/latest/API/APIRest.html
#
# Copyright OC Technology Pty Ltd 2014 - All rights reserved
#==============================================================================#


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

using AWS
using AWS.AWSServices: s3
using FilePathsBase
using FilePathsBase: /, join
using HTTP
using OrderedCollections: OrderedDict
using SymDict
using Retry
using XMLDict
using EzXML
using Dates
using Base64
using UUIDs

@service S3

const SSDict = Dict{String,String}
const S3PathVersion = Union{String,Nothing}

__init__() = FilePathsBase.register(S3Path)

"""
    s3_arn(resource)
    s3_arn(bucket,path)

[Amazon Resource Name](http://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html)
for S3 `resource` or `bucket` and `path`.
"""
s3_arn(resource) = "arn:aws:s3:::$resource"
s3_arn(bucket, path) = s3_arn("$bucket/$path")


"""
    s3_get([::AbstractAWSConfig], bucket, path; <keyword arguments>)

[Get Object](http://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectGET.html)
from `path` in `bucket`.

# Optional Arguments
- `version=`: version of object to get.
- `retry=true`: try again on "NoSuchBucket", "NoSuchKey"
                (common if object was recently created).
- `raw=false`:  return response as `Vector{UInt8}`
                (by default return type depends on `Content-Type` header).
- `byte_range=nothing`:  given an iterator of `(start_byte, end_byte)` gets only
    the range of bytes of the object from `start_byte` to `end_byte`.  For example,
    `byte_range=1:4` gets bytes 1 to 4 inclusive.  Arguments should use the Julia convention
    of 1-based indexing.
- `header::Dict{String,String}`: pass in an HTTP header to the request.

As an example of how to set custom HTTP headers, the below is equivalent to
`s3_get(aws, bucket, path; byte_range=range)`.

`s3_get(aws, bucket, path; headers=Dict{String,String}("Range" => "bytes=\$(first(range)-1)-\$(last(range)-1)"))`
"""
function s3_get(
    aws::AbstractAWSConfig, bucket, path; version::S3PathVersion=nothing, retry::Bool=true,
    byte_range::Union{Nothing,AbstractVector}=nothing, raw::Bool=false,
    headers::AbstractDict{<:AbstractString,<:Any}=Dict{String, Any}(),
    return_stream::Bool=false, kwargs...
)
    @repeat 4 try
        args = Dict{String, Any}(
            "return_raw" => raw,
            "return_stream" => return_stream,
        )

        if !isnothing(version) || !isempty(version)
            args["versionId"] = version
        end

        if byte_range ≢ nothing
            headers = copy(headers)  # make sure we don't mutate existing object
            # we make sure we stick to the Julia convention of 1-based indexing
            a, b = (first(byte_range) - 1), (last(byte_range) - 1)
            headers["Range"] = "bytes=$a-$b"
        end

        if !isempty(headers)
            args["headers"] = headers
        end

        return S3.get_object(bucket, path, args; aws_config=aws, kwargs...)
    catch e
        @delay_retry if retry && ecode(e) in ["NoSuchBucket", "NoSuchKey"] end
    end
end

s3_get(a...; b...) = s3_get(global_aws_config(), a...; b...)


"""
    s3_get_file([::AbstractAWSConfig], bucket, path, filename; [version=], kwargs...)

Like `s3_get` but streams result directly to `filename`.  Keyword arguments accept are
the same as those for `s3_get`.
"""
function s3_get_file(aws::AbstractAWSConfig, bucket, path, filename; version::S3PathVersion=nothing, kwargs...)
    stream = s3_get(aws, bucket, path; version=version, return_stream=true, kwargs...)

    open(filename, "w") do file
        while !eof(stream)
            write(file, readavailable(stream))
        end
    end
end

s3_get_file(a...; b...) = s3_get_file(global_aws_config(), a...; b...)


function s3_get_file(aws::AbstractAWSConfig, buckets::Vector, path, filename;
                     version::S3PathVersion=nothing, kwargs...)
    i = start(buckets)

    @repeat length(buckets) try
        bucket, i = next(buckets, i)
        s3_get_file(aws, bucket, path, filename; version=version, kwargs...)
    catch e
        @retry if ecode(e) in ["NoSuchKey", "AccessDenied"] end
    end
end


"""
   s3_get_meta([::AbstractAWSConfig], bucket, path; [version=], kwargs...)

[HEAD Object](http://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectHEAD.html)

Retrieves metadata from an object without returning the object itself.
"""
function s3_get_meta(aws::AbstractAWSConfig, bucket, path; version::S3PathVersion=nothing,
                     kwargs...)
    if isnothing(version) || isempty(version)
        S3.head_object(bucket, path; aws_config=aws, kwargs...)
    else
        S3.head_object(bucket, path, Dict("versionId"=>version); aws_config=aws, kwargs...)
    end
end

s3_get_meta(a...; b...) = s3_get_meta(global_aws_config(), a...; b...)


"""
    s3_exists([::AbstractAWSConfig], bucket, path [version=], kwargs...)

Is there an object in `bucket` at `path`?
"""
function s3_exists(aws::AbstractAWSConfig, bucket, path; version::S3PathVersion=nothing, kwargs...)
    @repeat 2 try
        s3_get_meta(aws, bucket, path; version=version, kwargs...)

        return true

    catch e
        @delay_retry if ecode(e) in ["NoSuchBucket", "404", "NoSuchKey", "AccessDenied"]
        end

        @ignore if ecode(e) in ["404", "NoSuchKey", "AccessDenied"]
            return false
        end
    end
end

s3_exists(a...; b...) = s3_exists(global_aws_config(), a...; b...)


"""
    s3_delete([::AbstractAWSConfig], bucket, path; [version=], kwargs...)

[DELETE Object](http://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectDELETE.html)
"""
function s3_delete(aws::AbstractAWSConfig, bucket, path; version::S3PathVersion=nothing,
                   kwargs...)
    if isnothing(version) || isempty(version)
        S3.delete_object(bucket, path; aws_config=aws, kwargs...)
    else
        S3.delete_object(bucket, path, Dict("versionId"=>version); aws_config=aws, kwargs...)
    end
end

s3_delete(a...; b...) = s3_delete(global_aws_config(), a...; b...)


"""
    s3_copy([::AbstractAWSConfig], bucket, path; to_bucket=bucket, to_path=path, kwargs...)

[PUT Object - Copy](http://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectCOPY.html)

# Optional Arguments
- `metadata::Dict=`; optional `x-amz-meta-` headers.
"""
function s3_copy(
    aws::AbstractAWSConfig, bucket, path;
    acl::AbstractString="", to_bucket=bucket, to_path=path, metadata::AbstractDict = SSDict(), kwargs...
)
    headers = SSDict(
        "x-amz-metadata-directive" => "REPLACE",
        Pair["x-amz-meta-$k" => v for (k, v) in metadata]...
    )

    if !isempty(acl)
        headers["x-amz-acl"] = acl
    end

    S3.copy_object(to_bucket, to_path, "$bucket/$path", Dict("headers"=>headers); aws_config=aws, kwargs...)
end

s3_copy(a...; b...) = s3_copy(global_aws_config(), a...; b...)


"""
    s3_create_bucket([:AbstractAWSConfig], bucket; kwargs...)

[PUT Bucket](http://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketPUT.html)
"""
function s3_create_bucket(aws::AbstractAWSConfig, bucket; kwargs...)
    @protected try
        if aws.region == "us-east-1"
            S3.create_bucket(bucket; aws_config=aws, kwargs...)
        else
            bucket_config =
            """
                <CreateBucketConfiguration
                            xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                    <LocationConstraint>$(aws.region)</LocationConstraint>
                </CreateBucketConfiguration>
            """

            S3.create_bucket(bucket, Dict("CreateBucketConfiguration"=>bucket_config); aws_config=aws, kwargs...)
        end
    catch e
        @ignore if ecode(e) == "BucketAlreadyOwnedByYou" end
    end
end

s3_create_bucket(a) = s3_create_bucket(global_aws_config(), a)


"""
    s3_put_cors([::AbstractAWSConfig], bucket, cors_config; kwargs...)

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
function s3_put_cors(aws::AbstractAWSConfig, bucket, cors_config; kwargs...)
    S3.put_bucket_cors(bucket, cors_config; aws_config=aws, kwargs...)
end

s3_put_cors(a...; b...) = s3_put_cors(AWS.global_aws_config(), a...; b...)


"""
    s3_enable_versioning([::AbstractAWSConfig], bucket; kwargs...)

[Enable versioning for `bucket`](http://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketPUTVersioningStatus.html).
"""
function s3_enable_versioning(aws::AbstractAWSConfig, bucket, status="Enabled"; kwargs...)
    versioning_config = """
        <VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <Status>$status</Status>
        </VersioningConfiguration>
    """

    s3("PUT", "/$(bucket)?versioning", Dict("body"=>versioning_config); aws_config=aws, kwargs...)
end

s3_enable_versioning(a; b...) = s3_enable_versioning(global_aws_config(), a; b...)


"""
    s3_put_tags([::AbstractAWSConfig], bucket, [path,] tags::Dict; kwargs...)

PUT `tags` on
[`bucket`](http://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketPUTtagging.html)
or
[object (`path`)](http://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectPUTtagging.html).

See also `tags=` option on [`s3_put`](@ref).
"""
function s3_put_tags(aws::AbstractAWSConfig, bucket, tags::SSDict; kwargs...)
    s3_put_tags(aws, bucket, "", tags; kwargs...)
end


function s3_put_tags(aws::AbstractAWSConfig, bucket, path, tags::SSDict; kwargs...)
    tags = Dict("Tagging" =>
           Dict("TagSet" =>
           Dict("Tag" =>
           [Dict("Key" => k, "Value" => v) for (k,v) in tags])))

    tags = XMLDict.node_xml(tags)

    if isempty(path)
        s3("PUT", "/$(bucket)?tagging", Dict("body"=>tags); aws_config=aws, kwargs...)
    else
        s3("PUT", "/$(bucket)/$(path)?tagging", Dict("body"=>tags); aws_config=aws, kwargs...)
    end
end

s3_put_tags(a...) = s3_put_tags(global_aws_config(), a...)


"""
    s3_get_tags([::AbstractAWSConfig], bucket, [path]; kwargs...)

Get tags from
[`bucket`](http://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketGETtagging.html)
or
[object (`path`)](http://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectGETtagging.html).
"""
function s3_get_tags(aws::AbstractAWSConfig, bucket, path=""; kwargs...)
    @protected try
        tags = []

        if isempty(path)
            tags = S3.get_bucket_tagging(bucket; aws_config=aws, kwargs...)
        else
            tags = S3.get_object_tagging(bucket, path; aws_config=aws, kwargs...)
        end

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

s3_get_tags(a...; b...) = s3_get_tags(global_aws_config(), a...; b...)


"""
    s3_delete_tags([::AbstractAWSConfig], bucket, [path])

Delete tags from
[`bucket`](http://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketDELETEtagging.html)
or
[object (`path`)](http://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectDELETEtagging.html).
"""
function s3_delete_tags(aws::AbstractAWSConfig, bucket, path=""; kwargs...)
    if isempty(path)
        S3.delete_bucket_tagging(bucket; aws_config=aws, kwargs...)
    else
        S3.delete_object_tagging(bucket, path; aws_config=aws, kwargs...)
    end
end

s3_delete_tags(a...; b...) = s3_delete_tags(global_aws_config(), a...; b...)


"""
    s3_delete_bucket([::AbstractAWSConfig], "bucket"; kwargs...)

[DELETE Bucket](http://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketDELETE.html).
"""
s3_delete_bucket(aws::AbstractAWSConfig, bucket; kwargs...) = S3.delete_bucket(bucket; aws_config=aws, kwargs...)
s3_delete_bucket(a; b...) = s3_delete_bucket(global_aws_config(), a; b...)


"""
    s3_list_buckets([::AbstractAWSConfig]; kwargs...)

[List of all buckets owned by the sender of the request](http://docs.aws.amazon.com/AmazonS3/latest/API/RESTServiceGET.html).
"""
function s3_list_buckets(aws::AbstractAWSConfig=global_aws_config(); kwargs...)
    r = S3.list_buckets(; aws_config=aws, kwargs...)
    buckets = r["Buckets"]

    if isempty(buckets)
        return []
    end

    buckets = buckets["Bucket"]
    [b["Name"] for b in (isa(buckets, Vector) ? buckets : [buckets])]
end


"""
    s3_list_objects([::AbstractAWSConfig], bucket, [path_prefix]; delimiter="/", max_items=1000, kwargs...)

[List Objects](http://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketGET.html)
in `bucket` with optional `path_prefix`.

Returns an iterator of `Dict`s with keys `Key`, `LastModified`, `ETag`, `Size`,
`Owner`, `StorageClass`.
"""
function s3_list_objects(aws::AbstractAWSConfig, bucket, path_prefix=""; delimiter="/", max_items=nothing, kwargs...)
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
                r = S3.list_objects(bucket, q; aws_config=aws, kwargs...)

                # Add each object from the response and update our object count / marker
                if haskey(r, "Contents")
                    l = isa(r["Contents"], Vector) ? r["Contents"] : [r["Contents"]]
                    for object in l
                        put!(chnl, object)
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

s3_list_objects(a...) = s3_list_objects(global_aws_config(), a...)


"""
    s3_list_keys([::AbstractAWSConfig], bucket, [path_prefix]; kwargs...)

Like [`s3_list_objects`](@ref) but returns object keys as `Vector{String}`.
"""
function s3_list_keys(aws::AbstractAWSConfig, bucket, path_prefix=""; kwargs...)
    (o["Key"] for o in s3_list_objects(aws, bucket, path_prefix; kwargs...))
end

s3_list_keys(a...; b...) = s3_list_keys(global_aws_config(), a...; b...)


"""
    s3_list_versions([::AbstractAWSConfig], bucket, [path_prefix]; kwargs...)

[List object versions](http://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketGETVersion.html) in `bucket` with optional `path_prefix`.
"""
function s3_list_versions(aws::AbstractAWSConfig, bucket, path_prefix=""; kwargs...)
    more = true
    versions = []
    marker = ""

    while more
        query = Dict{String, Any}("versions" => "", "prefix" => path_prefix, "return_raw"=>true)

        if !isempty(marker)
            query["key-marker"] = marker
        end

        r = S3.list_object_versions(bucket, query; aws_config=aws, kwargs...)
        r = parse_xml(String(r))

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

s3_list_versions(a...; b...) = s3_list_versions(global_aws_config(), a...; b...)


"""
    s3_purge_versions([::AbstractAWSConfig], bucket, [path [, pattern]]; kwargs...)

[DELETE](http://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectDELETE.html)
all object versions except for the latest version.
"""
function s3_purge_versions(aws::AbstractAWSConfig, bucket, path="", pattern=""; kwargs...)
    for v in s3_list_versions(aws, bucket, path; kwargs...)
        if pattern == "" || occursin(pattern, v["Key"])
            if v["IsLatest"] != "true"
                S3.delete_object(bucket, v["Key"], Dict("versionId"=>v["VersionId"]); aws_config=aws, kwargs...)
            end
        end
    end
end

s3_purge_versions(a...; b...) = s3_purge_versions(global_aws_config(), a...; b...)

"""
    s3_put([::AbstractAWSConfig], bucket, path, data, data_type="", encoding=""; <keyword arguments>)

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
function s3_put(aws::AbstractAWSConfig,
                bucket, path, data::Union{String,Vector{UInt8}},
                data_type="", encoding="";
                acl::AbstractString="",
                metadata::SSDict = SSDict(),
                tags::AbstractDict = SSDict(),
                kwargs...)
    headers = Dict{String, Any}(
        ["x-amz-meta-$k" => v for (k, v) in metadata]
    )

    if isempty(data_type)
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

    headers["Content-Type"] = data_type

    if !isempty(tags)
        headers["x-amz-tagging"] = HTTP.escapeuri(tags)
    end

    if !isempty(acl)
        headers["x-amz-acl"] = acl
    end

    if !isempty(encoding)
        headers["Content-Encoding"] = encoding
    end

    args = Dict("body"=>data, "headers"=>headers)

    S3.put_object(
        bucket,
        path,
        args;
        aws_config=aws,
        kwargs...
    )
end

s3_put(a...; b...) = s3_put(global_aws_config(), a...; b...)


function s3_begin_multipart_upload(aws::AbstractAWSConfig, bucket, path, args=Dict{String, Any}(); kwargs...)
    return S3.create_multipart_upload(bucket, path, args; aws_config=aws, kwargs...)
end


function s3_upload_part(aws::AbstractAWSConfig, upload, part_number, part_data; args=Dict{String, Any}(), kwargs...)
    args["body"] = part_data
    args["return_headers"] = true

    _, headers = S3.upload_part(
        upload["Bucket"],
        upload["Key"],
        part_number,
        upload["UploadId"],
        args;
        aws_config=aws, kwargs...
    )

    return Dict(headers)["ETag"]
end


function s3_complete_multipart_upload(aws::AbstractAWSConfig, upload, parts::Vector{String}, args=Dict{String, Any}(); kwargs...)
    doc = XMLDocument()
    rootnode = setroot!(doc, ElementNode("CompleteMultipartUpload"))

    for (i, etag) in enumerate(parts)
        part = addelement!(rootnode, "Part")
        addelement!(part, "PartNumber", string(i))
        addelement!(part, "ETag", etag)
    end

    args["body"] = string(doc)

    response = S3.complete_multipart_upload(
        upload["Bucket"],
        upload["Key"],
        upload["UploadId"],
        args;
        aws_config=aws,
        kwargs...
    )

    return response
end


function s3_multipart_upload(aws::AbstractAWSConfig, bucket, path, io::IO, part_size_mb=50; kwargs...)
    part_size = part_size_mb * 1024 * 1024

    upload = s3_begin_multipart_upload(aws, bucket, path)
    tags = Vector{String}()
    buf = Vector{UInt8}(undef, part_size)

    i = 0
    while (n = readbytes!(io, buf, part_size)) > 0
        if n < part_size
            resize!(buf, n)
        end

        push!(tags, s3_upload_part(aws, upload, (i += 1), buf; kwargs...))
    end

    s3_complete_multipart_upload(aws, upload, tags; kwargs...)
end

using MbedTLS

function _s3_sign_url_v2(
    aws::AbstractAWSConfig, bucket, path, seconds=3600;
    verb="GET", content_type="application/octet-stream", protocol="http",)

    path = HTTP.escapepath(path)

    expires = round(Int, Dates.datetime2unix(now(Dates.UTC)) + seconds)

    query = SSDict("AWSAccessKeyId" =>  aws.credentials.access_key_id,
                   "x-amz-security-token" => aws.credentials.token,
                   "Expires" => string(expires),
                   "response-content-disposition" => "attachment")

    if verb != "PUT"
        content_type = ""
    end

    to_sign = "$verb\n\n$content_type\n$(query["Expires"])\n" *
              "x-amz-security-token:$(query["x-amz-security-token"])\n" *
              "/$bucket/$path?" *
              "response-content-disposition=attachment"

    key = aws.credentials.secret_key
    query["Signature"] = digest(MD_SHA1, to_sign, key) |> base64encode |> strip

    endpoint=string(protocol, "://",
                    bucket, ".s3.", aws.region, ".amazonaws.com")
    return "$endpoint/$path?$(HTTP.escapeuri(query))"
end


function _s3_sign_url_v4(
    aws::AbstractAWSConfig, bucket, path, seconds=3600;
    verb="GET", content_type="application/octet-stream", protocol="http",)

    path = HTTP.escapepath("/$bucket/$path")

    now_datetime = now(Dates.UTC)
    datetime_stamp = Dates.format(now_datetime, "YYYYmmddTHHMMSS\\Z")
    date_stamp = Dates.format(now_datetime, "YYYYmmdd")

    service = "s3"
    scheme = "AWS4"
    algorithm = "HMAC-SHA256"
    terminator = "aws4_request"

    scope = "$date_stamp/$(aws.region)/$service/$terminator"
    host = if aws.region == "us-east-1"
        "s3.amazonaws.com"
    else
        "s3-$(aws.region).amazonaws.com"
    end

    headers = OrderedDict{String, String}("Host" => host)
    sort!(headers; by = name -> lowercase(name))
    canonical_header_names = join(map(name -> lowercase(name), headers |> keys |> collect), ";")

    query = OrderedDict{String, String}(
        "X-Amz-Expires" => string(seconds),
        "X-Amz-Algorithm" => "$scheme-$algorithm",
        "X-Amz-Credential" => "$(aws.credentials.access_key_id)/$scope",
        "X-Amz-Date" => datetime_stamp,
        "X-Amz-Security-Token" => aws.credentials.token,
        "X-Amz-SignedHeaders" => canonical_header_names
    )

    if !isempty(aws.credentials.token)
        query["X-Amz-Security-Token"] = aws.credentials.token
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

    key_secret = string(scheme, aws.credentials.secret_key)
    key_date = digest(MD_SHA256, date_stamp, key_secret)
    key_region = digest(MD_SHA256, aws.region, key_date)
    key_service = digest(MD_SHA256, service, key_region)
    key_signing = digest(MD_SHA256, terminator, key_service)
    signature = digest(MD_SHA256, string_to_sign, key_signing)

    query["X-Amz-Signature"] = signature |> bytes2hex

    return string(protocol, "://", host, path, "?", HTTP.escapeuri(query))
end


"""
    s3_sign_url([::AbstractAWSConfig], bucket, path, [seconds=3600];
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
    aws::AbstractAWSConfig, bucket, path, seconds=3600;
    verb="GET", content_type="application/octet-stream", protocol="http",
    signature_version="v4",)

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

s3_sign_url(a...;b...) = s3_sign_url(global_aws_config(), a...;b...)


"""
    s3_nuke_bucket(bucket_name)

This function is NOT exported on purpose. AWS does not officially support this type of action,
although it is a very nice utility one this is not exported just as a safe measure against
accidentally blowing up your bucket.

*Warning: It will delete all versions of objects in the given bucket and then the bucket itself.*
"""
function s3_nuke_bucket(aws::AbstractAWSConfig, bucket_name)
    for v in s3_list_versions(aws, bucket_name)
        s3_delete(aws, bucket_name, v["Key"]; version = v["VersionId"])
    end

    s3_delete_bucket(aws, bucket_name)
end


include("s3path.jl")

end #module AWSS3

#==============================================================================#
# End of file.
#==============================================================================#
