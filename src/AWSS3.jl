#==============================================================================#
# AWSS3.jl
#
# S3 API. See http://docs.aws.amazon.com/AmazonS3/latest/API/APIRest.html
#
# Copyright OC Technology Pty Ltd 2014 - All rights reserved
#==============================================================================#

module AWSS3

export S3Path,
    s3_arn,
    s3_put,
    s3_get,
    s3_get_file,
    s3_exists,
    s3_delete,
    s3_copy,
    s3_create_bucket,
    s3_put_cors,
    s3_enable_versioning,
    s3_delete_bucket,
    s3_list_buckets,
    s3_list_objects,
    s3_list_keys,
    s3_list_versions,
    s3_nuke_object,
    s3_get_meta,
    s3_directory_stat,
    s3_purge_versions,
    s3_sign_url,
    s3_begin_multipart_upload,
    s3_upload_part,
    s3_complete_multipart_upload,
    s3_multipart_upload,
    s3_get_tags,
    s3_put_tags,
    s3_delete_tags

using AWS
using AWS.AWSServices: s3
using ArrowTypes
using Base64
using Compat: @something
using Dates
using EzXML
using FilePathsBase
using FilePathsBase: /, join
using HTTP: HTTP
using Mocking
using OrderedCollections: OrderedDict, LittleDict
using Retry
using SymDict
using URIs
using UUIDs
using XMLDict

@service S3 use_response_type = true

const SSDict = Dict{String,String}
const AbstractS3Version = Union{AbstractString,Nothing}
const AbstractS3PathConfig = Union{AbstractAWSConfig,Nothing}

# Utility function to workaround https://github.com/JuliaCloud/AWS.jl/issues/547
function get_robust_case(x, key)
    lkey = lowercase(key)
    haskey(x, lkey) && return x[lkey]
    return x[key]
end

__init__() = FilePathsBase.register(S3Path)

# Declare new `parse` function to avoid type piracy
# TODO: remove when changes are released: https://github.com/JuliaCloud/AWS.jl/pull/502
function parse(r::AWS.Response, mime::MIME)
    # AWS doesn't always return a Content-Type which results the parsing returning bytes
    # instead of a dictionary. To address this we'll allow passing in the MIME type.
    return try
        AWS._rewind(r.io) do io
            AWS._read(io, mime)
        end
    catch e
        @warn "Failed to parse the following content as $mime:\n\"\"\"$(String(r.body))\"\"\""
        rethrow(e)
    end
end
parse(args...; kwargs...) = Base.parse(args...; kwargs...)

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

Retrieves an object from the `bucket` for a given `path`.

# Optional Arguments
- `version=nothing`: version of object to get.
- `retry=true`: try again on "NoSuchBucket", "NoSuchKey" (common if object was recently
  created).
- `raw=false`:  return response as `Vector{UInt8}`
- `byte_range=nothing`:  given an iterator of `(start_byte, end_byte)` gets only
  the range of bytes of the object from `start_byte` to `end_byte`.  For example,
  `byte_range=1:4` gets bytes 1 to 4 inclusive.  Arguments should use the Julia convention
  of 1-based indexing.
- `header::Dict{String,String}`: pass in an HTTP header to the request.

As an example of how to set custom HTTP headers, the below is equivalent to
`s3_get(aws, bucket, path; byte_range=range)`:

```julia
s3_get(aws, bucket, path; headers=Dict{String,String}("Range" => "bytes=\$(first(range)-1)-\$(last(range)-1)"))
```

# API Calls

- [`GetObject`](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html)

# Permissions

- [`s3:GetObject`](https://docs.aws.amazon.com/service-authorization/latest/reference/list_amazons3.html#amazons3-GetObject):
  (conditional): required when `version === nothing`.
- [`s3:GetObjectVersion`](https://docs.aws.amazon.com/service-authorization/latest/reference/list_amazons3.html#amazons3-GetObjectVersion):
  (conditional): required when `version !== nothing`.
- [`s3:ListBucket`](https://docs.aws.amazon.com/service-authorization/latest/reference/list_amazons3.html#amazons3-ListBucket)
  (optional): allows requests to non-existent objects to throw a exception with HTTP status
  code 404 (Not Found) instead of HTTP status code 403 (Access Denied).
"""
function s3_get(
    aws::AbstractAWSConfig,
    bucket,
    path;
    version::AbstractS3Version=nothing,
    retry::Bool=true,
    raw::Bool=false,
    byte_range::Union{Nothing,AbstractVector}=nothing,
    headers::AbstractDict{<:AbstractString,<:Any}=Dict{String,Any}(),
    return_stream::Bool=false,
    kwargs...,
)
    @repeat 4 try
        params = Dict{String,Any}()
        return_stream && (params["response_stream"] = Base.BufferStream())
        if version !== nothing
            params["versionId"] = version
        end

        if byte_range !== nothing
            headers = copy(headers)  # make sure we don't mutate existing object
            # we make sure we stick to the Julia convention of 1-based indexing
            a, b = (first(byte_range) - 1), (last(byte_range) - 1)
            headers["Range"] = "bytes=$a-$b"
        end

        if !isempty(headers)
            params["headers"] = headers
        end

        r = S3.get_object(bucket, path, params; aws_config=aws, kwargs...)
        return if return_stream
            close(r.io)
            r.io
        elseif raw
            r.body
        else
            parse(r)
        end
    catch e
        #! format: off
        # https://github.com/domluna/JuliaFormatter.jl/issues/459
        @delay_retry if retry && ecode(e) in ["NoSuchBucket", "NoSuchKey"] end
        #! format: on
    end
end

s3_get(a...; b...) = s3_get(global_aws_config(), a...; b...)

"""
    s3_get_file([::AbstractAWSConfig], bucket, path, filename; [version=], kwargs...)

Like `s3_get` but streams result directly to `filename`.  Keyword arguments accept are
the same as those for `s3_get`.
"""
function s3_get_file(
    aws::AbstractAWSConfig,
    bucket,
    path,
    filename;
    version::AbstractS3Version=nothing,
    kwargs...,
)
    stream = s3_get(aws, bucket, path; version=version, return_stream=true, kwargs...)

    open(filename, "w") do file
        while !eof(stream)
            write(file, readavailable(stream))
        end
    end
end

s3_get_file(a...; b...) = s3_get_file(global_aws_config(), a...; b...)

function s3_get_file(
    aws::AbstractAWSConfig,
    buckets::Vector,
    path,
    filename;
    version::AbstractS3Version=nothing,
    kwargs...,
)
    i = start(buckets)

    @repeat length(buckets) try
        bucket, i = next(buckets, i)
        s3_get_file(aws, bucket, path, filename; version=version, kwargs...)
    catch e
        #! format: off
        @retry if ecode(e) in ["NoSuchKey", "AccessDenied"] end
        #! format: on
    end
end

"""
   s3_get_meta([::AbstractAWSConfig], bucket, path; [version], kwargs...)

Retrieves metadata from an object without returning the object itself.

# API Calls

- [`HeadObject`](https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html)

# Permissions

- [`s3:GetObject`](https://docs.aws.amazon.com/service-authorization/latest/reference/list_amazons3.html#amazons3-GetObject)
  (conditional): required when `version === nothing`.
- [`s3:GetObjectVersion`](https://docs.aws.amazon.com/service-authorization/latest/reference/list_amazons3.html#amazons3-GetObjectVersion):
  (conditional): required when `version !== nothing`.
- [`s3:ListBucket`](https://docs.aws.amazon.com/service-authorization/latest/reference/list_amazons3.html#amazons3-ListBucket)
  (optional): allows requests to non-existent objects to throw a exception with HTTP status
  code 404 (Not Found) instead of HTTP status code 403 (Access Denied).
"""
function s3_get_meta(
    aws::AbstractAWSConfig, bucket, path; version::AbstractS3Version=nothing, kwargs...
)
    params = Dict{String,Any}()
    if version !== nothing
        params["versionId"] = version
    end

    r = S3.head_object(bucket, path, params; aws_config=aws, kwargs...)
    return Dict(r.headers)
end

s3_get_meta(a...; b...) = s3_get_meta(global_aws_config(), a...; b...)

function _s3_exists_file(aws::AbstractAWSConfig, bucket, path)
    q = Dict("prefix" => path, "delimiter" => "/", "max-keys" => 1)
    l = parse(S3.list_objects_v2(bucket, q; aws_config=aws))
    c = get(l, "Contents", nothing)
    c === nothing && return false
    return get(c, "Key", "") == path
end

"""
    _s3_exists_dir(aws::AbstractAWSConfig, bucket, path)

An internal function used by [`s3_exists`](@ref).

Checks if the given directory exists within the `bucket`. Since S3 uses a flat structure, as
opposed to being hierarchical like a file system, directories are actually just a collection
of object keys which share a common prefix. S3 implements empty directories as
[0-byte objects](https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-folders.html)
with keys ending with the delimiter.

It is possible to create non 0-byte objects with a key ending in the delimiter
(e.g. `s3_put(bucket, "abomination/", "If I cannot inspire love, I will cause fear!")`)
which the AWS console interprets as the directory "abmonination" containing the object "/".
"""
function _s3_exists_dir(aws::AbstractAWSConfig, bucket, path)
    endswith(path, '/') || throw(ArgumentError("S3 directories must end with '/': $path"))
    q = Dict("prefix" => path, "delimiter" => "/", "max-keys" => 1)
    r = parse(S3.list_objects_v2(bucket, q; aws_config=aws))
    return get(r, "KeyCount", "0") != "0"
end

"""
    s3_exists_versioned([::AbstractAWSConfig], bucket, path, version)

Returns if an object `version` exists with the key `path` in the `bucket`.

Note that the AWS API's support for object versioning is quite limited and this check will
involve `try`/`catch` logic. Prefer using [`s3_exists_unversioned `](@ref) where possible
for more performant checks.

See [`s3_exists`](@ref) and [`s3_exists_unversioned`](@ref).

# API Calls

- [`ListObjectV2`](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html)

# Permissions

- [`s3:GetObjectVersion`](https://docs.aws.amazon.com/service-authorization/latest/reference/list_amazons3.html#amazons3-GetObjectVersion)
- [`s3:ListBucket`](https://docs.aws.amazon.com/service-authorization/latest/reference/list_amazons3.html#amazons3-ListBucket)
  (optional): allows requests to non-existent objects to throw a exception with HTTP status
  code 404 (Not Found) instead of HTTP status code 403 (Access Denied).
"""
function s3_exists_versioned(
    aws::AbstractAWSConfig, bucket, path, version::AbstractS3Version
)
    @repeat 2 try
        s3_get_meta(aws, bucket, path; version=version)
        return true
    catch e
        #! format: off
        @delay_retry if ecode(e) in ["NoSuchBucket", "404", "NoSuchKey", "AccessDenied"] end
        #! format: on

        @ignore if ecode(e) in ["404", "NoSuchKey", "AccessDenied"]
            return false
        end
    end
end

"""
    s3_exists_unversioned([::AbstractAWSConfig], bucket, path)

Returns a boolean whether an object exists at  `path` in `bucket`.

See [`s3_exists`](@ref) and [`s3_exists_versioned`](@ref).

# API Calls

- [`ListObjectV2`](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html)

# Permissions

- [`s3:GetObject`](https://docs.aws.amazon.com/service-authorization/latest/reference/list_amazons3.html#amazons3-GetObjectVersion)
- [`s3:ListBucket`](https://docs.aws.amazon.com/service-authorization/latest/reference/list_amazons3.html#amazons3-ListBucket)
  (optional): allows requests to non-existent objects to throw a exception with HTTP status
  code 404 (Not Found) instead of HTTP status code 403 (Access Denied).
"""
function s3_exists_unversioned(aws::AbstractAWSConfig, bucket, path)
    f = endswith(path, '/') ? _s3_exists_dir : _s3_exists_file
    return f(aws, bucket, path)
end

"""
    s3_exists([::AbstractAWSConfig], bucket, path; version=nothing)

Returns if an object exists with the key `path` in the `bucket`. If a `version` is specified
then an object must exist with the specified version identifier.

Note that the AWS API's support for object versioning is quite limited and this check will
involve `try`/`catch` logic if a `version` is specified.

# API Calls

- [`ListObjectV2`](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html)

# Permissions

- [`s3:GetObject`](https://docs.aws.amazon.com/service-authorization/latest/reference/list_amazons3.html#amazons3-GetObject)
  (conditional): required when `version === nothing`.
- [`s3:GetObjectVersion`](https://docs.aws.amazon.com/service-authorization/latest/reference/list_amazons3.html#amazons3-GetObjectVersion):
  (conditional): required when `version !== nothing`.
- [`s3:ListBucket`](https://docs.aws.amazon.com/service-authorization/latest/reference/list_amazons3.html#amazons3-ListBucket)
  (optional): allows requests to non-existent objects to throw a exception with HTTP status
  code 404 (Not Found) instead of HTTP status code 403 (Access Denied).
"""
function s3_exists(aws::AbstractAWSConfig, bucket, path; version::AbstractS3Version=nothing)
    if version !== nothing
        s3_exists_versioned(aws, bucket, path, version)
    else
        s3_exists_unversioned(aws, bucket, path)
    end
end
s3_exists(a...; b...) = s3_exists(global_aws_config(), a...; b...)

"""
    s3_delete([::AbstractAWSConfig], bucket, path; [version], kwargs...)

Deletes an object from a bucket. The `version` argument can be used to delete a specific
version.

# API Calls

- [`DeleteObject`](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObject.html)

# Permissions

- [`s3:DeleteObject`](https://docs.aws.amazon.com/service-authorization/latest/reference/list_amazons3.html#amazons3-DeleteObject)
"""
function s3_delete(
    aws::AbstractAWSConfig, bucket, path; version::AbstractS3Version=nothing, kwargs...
)
    params = Dict{String,Any}()
    if version !== nothing
        params["versionId"] = version
    end

    return parse(S3.delete_object(bucket, path, params; aws_config=aws, kwargs...))
end

s3_delete(a...; b...) = s3_delete(global_aws_config(), a...; b...)

"""
    s3_nuke_object([::AbstractAWSConfig], bucket, path; kwargs...)

Deletes all versions of object `path` from `bucket`. All provided `kwargs` are forwarded to
[`s3_delete`](@ref). In the event an error occurs any object versions already deleted by
`s3_nuke_object` will be lost.

To only delete one specific version, use [`s3_delete`](@ref); to delete all versions
EXCEPT the latest version, use [`s3_purge_versions`](@ref); to delete all versions
in an entire bucket, use [`AWSS3.s3_nuke_bucket`](@ref).

# API Calls

- [`DeleteObject`](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObject.html)
- [`ListObjectVersions`](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectVersions.html)

# Permissions

- [`s3:DeleteObject`](https://docs.aws.amazon.com/service-authorization/latest/reference/list_amazons3.html#amazons3-DeleteObject)
- [`s3:ListBucketVersions`](https://docs.aws.amazon.com/service-authorization/latest/reference/list_amazons3.html#amazons3-ListBucketVersions)
"""
function s3_nuke_object(aws::AbstractAWSConfig, bucket, path; kwargs...)
    # Because list_versions returns ALL keys with the given _prefix_, we need to
    # restrict the results to ones with the _exact same_ key.
    for object in s3_list_versions(aws, bucket, path)
        object["Key"] == path || continue
        version = object["VersionId"]
        try
            s3_delete(aws, bucket, path; version, kwargs...)
        catch e
            @warn "Failed to delete version $(version) of $(path)"
            rethrow(e)
        end
    end
    return nothing
end

function s3_nuke_object(bucket, path; kwargs...)
    return s3_nuke_object(global_aws_config(), bucket, path; kwargs...)
end

"""
    s3_copy([::AbstractAWSConfig], bucket, path; acl::AbstractString="",
            to_bucket=bucket, to_path=path, metadata::AbstractDict=SSDict(),
            parse_response::Bool=true, kwargs...)

Copy the object at `path` in `bucket` to `to_path` in `to_bucket`.

# Optional Arguments
- `acl=`; `x-amz-acl` header for setting access permissions with canned config.
    See [here](https://docs.aws.amazon.com/AmazonS3/latest/dev/acl-overview.html#canned-acl).
- `metadata::Dict=`; `x-amz-meta-` headers.
- `parse_response::Bool=`; when `false`, return raw `AWS.Response`
- `kwargs`; additional kwargs passed through into `S3.copy_object`

# API Calls

- [`CopyObject`](http://https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyObject.html)

# Permissions

- [`s3:PutObject`](https://docs.aws.amazon.com/service-authorization/latest/reference/list_amazons3.html#amazons3-PutObject)
- [`s3:GetObject`](https://docs.aws.amazon.com/service-authorization/latest/reference/list_amazons3.html#amazons3-GetObject)
"""
function s3_copy(
    aws::AbstractAWSConfig,
    bucket,
    path;
    acl::AbstractString="",
    to_bucket=bucket,
    to_path=path,
    metadata::AbstractDict=SSDict(),
    parse_response::Bool=true,
    kwargs...,
)
    headers = SSDict(
        "x-amz-metadata-directive" => "REPLACE",
        Pair["x-amz-meta-$k" => v for (k, v) in metadata]...,
    )

    if !isempty(acl)
        headers["x-amz-acl"] = acl
    end

    response = S3.copy_object(
        to_bucket,
        to_path,
        "$bucket/$path",
        Dict("headers" => headers);
        aws_config=aws,
        kwargs...,
    )
    return parse_response ? parse(response) : response
end

s3_copy(a...; b...) = s3_copy(global_aws_config(), a...; b...)

"""
    s3_create_bucket([::AbstractAWSConfig], bucket; kwargs...)

Creates a new S3 bucket with the globally unique `bucket` name. The bucket will be created
AWS region associated with the `AbstractAWSConfig` (defaults to "us-east-1").

# API Calls

- [`CreateBucket`](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateBucket.html)

# Permissions

- [`s3:CreateBucket`](https://docs.aws.amazon.com/service-authorization/latest/reference/list_amazons3.html#amazons3-CreateBucket)
"""
function s3_create_bucket(aws::AbstractAWSConfig, bucket; kwargs...)
    r = @protected try
        if aws.region == "us-east-1"
            S3.create_bucket(bucket; aws_config=aws, kwargs...)
        else
            bucket_config = """
                <CreateBucketConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                    <LocationConstraint>$(aws.region)</LocationConstraint>
                </CreateBucketConfiguration>
                """

            S3.create_bucket(
                bucket,
                Dict("CreateBucketConfiguration" => bucket_config);
                aws_config=aws,
                kwargs...,
            )
        end
    catch e
        #! format: off
        @ignore if ecode(e) == "BucketAlreadyOwnedByYou" end
        #! format: on
    end
    return parse(r)
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
    return parse(S3.put_bucket_cors(bucket, cors_config; aws_config=aws, kwargs...))
end

s3_put_cors(a...; b...) = s3_put_cors(AWS.global_aws_config(), a...; b...)

"""
    s3_enable_versioning([::AbstractAWSConfig], bucket, [status]; kwargs...)

Enables or disables versioning for all objects within the given `bucket`. Use `status` to
either enable or disable versioning (respectively "Enabled" and "Suspended").

# API Calls

- [`PutBucketVersioning`](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketVersioning.html)

# Permissions

- [`s3:PutBucketVersioning`](https://docs.aws.amazon.com/service-authorization/latest/reference/list_amazons3.html#amazons3-PutBucketVersioning)
"""
function s3_enable_versioning(aws::AbstractAWSConfig, bucket, status="Enabled"; kwargs...)
    versioning_config = """
        <VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <Status>$status</Status>
        </VersioningConfiguration>
        """

    r = s3(
        "PUT",
        "/$(bucket)?versioning",
        Dict("body" => versioning_config);
        aws_config=aws,
        feature_set=AWS.FeatureSet(; use_response_type=true),
        kwargs...,
    )
    return parse(r)
end

s3_enable_versioning(a; b...) = s3_enable_versioning(global_aws_config(), a; b...)

"""
    s3_put_tags([::AbstractAWSConfig], bucket, [path], tags::Dict; kwargs...)

Sets the tags for a bucket or an existing object. When `path` is specified then tagging
is performed on the object, otherwise it is performed on the `bucket`.

See also [`s3_put_tags`](@ref), [`s3_delete_tags`](@ref), and [`s3_put`'s](@ref) `tag`
option.

# API Calls

- [`PutBucketTagging`](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketTagging.html)(conditional): used when `path` is not specified (bucket tagging).
- [`PutObjectTagging`](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectTagging.html) (conditional): used when `path` is specified (object tagging).

# Permissions

- [`s3:PutBucketTagging`](https://docs.aws.amazon.com/service-authorization/latest/reference/list_amazons3.html#amazons3-PutBucketTagging)
  (conditional): required for when `path` is not specified (bucket tagging).
- [`s3:PutObjectTagging`](https://docs.aws.amazon.com/service-authorization/latest/reference/list_amazons3.html#amazons3-PutObjectTagging)
  (conditional): required when `path` is specified (object tagging).
"""
function s3_put_tags(aws::AbstractAWSConfig, bucket, path, tags::SSDict; kwargs...)
    tag_set = Dict("Tag" => [Dict("Key" => k, "Value" => v) for (k, v) in tags])
    tags = Dict("Tagging" => Dict("TagSet" => tag_set))

    tags = XMLDict.node_xml(tags)

    uri_path = isempty(path) ? "/$(bucket)?tagging" : "/$(bucket)/$(path)?tagging"

    r = s3(
        "PUT",
        uri_path,
        Dict("body" => tags);
        feature_set=AWS.FeatureSet(; use_response_type=true),
        aws_config=aws,
        kwargs...,
    )
    return parse(r)
end

function s3_put_tags(aws::AbstractAWSConfig, bucket, tags::SSDict; kwargs...)
    return s3_put_tags(aws, bucket, "", tags; kwargs...)
end

s3_put_tags(a...) = s3_put_tags(global_aws_config(), a...)

"""
    s3_get_tags([::AbstractAWSConfig], bucket, [path]; kwargs...)

Get the tags associated with a bucket or an existing object. When `path` is specified then
tag retrieval is performed on the object, otherwise it is performed on the `bucket`.

See also [`s3_put_tags`](@ref) and [`s3_delete_tags`](@ref).

# API Calls

- [`GetBucketTagging`](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketTagging.html)  (conditional): used when `path` is not specified (bucket tagging).
- [`GetObjectTagging`](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectTagging.html) (conditional): used when `path` is specified (object tagging).

# Permissions

- [`s3:GetBucketTagging`](https://docs.aws.amazon.com/service-authorization/latest/reference/list_amazons3.html#amazons3-GetBucketTagging)
  (conditional): required for when `path` is not specified (bucket tagging).
- [`s3:GetObjectTagging`](https://docs.aws.amazon.com/service-authorization/latest/reference/list_amazons3.html#amazons3-GetObjectTagging)
  (conditional): required when `path` is specified (object tagging).
"""
function s3_get_tags(aws::AbstractAWSConfig, bucket, path=""; kwargs...)
    @protected try
        tags = []

        r = if isempty(path)
            S3.get_bucket_tagging(bucket; aws_config=aws, kwargs...)
        else
            S3.get_object_tagging(bucket, path; aws_config=aws, kwargs...)
        end
        tags = parse(r, MIME"application/xml"())

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

Delete the tags associated with a bucket or an existing object. When `path` is specified then
tag deletion is performed on the object, otherwise it is performed on the `bucket`.

See also [`s3_put_tags`](@ref) and [`s3_get_tags`](@ref).

# API Calls

- [`DeleteBucketTagging`](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucketTagging.html) (conditional): used when `path` is not specified (bucket tagging).
- [`DeleteObjectTagging`](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjectTagging.html) (conditional): used when `path` is specified (object tagging).

# Permissions

- [`s3:PutBucketTagging`](https://docs.aws.amazon.com/service-authorization/latest/reference/list_amazons3.html#amazons3-PutBucketTagging)
  (conditional): required for when `path` is not specified (bucket tagging).
- [`s3:DeleteObjectTagging`](https://docs.aws.amazon.com/service-authorization/latest/reference/list_amazons3.html#amazons3-DeleteObjectTagging)
  (conditional): required when `path` is specified (object tagging).
"""
function s3_delete_tags(aws::AbstractAWSConfig, bucket, path=""; kwargs...)
    r = if isempty(path)
        S3.delete_bucket_tagging(bucket; aws_config=aws, kwargs...)
    else
        S3.delete_object_tagging(bucket, path; aws_config=aws, kwargs...)
    end
    return parse(r)
end

s3_delete_tags(a...; b...) = s3_delete_tags(global_aws_config(), a...; b...)

"""
    s3_delete_bucket([::AbstractAWSConfig], "bucket"; kwargs...)

Deletes an empty bucket. All objects in the bucket must be deleted before a bucket can be
deleted.

See also [`AWSS3.s3_nuke_bucket`](@ref).

# API Calls

- [`DeleteBucket`](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucket.html)

# Permissions

- [`s3:DeleteBucket`](https://docs.aws.amazon.com/service-authorization/latest/reference/list_amazons3.html#amazons3-DeleteBucket)
"""
function s3_delete_bucket(aws::AbstractAWSConfig, bucket; kwargs...)
    return parse(S3.delete_bucket(bucket; aws_config=aws, kwargs...))
end
s3_delete_bucket(a; b...) = s3_delete_bucket(global_aws_config(), a; b...)

"""
    s3_list_buckets([::AbstractAWSConfig]; kwargs...)

Return a list of all of the buckets owned by the authenticated sender of the request.

# API Calls

- [`ListBuckets`](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListBuckets.html)

# Permissions

- [`s3:ListAllMyBuckets`](https://docs.aws.amazon.com/service-authorization/latest/reference/list_amazons3.html#amazons3-ListAllMyBuckets)
"""
function s3_list_buckets(aws::AbstractAWSConfig=global_aws_config(); kwargs...)
    r = S3.list_buckets(; aws_config=aws, kwargs...)
    buckets = parse(r)["Buckets"]

    isempty(buckets) && return []

    buckets = buckets["Bucket"]
    return [b["Name"] for b in (isa(buckets, Vector) ? buckets : [buckets])]
end

"""
    s3_list_objects([::AbstractAWSConfig], bucket, [path_prefix]; delimiter="/", max_items=1000, kwargs...)

[List Objects](http://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketGET.html)
in `bucket` with optional `path_prefix`.

Returns an iterator of `Dict`s with keys `Key`, `LastModified`, `ETag`, `Size`,
`Owner`, `StorageClass`.
"""
function s3_list_objects(
    aws::AbstractAWSConfig,
    bucket,
    path_prefix="";
    delimiter="/",
    start_after="",
    max_items=nothing,
    kwargs...,
)
    return Channel(; ctype=LittleDict, csize=128) do chnl
        more = true
        num_objects = 0
        token = ""

        while more
            q = Dict{String,String}("prefix" => path_prefix)

            for (name, v) in [
                ("delimiter", delimiter),
                ("start-after", start_after),
                ("continuation-token", token),
            ]
                isempty(v) || (q[name] = v)
            end

            if max_items !== nothing
                # Note: AWS seems to only return up to 1000 items
                q["max-keys"] = string(max_items - num_objects)
            end

            @repeat 4 try
                # Request objects
                r = parse(S3.list_objects_v2(bucket, q; aws_config=aws, kwargs...))

                token = get(r, "NextContinuationToken", "")
                isempty(token) && (more = false)
                if haskey(r, "Contents")
                    l = isa(r["Contents"], Vector) ? r["Contents"] : [r["Contents"]]
                    for object in l
                        put!(chnl, object)
                        num_objects += 1
                    end
                end
            catch e
                #! format: off
                @delay_retry if ecode(e) in ["NoSuchBucket"] end
                #! format: on
            end
        end
    end
end
s3_list_objects(a...; kw...) = s3_list_objects(global_aws_config(), a...; kw...)

"""
    s3_directory_stat([::AbstractAWSConfig], bucket, path)

Determine the properties of an S3 "directory", size and time of last modification, that cannot be determined
directly with the standard AWS API.  This returns a tuple `(s, tmlast)` where `s` is the size in bytes, and
`tmlast` is the time of the latest modification to a file within that directory.
"""
function s3_directory_stat(
    aws::AbstractAWSConfig, bucket::AbstractString, path::AbstractString
)
    s = 0  # total size in bytes
    tmlast = typemin(DateTime)
    # setting delimiter is needed to get all objects within path,
    # additionally, we have to make sure the path ends with "/" or it will pick up extra stuff
    endswith(path, "/") || (path = path * "/")
    for obj in s3_list_objects(aws, bucket, path; delimiter="")
        s += parse(Int, get(obj, "Size", "0"))
        t = get(obj, "LastModified", nothing)
        t = t ≡ nothing ? tmlast : DateTime(t[1:(end - 4)])
        tmlast = max(tmlast, t)
    end
    return s, tmlast
end
s3_directory_stat(a...) = s3_directory_stat(global_aws_config(), a...)

"""
    s3_list_keys([::AbstractAWSConfig], bucket, [path_prefix]; kwargs...)

Like [`s3_list_objects`](@ref) but returns object keys as `Vector{String}`.
"""
function s3_list_keys(aws::AbstractAWSConfig, bucket, path_prefix=""; kwargs...)
    return (o["Key"] for o in s3_list_objects(aws, bucket, path_prefix; kwargs...))
end

s3_list_keys(a...; b...) = s3_list_keys(global_aws_config(), a...; b...)

"""
    s3_list_versions([::AbstractAWSConfig], bucket, [path_prefix]; kwargs...)

List metadata about all versions of the objects in the `bucket` matching the
optional `path_prefix`.

# API Calls

- [`ListObjectVersions`](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectVersions.html)

# Permissions

- [`s3:ListBucketVersions`](https://docs.aws.amazon.com/service-authorization/latest/reference/list_amazons3.html#amazons3-ListBucketVersions)
"""
function s3_list_versions(aws::AbstractAWSConfig, bucket, path_prefix=""; kwargs...)
    more = true
    versions = []
    marker = ""

    while more
        query = Dict{String,Any}("prefix" => path_prefix)

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
    s3_purge_versions([::AbstractAWSConfig], bucket, [path_prefix [, pattern]]; kwargs...)

Removes all versions of an object except for the latest version. When `path_prefix` is
provided then only objects whose key starts with `path_prefix` will be purged. Use of
`pattern` further restricts which objects are purged by only purging object keys containing
the `pattern` (i.e string literal or regex). When both `path_prefix` and `pattern` are not'
specified then all objects in the bucket will be purged.

# API Calls

- [`ListObjectVersions`](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectVersions.html)
- [`DeleteObject`](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObject.html)

# Permissions

- [`s3:ListBucketVersions`](https://docs.aws.amazon.com/service-authorization/latest/reference/list_amazons3.html#amazons3-ListBucketVersions)
- [`s3:DeleteObjectVersion`](https://docs.aws.amazon.com/service-authorization/latest/reference/list_amazons3.html#amazons3-DeleteObjectVersion)
"""
function s3_purge_versions(
    aws::AbstractAWSConfig, bucket, path_prefix="", pattern=""; kwargs...
)
    for v in s3_list_versions(aws, bucket, path_prefix; kwargs...)
        if pattern == "" || occursin(pattern, v["Key"])
            if v["IsLatest"] != "true"
                S3.delete_object(
                    bucket,
                    v["Key"],
                    Dict("versionId" => v["VersionId"]);
                    aws_config=aws,
                    kwargs...,
                )
            end
        end
    end
end

s3_purge_versions(a...; b...) = s3_purge_versions(global_aws_config(), a...; b...)

"""
    s3_put([::AbstractAWSConfig], bucket, path, data, data_type="", encoding="";
           acl::AbstractString="", metadata::SSDict=SSDict(), tags::AbstractDict=SSDict(),
           parse_response::Bool=true, kwargs...)

[PUT Object](http://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectPUT.html)
`data` at `path` in `bucket`.

# Optional Arguments
- `data_type=`; `Content-Type` header.
- `encoding=`; `Content-Encoding` header.
- `acl=`; `x-amz-acl` header for setting access permissions with canned config.
    See [here](https://docs.aws.amazon.com/AmazonS3/latest/dev/acl-overview.html#canned-acl).
- `metadata::Dict=`; `x-amz-meta-` headers.
- `tags::Dict=`; `x-amz-tagging-` headers
                 (see also [`s3_put_tags`](@ref) and [`s3_get_tags`](@ref)).
- `parse_response::Bool=`; when `false`, return raw `AWS.Response`
- `kwargs`; additional kwargs passed through into `S3.put_object`
"""
function s3_put(
    aws::AbstractAWSConfig,
    bucket,
    path,
    data::Union{String,Vector{UInt8}},
    data_type="",
    encoding="";
    acl::AbstractString="",
    metadata::SSDict=SSDict(),
    tags::AbstractDict=SSDict(),
    parse_response::Bool=true,
    kwargs...,
)
    headers = Dict{String,Any}(["x-amz-meta-$k" => v for (k, v) in metadata])

    if isempty(data_type)
        data_type = "application/octet-stream"
        ext = splitext(path)[2]
        for (e, t) in [
            (".html", "text/html"),
            (".js", "application/javascript"),
            (".pdf", "application/pdf"),
            (".csv", "text/csv"),
            (".txt", "text/plain"),
            (".log", "text/plain"),
            (".dat", "application/octet-stream"),
            (".gz", "application/octet-stream"),
            (".bz2", "application/octet-stream"),
        ]
            if ext == e
                data_type = t
                break
            end
        end
    end

    headers["Content-Type"] = data_type

    if !isempty(tags)
        headers["x-amz-tagging"] = URIs.escapeuri(tags)
    end

    if !isempty(acl)
        headers["x-amz-acl"] = acl
    end

    if !isempty(encoding)
        headers["Content-Encoding"] = encoding
    end

    args = Dict("body" => data, "headers" => headers)

    response = S3.put_object(bucket, path, args; aws_config=aws, kwargs...)
    return parse_response ? parse(response) : response
end

s3_put(a...; b...) = s3_put(global_aws_config(), a...; b...)

function s3_begin_multipart_upload(
    aws::AbstractAWSConfig,
    bucket,
    path,
    args=Dict{String,Any}();
    kwargs...,
    # format trick: using this comment to force use of multiple lines
)
    r = S3.create_multipart_upload(bucket, path, args; aws_config=aws, kwargs...)
    return parse(r, MIME"application/xml"())
end

function s3_upload_part(
    aws::AbstractAWSConfig,
    upload,
    part_number,
    part_data;
    args=Dict{String,Any}(),
    kwargs...,
)
    args["body"] = part_data

    response = S3.upload_part(
        upload["Bucket"],
        upload["Key"],
        part_number,
        upload["UploadId"],
        args;
        aws_config=aws,
        kwargs...,
    )

    return get_robust_case(Dict(response.headers), "ETag")
end

function s3_complete_multipart_upload(
    aws::AbstractAWSConfig,
    upload,
    parts::Vector{String},
    args=Dict{String,Any}();
    parse_response::Bool=true,
    kwargs...,
)
    doc = XMLDocument()
    rootnode = setroot!(doc, ElementNode("CompleteMultipartUpload"))

    for (i, etag) in enumerate(parts)
        part = addelement!(rootnode, "Part")
        addelement!(part, "PartNumber", string(i))
        addelement!(part, "ETag", etag)
    end

    args["body"] = string(doc)

    response = S3.complete_multipart_upload(
        upload["Bucket"], upload["Key"], upload["UploadId"], args; aws_config=aws, kwargs...
    )

    return parse_response ? parse(response) : response
end

"""
    s3_multipart_upload(aws::AbstractAWSConfig, bucket, path, io::IO, part_size_mb=50;
                        parse_response::Bool=true, kwargs...)

Upload `data` at `path` in `bucket` using a [multipart upload](https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html)

# Optional Arguments
- `part_size_mb`: maximum size per uploaded part, in bytes.
- `parse_response`: when `false`, return raw `AWS.Response`
- `kwargs`: additional kwargs passed through into `s3_upload_part` and `s3_complete_multipart_upload`

# API Calls

- [`CreateMultipartUpload`](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateMultipartUpload.html)
- [`UploadPart`](https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPart.html)
- [`CompleteMultipartUpload`](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html)
"""
function s3_multipart_upload(
    aws::AbstractAWSConfig,
    bucket,
    path,
    io::IO,
    part_size_mb=50;
    parse_response::Bool=true,
    kwargs...,
)
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

    return s3_complete_multipart_upload(aws, upload, tags; parse_response, kwargs...)
end

using MbedTLS

function _s3_sign_url_v2(
    aws::AbstractAWSConfig,
    bucket,
    path,
    seconds=3600;
    verb="GET",
    content_type="application/octet-stream",
    protocol="http",
)
    path = URIs.escapepath(path)

    expires = round(Int, Dates.datetime2unix(now(Dates.UTC)) + seconds)

    query = SSDict(
        "AWSAccessKeyId" => aws.credentials.access_key_id,
        "x-amz-security-token" => aws.credentials.token,
        "Expires" => string(expires),
        "response-content-disposition" => "attachment",
    )

    if verb != "PUT"
        content_type = ""
    end

    to_sign =
        "$verb\n\n$content_type\n$(query["Expires"])\n" *
        "x-amz-security-token:$(query["x-amz-security-token"])\n" *
        "/$bucket/$path?" *
        "response-content-disposition=attachment"

    key = aws.credentials.secret_key
    query["Signature"] = strip(base64encode(digest(MD_SHA1, to_sign, key)))

    endpoint = string(protocol, "://", bucket, ".s3.", aws.region, ".amazonaws.com")
    return "$endpoint/$path?$(URIs.escapeuri(query))"
end

function _s3_sign_url_v4(
    aws::AbstractAWSConfig,
    bucket,
    path,
    seconds=3600;
    verb="GET",
    content_type="application/octet-stream",
    protocol="http",
)
    path = URIs.escapepath("/$bucket/$path")

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

    headers = OrderedDict{String,String}("Host" => host)
    sort!(headers; by=name -> lowercase(name))
    canonical_header_names = join(map(name -> lowercase(name), collect(keys(headers))), ";")

    query = OrderedDict{String,String}(
        "X-Amz-Expires" => string(seconds),
        "X-Amz-Algorithm" => "$scheme-$algorithm",
        "X-Amz-Credential" => "$(aws.credentials.access_key_id)/$scope",
        "X-Amz-Date" => datetime_stamp,
        "X-Amz-Security-Token" => aws.credentials.token,
        "X-Amz-SignedHeaders" => canonical_header_names,
    )

    if !isempty(aws.credentials.token)
        query["X-Amz-Security-Token"] = aws.credentials.token
    end

    sort!(query; by=name -> lowercase(name))

    canonical_headers = join(
        map(
            header -> "$(lowercase(header.first)):$(lowercase(header.second))\n",
            collect(headers),
        ),
    )

    canonical_request = string(
        "$verb\n",
        "$path\n",
        "$(URIs.escapeuri(query))\n",
        "$canonical_headers\n",
        "$canonical_header_names\n",
        "UNSIGNED-PAYLOAD",
    )

    string_to_sign = string(
        "$scheme-$algorithm\n",
        "$datetime_stamp\n",
        "$scope\n",
        bytes2hex(digest(MD_SHA256, canonical_request)),
    )

    key_secret = string(scheme, aws.credentials.secret_key)
    key_date = digest(MD_SHA256, date_stamp, key_secret)
    key_region = digest(MD_SHA256, aws.region, key_date)
    key_service = digest(MD_SHA256, service, key_region)
    key_signing = digest(MD_SHA256, terminator, key_service)
    signature = digest(MD_SHA256, string_to_sign, key_signing)

    query["X-Amz-Signature"] = bytes2hex(signature)

    return string(protocol, "://", host, path, "?", URIs.escapeuri(query))
end

"""
    s3_sign_url([::AbstractAWSConfig], bucket, path, [seconds=3600];
                [verb="GET"], [content_type="application/octet-stream"],
                [protocol="http"], [signature_version="v4"])

Create a [pre-signed url](http://docs.aws.amazon.com/AmazonS3/latest/dev/ShareObjectPreSignedURL.html) for `bucket` and `path` (expires after for `seconds`).

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

# Permissions

- [`s3:GetObject`](https://docs.aws.amazon.com/service-authorization/latest/reference/list_amazons3.html#amazons3-GetObject)
  (conditional): required permission when `verb="GET"`.
- [`s3:PutObject`](https://docs.aws.amazon.com/service-authorization/latest/reference/list_amazons3.html#amazons3-PutObject)
  (conditional): required permission when `verb="PUT"`.
"""
function s3_sign_url(
    aws::AbstractAWSConfig,
    bucket,
    path,
    seconds=3600;
    verb="GET",
    content_type="application/octet-stream",
    protocol="http",
    signature_version="v4",
)
    if signature_version == "v2"
        _s3_sign_url_v2(aws, bucket, path, seconds; verb, content_type, protocol)
    elseif signature_version == "v4"
        _s3_sign_url_v4(aws, bucket, path, seconds; verb, content_type, protocol)
    else
        throw(ArgumentError("Unknown signature version $signature_version"))
    end
end

s3_sign_url(a...; b...) = s3_sign_url(global_aws_config(), a...; b...)

"""
    s3_nuke_bucket([::AbstractAWSConfig], bucket_name)

Deletes a bucket including all of the object versions in that bucket. Users should not call
this function unless they are certain they want to permanently delete all of the data that
resides within this bucket.

The `s3_nuke_bucket` is purposefully *not* exported as a safe guard against accidental
usage.

!!! warning

    Permanent data loss will occur when using this function. Do not use this function unless
    you understand the risks. By using this function you accept all responsibility around
    any repercussions with the loss of this data.

# API Calls

- [`ListObjectVersions`](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectVersions.html)
- [`DeleteObject`](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObject.html)
- [`DeleteBucket`](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucket.html)

# Permissions

- [`s3:ListBucketVersions`](https://docs.aws.amazon.com/service-authorization/latest/reference/list_amazons3.html#amazons3-ListBucketVersions)
- [`s3:DeleteObjectVersion`](https://docs.aws.amazon.com/service-authorization/latest/reference/list_amazons3.html#amazons3-DeleteObjectVersion):
  required even on buckets that do not have versioning enabled.
- [`s3:DeleteBucket`](https://docs.aws.amazon.com/service-authorization/latest/reference/list_amazons3.html#amazons3-DeleteBucket)
"""
function s3_nuke_bucket(aws::AbstractAWSConfig, bucket_name)
    for v in s3_list_versions(aws, bucket_name)
        s3_delete(aws, bucket_name, v["Key"]; version=v["VersionId"])
    end

    return s3_delete_bucket(aws, bucket_name)
end

include("s3path.jl")

end #module AWSS3

#==============================================================================#
# End of file.
#==============================================================================#
