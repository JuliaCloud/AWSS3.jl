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

[Get Object](http://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectGET.html)
from `path` in `bucket`.

# Optional Arguments
- `version=`: version of object to get.
- `retry=true`: try again on "NoSuchBucket", "NoSuchKey"
                (common if object was recently created).
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
   s3_get_meta([::AbstractAWSConfig], bucket, path; [version=], kwargs...)

[HEAD Object](http://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectHEAD.html)

Retrieves metadata from an object without returning the object itself.
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
    q = Dict("prefix" => path, "delimiter" => "", "max-keys" => 1)
    l = parse(S3.list_objects_v2(bucket, q; aws_config=aws))
    c = get(l, "Contents", nothing)
    c === nothing && return false
    return get(c, "Key", "") == path
end

"""
    _s3_exists_dir(aws::AbstractAWSConfig, bucket, path)

Internal, called by [`s3_exists`](@ref).

Checks whether the given directory exists.  This is a bit subtle because of how the
AWS API handles empty directories.  Empty directories are really just 0-byte nodes
which are named like directories, i.e. their name has a trailing `"/"`.

What this function does is, given a directory name `dir/`, check for all keys which
are lexographically greater than `dir.`.  The reason for this is that, if `dir/`
is a 0-byte node, checking for it directly will not reveal its existence due to
some rather peculiar design choices on the part of the S3 developers.

In all such cases, if the directory exists it will be seen in the *first* item
returned from `S3.list_objects_v2`: for empty directories this is because using
`start-after` explicitly excludes `dir.` itself and `dir/` is next; for directories
with actual keys, it is guaranteed that the first contained key will start with
the directory name.
"""
function _s3_exists_dir(aws::AbstractAWSConfig, bucket, path)
    a = chop(string(path)) * "."
    # note that you are not allowed to use *both* `prefix` and `start-after`
    q = Dict("delimiter" => "", "max-keys" => 1, "start-after" => a)
    l = parse(S3.list_objects_v2(bucket, q; aws_config=aws))
    c = get(l, "Contents", nothing)
    c === nothing && return false
    return startswith(get(c, "Key", ""), path)
end

"""
    s3_exists_versioned([::AbstractAWSConfig], bucket, path, version)

Check if the version specified by `version` of the object in bucket `bucket` exists at `path`.

Note that this function relies on error catching and may be less performant than [`s3_exists_unversioned `](@ref)
which is preferred.  The reason for this is that support for versioning in the AWS API is very limited.
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

See [`s3_exists_versioned`](@ref) to check for specific versions.
"""
function s3_exists_unversioned(aws::AbstractAWSConfig, bucket, path)
    f = endswith(path, '/') ? _s3_exists_dir : _s3_exists_file
    return f(aws, bucket, path)
end

"""
    s3_exists([::AbstractAWSConfig], bucket, path; version=nothing)

Returns a boolean whether an object exists at `path` in `bucket`.

Note that the AWS API's support for object versioning is quite limited and this
check will involve `try` `catch` logic if `version` is not `nothing`.
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
    s3_delete([::AbstractAWSConfig], bucket, path; [version=], kwargs...)

[DELETE Object](http://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectDELETE.html)
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
    s3_copy([::AbstractAWSConfig], bucket, path; to_bucket=bucket, to_path=path, kwargs...)

[PUT Object - Copy](http://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectCOPY.html)

# Optional Arguments
- `metadata::Dict=`; optional `x-amz-meta-` headers.
"""
function s3_copy(
    aws::AbstractAWSConfig,
    bucket,
    path;
    acl::AbstractString="",
    to_bucket=bucket,
    to_path=path,
    metadata::AbstractDict=SSDict(),
    kwargs...,
)
    headers = SSDict(
        "x-amz-metadata-directive" => "REPLACE",
        Pair["x-amz-meta-$k" => v for (k, v) in metadata]...,
    )

    if !isempty(acl)
        headers["x-amz-acl"] = acl
    end

    return parse(
        S3.copy_object(
            to_bucket,
            to_path,
            "$bucket/$path",
            Dict("headers" => headers);
            aws_config=aws,
            kwargs...,
        ),
    )
end

s3_copy(a...; b...) = s3_copy(global_aws_config(), a...; b...)

"""
    s3_create_bucket([:AbstractAWSConfig], bucket; kwargs...)

[PUT Bucket](http://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketPUT.html)
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
    s3_enable_versioning([::AbstractAWSConfig], bucket; kwargs...)

[Enable versioning for `bucket`](http://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketPUTVersioningStatus.html).
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
    s3_put_tags([::AbstractAWSConfig], bucket, [path,] tags::Dict; kwargs...)

PUT `tags` on
[`bucket`](http://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketPUTtagging.html)
or
[object (`path`)](http://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectPUTtagging.html).

See also `tags=` option on [`s3_put`](@ref).
"""
function s3_put_tags(aws::AbstractAWSConfig, bucket, tags::SSDict; kwargs...)
    return s3_put_tags(aws, bucket, "", tags; kwargs...)
end

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

Delete tags from
[`bucket`](http://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketDELETEtagging.html)
or
[object (`path`)](http://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectDELETEtagging.html).
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

[DELETE Bucket](http://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketDELETE.html).
"""
function s3_delete_bucket(aws::AbstractAWSConfig, bucket; kwargs...)
    return parse(S3.delete_bucket(bucket; aws_config=aws, kwargs...))
end
s3_delete_bucket(a; b...) = s3_delete_bucket(global_aws_config(), a; b...)

"""
    s3_list_buckets([::AbstractAWSConfig]; kwargs...)

[List of all buckets owned by the sender of the request](http://docs.aws.amazon.com/AmazonS3/latest/API/RESTServiceGET.html).
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
            q = Dict{String,String}()
            for (name, v) in [
                ("prefix", path_prefix),
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

[List object versions](http://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketGETVersion.html) in `bucket` with optional `path_prefix`.
"""
function s3_list_versions(aws::AbstractAWSConfig, bucket, path_prefix=""; kwargs...)
    more = true
    versions = []
    marker = ""

    while more
        query = Dict{String,Any}("versions" => "", "prefix" => path_prefix)

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
    s3_put([::AbstractAWSConfig], bucket, path, data, data_type="", encoding=""; <keyword arguments>)

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

    return parse(S3.put_object(bucket, path, args; aws_config=aws, kwargs...))
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

    return parse(response)
end

function s3_multipart_upload(
    aws::AbstractAWSConfig, bucket, path, io::IO, part_size_mb=50; kwargs...
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

    return s3_complete_multipart_upload(aws, upload, tags; kwargs...)
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
        _s3_sign_url_v2(
            aws,
            bucket,
            path,
            seconds;
            verb=verb,
            content_type=content_type,
            protocol=protocol,
        )
    elseif signature_version == "v4"
        _s3_sign_url_v4(
            aws,
            bucket,
            path,
            seconds;
            verb=verb,
            content_type=content_type,
            protocol=protocol,
        )
    else
        throw(ArgumentError("Unknown signature version $signature_version"))
    end
end

s3_sign_url(a...; b...) = s3_sign_url(global_aws_config(), a...; b...)

"""
    s3_nuke_bucket(bucket_name)

This function is NOT exported on purpose. AWS does not officially support this type of action,
although it is a very nice utility one this is not exported just as a safe measure against
accidentally blowing up your bucket.

!!! warning

    It will delete all versions of objects in the given bucket and then the bucket itself.
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
