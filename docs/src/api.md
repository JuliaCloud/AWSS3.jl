```@meta
CurrentModule = AWSS3
```

## S3 Interaction
```@docs
s3_arn
s3_get
s3_get_file
s3_get_meta
s3_exists
s3_delete
s3_copy
s3_create_bucket
s3_put_cors
s3_enable_versioning
s3_put_tags
s3_get_tags
s3_delete_tags
s3_delete_bucket
s3_list_buckets
s3_list_objects
s3_list_keys
s3_purge_versions
s3_put
s3_sign_url
s3_nuke_bucket
```

## `S3Path`
Note that `S3Path` implements the `AbstractPath` interface, some the FilePathsBase documentation for
the interface [here](https://rofinn.github.io/FilePathsBase.jl/stable/api/).
```@docs
S3Path
stat
mkdir
read
get_config
```


## Internal
```@docs
_s3_exists_dir
s3_exists_versioned
s3_exists_unversioned
```
