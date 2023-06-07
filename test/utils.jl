const BUCKET_DATE_FORMAT = dateformat"yyyymmdd\THHMMSS\Z"

is_aws(config) = config isa AWSConfig
AWS.aws_account_number(::Minio.MinioConfig) = "123"

function gen_bucket_name(prefix="awss3.jl.test.")
    # # https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html
    return lowercase(prefix * Dates.format(now(Dates.UTC), BUCKET_DATE_FORMAT))
end

function assume_role(
    role; aws_config::AbstractAWSConfig, duration=nothing, mfa_serial=nothing,
)
    if startswith(role, "arn:aws:iam")
        role_arn = role
        role_name = basename(role)
    else
        response = AWSServices.sts(
            "GetCallerIdentity";
            aws_config,
            feature_set=AWS.FeatureSet(; use_response_type=true),
        )
        account_id = parse(response)["GetCallerIdentityResult"]["Account"]
        role_name = role
        role_arn = "arn:aws:iam::$account_id:role/$role_name"
    end

    role_session = AWS._role_session_name(
        "AWS.jl-role-",
        role_name,
        "-" * Dates.format(now(UTC), dateformat"yyyymmdd\THHMMSS\Z"),
    )
    params = Dict{String,Any}("RoleArn" => role_arn, "RoleSessionName" => role_session)
    if duration !== nothing
        params["DurationSeconds"] = duration
    end
    if mfa_serial !== nothing
        params["SerialNumber"] = mfa_serial
        token = Base.getpass("Enter MFA code for $mfa_serial")
        params["TokenCode"] = read(token, String)
        Base.shred!(token)
    end

    response = AWSServices.sts(
        "AssumeRole",
        params;
        aws_config,
        feature_set=AWS.FeatureSet(; use_response_type=true),
    )
    dict = parse(response)
    role_creds = dict["AssumeRoleResult"]["Credentials"]
    role_user = dict["AssumeRoleResult"]["AssumedRoleUser"]
    return AWSConfig(;
        creds=AWSCredentials(
            role_creds["AccessKeyId"],
            role_creds["SecretAccessKey"],
            role_creds["SessionToken"],
            role_user["Arn"];
            expiry=DateTime(rstrip(role_creds["Expiration"], 'Z')),
            renew=() -> assume_role(config, role_arn; duration, mfa_serial).credentials,
        ),
    )
end

function assume_testset_role(role_suffix; base_config)
    return assume_role("AWSS3.jl-$role_suffix"; aws_config=base_config)
end

function with_aws_config(f, config::AbstractAWSConfig)
    local result
    old_config = global_aws_config()
    global_aws_config(config)
    try
        result = f()
    finally
        global_aws_config(old_config)
    end
    return result
end
