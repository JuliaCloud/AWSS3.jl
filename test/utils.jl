using Test: @test, @test_throws, contains_warn

is_aws(config) = config isa AWSConfig
AWS.aws_account_number(::Minio.MinioConfig) = "123"

function minio_server(body, dirs=[mktempdir()]; address="localhost:9005")
    server = Minio.Server(dirs; address)

    try
        run(server; wait=false)
        sleep(0.5)  # give the server just a bit of time, though it is amazingly fast to start

        config = MinioConfig(
            "http://$address"; username="minioadmin", password="minioadmin"
        )
        body(config)
    finally
        # Make sure we kill the server even if a test failed.
        kill(server)
    end
end

function gen_bucket_name(prefix="awss3.jl.test.")
    # https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html
    return lowercase(string(prefix, uuid4()))
end

function assume_role(aws_config::AbstractAWSConfig, role; duration=nothing)
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

    role_session = AWS._role_session_name("AWS.jl-role-", role_name, string("-", uuid4()))
    params = Dict{String,Any}("RoleArn" => role_arn, "RoleSessionName" => role_session)
    if duration !== nothing
        params["DurationSeconds"] = duration
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
            renew=() -> assume_role(aws_config, role_arn; duration, mfa_serial).credentials,
        ),
    )
end

# TODO: We're ignoring assume role calls when using a `MinioConfig` as we don't yet support
# this.
function assume_role(config::MinioConfig, role; kwargs...)
    return config
end

function assume_testset_role(role_suffix; base_config)
    return assume_role(base_config, "AWSS3.jl-$role_suffix")
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

# Rudementary support for `@test_throws ["Try", "Complex"] sqrt(-1)` for Julia 1.6
macro test_throws_msg(extype, ex)
    # https://github.com/JuliaLang/julia/pull/41888
    expr = if VERSION >= v"1.8.0-DEV.363"
        :(@test_throws $extype $ex)
    else
        quote
            @test try
                $ex
                false
            catch e
                exc = sprint(showerror, e)
                contains_warn(exc, $extype)
            end
        end
    end
    return esc(expr)
end
