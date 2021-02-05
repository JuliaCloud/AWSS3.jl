
abstract type S3SerializationStruct end

function _key_string(field::Symbol)
    strs = split(string(field), '_')
    join(uppercasefirst.(strs), "")
end

s3_serialization_dict(kwargs) = Dict(_key_string(k)=>v for (k,v) ∈ kwargs)

Base.Dict(s::S3SerializationStruct) = s.dict


"""
    S3InputSerialization

see AWS API docs: https://docs.aws.amazon.com/AmazonS3/latest/API/API_InputSerialization.html
"""
module S3InputSerialization
    using AWSS3: S3SerializationStruct, s3_serialization_dict

    """
        S3InputSerialization.CSV

    see AWS API docs: https://docs.aws.amazon.com/AmazonS3/latest/API/API_CSVInput.html
    """
    struct CSV <: S3SerializationStruct
        dict::Dict{String,Any}
    end
    CSV(;kwargs...) = s3_serialization_dict(kwargs)


    """
        S3InputSerialization.JSON

    see AWS API docs: https://docs.aws.amazon.com/AmazonS3/latest/API/API_JSONInput.html
    """
    struct JSON <: S3SerializationStruct
        dict::Dict{String,Any}
    end
    JSON(;kwargs...) = JSON(s3_serialization_dict(kwargs))

    """
        S3InputSerialization.Parquet

    see AWS API docs: https://docs.aws.amazon.com/AmazonS3/latest/API/API_ParquetInput.html
    """
    struct Parquet <: S3SerializationStruct
        dict::Dict{String,Any}
    end
    Parquet(;kwargs...) = Parquet(s3_serialization_dict(kwargs))
end

"""
    S3OutputSerialization

see AWS API docs: https://docs.aws.amazon.com/AmazonS3/latest/API/API_OutputSerialization.html
"""
module S3OutputSerialization
    using AWSS3: S3SerializationStruct, s3_serialization_dict

    """
        S3OutputSerialization.CSV

    see AWS API docs: https://docs.aws.amazon.com/AmazonS3/latest/API/API_CSVOutput.html
    """
    struct CSV <: S3SerializationStruct
        dict::Dict{String,Any}
    end
    CSV(;kwargs...) = CSV(s3_serialization_dict(kwargs))

    """
        S3OutputSerialization.JSON

    see AWS API docs: https://docs.aws.amazon.com/AmazonS3/latest/API/API_JSONOutput.html
    """
    struct JSON <: S3SerializationStruct
        dict::Dict{String,Any}
    end
    JSON(;kwargs...) = JSON(s3_serialization_dict(kwargs))
end

# for now this just looks at the key
function _infer_input_serialization(aws::AbstractAWSConfig, bucket, key)
    if endswith(key, "csv")
        S3InputSerialization.CSV()
    elseif endswith(key, "json")
        S3InputSerialization.JSON()
    else
        S3InputSerialization.Parquet()
    end
end

function s3_select_object_content(aws::AbstractAWSConfig, bucket, key, expression,
                                  args::AbstractDict{String,<:Any}=Dict{String,Any}();
                                  expression_type::AbstractString="SQL",
                                  input_serialization::Union{Nothing,S3SerializationStruct}=nothing,
                                  output_serialization::S3SerializationStruct=S3OutputSerialization.CSV())
    input_serialization ≡ nothing && (input_serialization = _infer_input_serialization(aws, bucket, key))
    S3.select_object_content(bucket, expression, expression_type, Dict(input_serialization), key,
                             Dict(output_serialization), args, aws_config=aws)
end
s3_select_object_content(a...; b...) = s3_select_object_content(global_aws_config(), a...; b...)
