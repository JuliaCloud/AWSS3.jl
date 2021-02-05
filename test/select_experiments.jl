using AWSS3, Minio

s = Minio.Server(@__DIR__)

csv = S3Path("s3://testbucket/test.csv", config=MinioConfig(s))

test(csv) = s3select(csv, "select * from s3object s where s.C >= 4")

