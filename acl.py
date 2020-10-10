import boto3
import os
import shortuuid

bucket_name = os.getenv("LIVE_S3_BUCKET")
print("len(bucket_name):", len(bucket_name))
access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
print("len(access_key_id):", len(access_key_id))
secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
print("len(secret_access_key):", len(secret_access_key))


# Retrieve a bucket's ACL
session = boto3.Session(
    aws_access_key_id=access_key_id,
    aws_secret_access_key=secret_access_key,
)
# boto3.DEFAULT_SESSION = boto3.Session(
#     aws_access_key_id=access_key_id,
#     aws_secret_access_key=secret_access_key,
# )
s3 = session.resource("s3")
bucket = s3.Bucket(bucket_name)

new_file = f"README-{shortuuid.uuid()}.txt"
print("New file:", new_file)
bucket.upload_file("README.md", new_file)

[print(o) for o in bucket.objects.all()]


bucket.objects.filter(Prefix="README").delete()
print("Deleting")
[print(o) for o in bucket.objects.all()]
