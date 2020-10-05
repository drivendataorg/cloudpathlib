# Authentication

For standard use, we recommend using environment variables to authenticate with the cloud storage services. This way, `cloudpathlib` will be able to automatically read those credentials and authenticate without you needing to do anything else. Passing credentials via environment variables is also generally a security best practice for avoiding accidental sharing.

`cloudpathlib` supports the standard environment variables used by each respective cloud service SDK.

Cloud              | Environment Variables | SDK Documentation |
------------------ | --------------------- | ------------------|
Amazon S3          | `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` <br /> _or_ <br /> `AWS_PROFILE` with credentials file | [Link](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html#environment-variables) |
Azure Blob Storage | `AZURE_STORAGE_CONNECTION_STRING` | [Link](https://docs.microsoft.com/en-us/azure/storage/blobs/storage-quickstart-blobs-python#copy-your-credentials-from-the-azure-portal) |

## Advanced Use

The communication between `cloudpathlib` and cloud storage services are handled by `Client` objects. Each cloud storage service has its own `Client` class implementation. See the linked API documentation pages for additional authentication options.

Cloud              | Client                | API Documentation |
------------------ | --------------------- | ----------------- |
Amazon S3          | `S3Client`            | [Link](../api-reference/s3client/) |
Azure Blob Storage | `AzureBlobClient`     | [Link](../api-reference/azblobclient/) |

A client object holds the authenticated connection with a cloud service, as well as the configuration for the [local cache](../caching/). When you create instantiate a cloud path instance for the first time, a default client object is created for the respective cloud service.

```python
from cloudpathlib import CloudPath

cloud_path = CloudPath("s3://cloudpathlib-test-bucket/")   # same for S3Path(...)
cloud_path.client
#> <cloudpathlib.s3.s3client.S3Client at 0x7feac3d1fb90>
```

All subsequent instances of that service's cloud paths (in the example, all subsequent `S3Path` instances) will reference the same client instance.

You can also explicitly instantiate a client instance. You will need to do so if you want to authenticate using any option other than the environment variables from the table in the previous section. (To see what those options are, check out the API documentation pages linked to in the table above.) You can then use that client instance's cloud path factory method, or pass it into a cloud path instantiation

```python
from cloudpathlib import S3Client

client = S3Client(aws_access_key_id="myaccesskey", aws_secret_access_key="mysecretkey")

# these next two commands are equivalent
# use client's factory method
cp1 = client.CloudPath("s3://cloudpathlib-test-bucket/")
# or pass client as keyword argument
cp2 = CloudPath("s3://cloudpathlib-test-bucket/", client=client)
```

If you have instantiated a client instance explicitly, you can also set it as the default client. Then, future cloud paths without a client specified will use that client instance.

```python
client = S3Client(aws_access_key_id="myaccesskey", aws_secret_access_key="mysecretkey")
client.set_as_default_client()
```

If you need a reference to the default client:

```python
S3Client.get_default_client()
#> <cloudpathlib.s3.s3client.S3Client at 0x7feac3d1fb90>
```
