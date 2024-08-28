# Authentication

For standard use, we recommend using environment variables to authenticate with the cloud storage services. This way, `cloudpathlib` will be able to automatically read those credentials and authenticate without you needing to do anything else. Passing credentials via environment variables is also generally a security best practice for avoiding accidental sharing.

`cloudpathlib` supports the standard environment variables used by each respective cloud service SDK.

Cloud                | Environment Variables | SDK Documentation |
-------------------- | --------------------- | ------------------|
Amazon S3            | `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` <br /> _or_ <br /> `AWS_PROFILE` with credentials file | [Link](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html#environment-variables) |
Azure Blob Storage   | `AZURE_STORAGE_CONNECTION_STRING` | [Link](https://docs.microsoft.com/en-us/azure/storage/blobs/storage-quickstart-blobs-python#copy-your-credentials-from-the-azure-portal) |
Google Cloud Storage | `GOOGLE_APPLICATION_CREDENTIALS` | [Link](https://cloud.google.com/docs/authentication/production#passing_variable) |
 
## Advanced Use

The communication between `cloudpathlib` and cloud storage services are handled by `Client` objects. Each cloud storage service has its own `Client` class implementation. See the linked API documentation pages for additional authentication options.

Cloud                | Client                | API Documentation |
-------------------- | --------------------- | ----------------- |
Amazon S3            | `S3Client`            | [Link](../api-reference/s3client/) |
Azure Blob Storage   | `AzureBlobClient`     | [Link](../api-reference/azblobclient/) |
Google Cloud Storage | `GSClient`            | [Link](../api-reference/gsclient/) |

A client object holds the authenticated connection with a cloud service, as well as the configuration for the [local cache](../caching/). When you create instantiate a cloud path instance for the first time, a default client object is created for the respective cloud service.

```python
from cloudpathlib import CloudPath

cloud_path = CloudPath("s3://cloudpathlib-test-bucket/")   # same for S3Path(...)
cloud_path.client
#> <cloudpathlib.s3.s3client.S3Client at 0x7feac3d1fb90>
```

All subsequent instances of that service's cloud paths (in the example, all subsequent `S3Path` instances) will reference the same client instance.

You can also explicitly instantiate a client instance. You will need to do so if you want to authenticate using any option other than the environment variables from the table in the previous section. (To see what those options are, check out the API documentation pages linked to in the table above.) You can then use that client instance's cloud path factory method, or pass it into a cloud path instantiation.

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


## Accessing public S3 buckets without credentials

For most operations, you will need to have your S3 credentials configured. However, for buckets that provide public access, you can use `cloudpathlib` without credentials. To do so, you need to instantiate a client and pass the kwarg `no_sign_request=True`. Failure to do so will result in a `NoCredentialsError` being thrown.

```python
from cloudpathlib import CloudPath

# this file deinitely exists, but credentials are not configured
CloudPath("s3://ladi/Images/FEMA_CAP/2020/70349/DSC_0001_5a63d42e-27c6-448a-84f1-bfc632125b8e.jpg").exists()

#> NoCredentialsError
```

Instead, you must either configure credentials or instantiate a client object using `no_sign_request=True`:

```python
from cloudpathlib import S3Client

c = S3Client(no_sign_request=True)

# use this client object to create the CloudPath
c.CloudPath("s3://ladi/Images/FEMA_CAP/2020/70349/DSC_0001_5a63d42e-27c6-448a-84f1-bfc632125b8e.jpg").exists()
#> True
```

**Note:** Many public buckets _do not_ allow listing of the bucket contents by anonymous users. If this is the case, any listing operation on a directory will fail with an error like `ClientError: An error occurred (AccessDenied) when calling the ListObjectsV2 operation: Access Denied` when you try to do an operation, even with `no_sign_request=True`. In this case, you can generally only work with `CloudPath` objects that refer to the files themselves (instead of directories). You can contact the bucket owner to request that they allow listing, or write your code in a way that only references files you know will exist.

As noted above, you can also call `.set_as_default_client()` on the client object that you create and then it will be used by default without your having to explicitly use the client object that you created.


## Requester Pays buckets on S3

S3 supports [Requester Pays](https://docs.aws.amazon.com/AmazonS3/latest/userguide/RequesterPaysBuckets.html) buckets where you must have credentials to access the bucket and any costs are passed on to you rather than the owner of the bucket.

For a requester pays bucket, you need to pass extras telling cloudpathlib you will pay for any operations.

For example, on the requester pays bucket `arxiv`, just trying to list the contents will result in a `ClientError`:

```python
from cloudpathlib import CloudPath

tars = list(CloudPath("s3://arxiv/src/").iterdir())
print(tars)

#> ClientError: An error occurred (AccessDenied) ...
```

To indicate that the request payer will be the "requester," pass the extra args to an `S3Client` and use that client to instantiate paths:

```python
from cloudpathlib import S3Client

c = S3Client(extra_args={"RequestPayer": "requester"})

# use the client we created to build the path
tars = list(c.CloudPath("s3://arxiv/src/").iterdir())
print(tars)
```

As noted above, you can also call `.set_as_default_client()` on the client object that you create and then it will be used by default without your having to explicitly use the client object that you created.


## Other S3 `ExtraArgs` in `boto3`

The S3 SDK, `boto3` supports a set of `ExtraArgs` for uploads, downloads, and listing operations. When you instatiate a client, you can pass the `extra_args` keyword argument with any of those extra args that you want to set. We will pass these on to the upload, download, and list methods insofar as those methods support the specific args.

The args supported for uploads are the same as `boto3.s3.transfer.S3Transfer.ALLOWED_UPLOAD_ARGS`, see the [`boto3` documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/customizations/s3.html#boto3.s3.transfer.S3Transfer) for the latest, but as of the time of writing, these are:

 - `ACL`
 - `CacheControl`
 - `ChecksumAlgorithm`
 - `ContentDisposition`
 - `ContentEncoding`
 - `ContentLanguage`
 - `ContentType`
 - `ExpectedBucketOwner`
 - `Expires`
 - `GrantFullControl`
 - `GrantRead`
 - `GrantReadACP`
 - `GrantWriteACP`
 - `Metadata`
 - `ObjectLockLegalHoldStatus`
 - `ObjectLockMode`
 - `ObjectLockRetainUntilDate`
 - `RequestPayer`
 - `ServerSideEncryption`
 - `StorageClass`
 - `SSECustomerAlgorithm`
 - `SSECustomerKey`
 - `SSECustomerKeyMD5`
 - `SSEKMSKeyId`
 - `SSEKMSEncryptionContext`
 - `Tagging`
 - `WebsiteRedirectLocation`

The args supported for downloads are the same as `boto3.s3.transfer.S3Transfer.ALLOWED_DOWNLOAD_ARGS`, see the [`boto3` documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/customizations/s3.html#boto3.s3.transfer.S3Transfer) for the latest, but as of the time of writing, these are:

 - `ChecksumMode`
 - `VersionId`
 - `SSECustomerAlgorithm`
 - `SSECustomerKey`
 - `SSECustomerKeyMD5`
 - `RequestPayer`
 - `ExpectedBucketOwner`

To use any of these extra args, pass them as a dict to `extra_args` when instantiating and `S3Client`.

```python
from cloudpathlib import S3Client

c = S3Client(extra_args={
    "ChecksumMode": "ENABLED",  # download extra arg, only used when downloading
    "ACL": "public-read",       # upload extra arg, only used when uploading
})

# use these extras for all CloudPaths
c.set_as_default_client()
```

**Note:** The `extra_args` kwargs accepts the union of upload and download args, and will only pass on the relevant subset to the `boto3` method that is called by the internals of `S3Client`.

**Note:** The ExtraArgs on the client will be used for every call that client makes. If you need to set different `ExtraArgs` in different code paths, we recommend creating separate explicit client objects and using those to create and manage the CloudPath objects with different needs.

**Note:** To explicitly set the `ContentType` and `ContentEncoding`, we recommend using the `content_type_method` kwarg when instantiating the client. If instead you want to set this for all uploads via the extras, you must additionally pass `content_type_method=None` to the `S3Client` so we don't try to guess these automatically.


## Accessing custom S3-compatible object stores
It might happen so that you need to access a customly deployed S3 object store ([MinIO](https://min.io/), [Ceph](https://ceph.io/ceph-storage/object-storage/) or any other).
In such cases, the service endpoint will be different from the AWS object store endpoints (used by default).
To specify a custom endpoint address, you will need to manually instantiate `Client` with the `endpoint_url` parameter,
provinding http/https URL including port.

```python
from cloudpathlib import S3Client, CloudPath

# create a client pointing to the endpoint
client = S3Client(endpoint_url="http://my.s3.server:1234")

# option 1: use the client to create paths
cp1 = client.CloudPath("s3://cloudpathlib-test-bucket/")

# option 2: pass the client as keyword argument
cp2 = CloudPath("s3://cloudpathlib-test-bucket/", client=client)

# option3: set this client as the default so it is used in any future paths
client.set_as_default_client()
cp3 = CloudPath("s3://cloudpathlib-test-bucket/")
```

## Accessing Azure DataLake Storage Gen2 (ADLS Gen2) storage with hierarchical namespace enabled

Some Azure storage accounts are configured with "hierarchical namespace" enabled. This means that the storage account is backed by the Azure DataLake Storage Gen2 product rather than Azure Blob Storage. For many operations, the two are the same and one can use the Azure Blob Storage API. However, for some operations, a developer will need to use the Azure DataLake Storage API. The `AzureBlobClient` class implemented in cloudpathlib is designed to detect if hierarchical namespace is enabled and use the Azure DataLake Storage API in the places where it is necessary or it provides a performance improvement. Usually, a user of cloudpathlib will not need to know if hierarchical namespace is enabled and the storage account is backed by Azure DataLake Storage Gen2 or Azure Blob Storage.

If needed, the Azure SDK provided `DataLakeServiceClient` object can be accessed via the `AzureBlobClient.data_lake_client`. The Azure SDK provided `BlobServiceClient` object can be accessed via `AzureBlobClient.service_client`.


## Pickling `CloudPath` objects

You can pickle and unpickle `CloudPath` objects normally, for example:

```python
from pathlib import Path
import pickle

from cloudpathlib import CloudPath


with Path("cloud_path.pkl").open("wb") as f:
    pickle.dump(CloudPath("s3://my-awesome-bucket/cool-file.txt"), f)

with Path("cloud_path.pkl").open("rb") as f:
    pickled = pickle.load(f)

assert pickled.bucket == "my-awesome-bucket"
```

The associated `client`, however, is not pickled. When a `CloudPath` is 
unpickled, the client on the unpickled object will be set to the default 
client for that class.

For example, this **will not work**:

```python
from pathlib import Path
import pickle

from cloudpathlib import S3Client, CloudPath


# create a custom client pointing to the endpoint
client = S3Client(endpoint_url="http://my.s3.server:1234")

# use that client when creating a cloud path
p = CloudPath("s3://cloudpathlib-test-bucket/cool_file.txt", client=client)
p.write_text("hello!")

with Path("cloud_path.pkl").open("wb") as f:
    pickle.dump(p, f)

with Path("cloud_path.pkl").open("rb") as f:
    pickled = pickle.load(f)

# this will be False, because it will use the default `S3Client`
assert pickled.exists() == False
```

To get this to work, you need to set the custom `client` to the default
before unpickling:

```python
# set the custom client as the default before unpickling
client.set_as_default_client()

with ("cloud_path.pkl").open("rb") as f:
    pickled2 = pickle.load(f)

assert pickled2.exists()
assert pickled2.client == client
```
