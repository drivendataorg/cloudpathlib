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

# Other `Client` settings

## Content type guessing (`content_type_method`)

All of the clients support passing a `content_type_method` when they are instantiated.  
This is a method that is used to guess the [MIME (media) type](https://en.wikipedia.org/wiki/Media_type)
(often called the "content type") of the file and set that on the cloud provider.

By default, `content_type_method` use the Python built-in 
[`guess_type`](https://docs.python.org/3/library/mimetypes.html#mimetypes.guess_type)
to set this content type. This guesses based on the file extension, and may not always get the correct type.
In these cases, you can set `content_type_method` to your own function that gets the proper type; for example, by 
reading the file content or by looking it up in a dictionary of filename-to-media-type mappings that you maintain.

If you set a custom method, it should follow the signature of `guess_type` and return a tuple of the form: 
`(content_type, content_encoding)`; for example, `("text/css", None)`.

If you set `content_type_method` to None, it will do whatever the default of the cloud provider's SDK does. This
varies from provider to provider.

Here is an example of using a custom `content_type_method`.

```python
import mimetypes
from pathlib import Path

from cloudpathlib import S3Client, CloudPath

def my_content_type(path):
    # do lookup for content types I define; fallback to
    # guess_type for anything else
    return {
        ".potato": ("application/potato", None),
    }.get(Path(path).suffix, mimetypes.guess_type(path))


# create a client with my custom content type
client = S3Client(content_type_method=my_content_type)

# To use this same method for every cloud path, set our client as the default.
# This is optional, and you could use client.CloudPath to create paths instead.
client.set_as_default_client()

# create a cloud path
cp1 = CloudPath("s3://cloudpathlib-test-bucket/i_am_a.potato")
cp1.write_text("hello")

# check content type with boto3
print(client.s3.Object(cp1.bucket, cp1.key).content_type)
#> application/potato
```
