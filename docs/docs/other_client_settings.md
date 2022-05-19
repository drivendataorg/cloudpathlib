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
