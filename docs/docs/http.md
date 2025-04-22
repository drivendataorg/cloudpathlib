# HTTP Support in CloudPathLib

We support `http://` and `https://` URLs with `CloudPath`, but these behave somewhat differently from typical cloud provider URIs (e.g., `s3://`, `gs://`) or local file paths. This document describes those differences, caveats, and the additional configuration options available.

 > **Note:** We don't currently automatically detect `http` links to cloud storage providers (for example, `http://s3.amazonaws.com/bucket/key`) and treat those as `S3Path`, `GSPath`, etc. They will be treated as normal urls (i.e., `HttpPath` objects).

## Basic Usage

```python
from cloudpathlib import CloudPath

# Create a path object
path = CloudPath("https://example.com/data/file.txt")

# Read file contents
text = path.read_text()
binary = path.read_bytes()

# Get parent directory
parent = path.parent  # https://example.com/data/

# Join paths
subpath = path.parent / "other.txt"  # https://example.com/data/other.txt

# Check if file exists
if path.exists():
    print("File exists!")

# Get file name and suffix
print(path.name)      # "file.txt"
print(path.suffix)    # ".txt"

# List directory contents (if server supports directory listings)
data_dir = CloudPath("https://example.com/data/")
for child_path in data_dir.iterdir():
    print(child_path)
```

## How HTTP Paths Differ

 - HTTP servers are not necessarily structured like file systems. Operations such as listing directories, removing files, or creating folders depend on whether the server supports them.
 - For many operations (e.g., uploading, removing files), this implementation relies on specific HTTP verbs like `PUT` or `DELETE`. If the server does not allow these verbs, those operations will fail.
 - While some cloud storage backends (e.g., AWS S3) provide robust directory emulation, a basic HTTP server may only partially implement these concepts (e.g., listing a directory might just be an HTML page with links).
 - HTTP URLs often include more than just a path, for example query strings, fragments, and other URL modifiers that are not part of the path. These are handled differently than with other cloud storage providers.

## URL components

You can access the various components of a URL via the `HttpPath.parsed_url` property, which is a [`urllib.parse.ParseResult`](https://docs.python.org/3/library/urllib.parse.html#urllib.parse.urlparse) object.

For example for the following URL:

```
https://username:password@www.example.com:8080/path/to/resource?query=param#fragment
```

The components are:

```mermaid
flowchart LR

    %% Define colors for each block
    classDef scheme fill:#FFD700,stroke:#000,stroke-width:1px,color:#000
    classDef netloc fill:#ADD8E6,stroke:#000,stroke-width:1px,color:#000
    classDef path fill:#98FB98,stroke:#000,stroke-width:1px,color:#000
    classDef query fill:#EE82EE,stroke:#000,stroke-width:1px,color:#000
    classDef fragment fill:#FFB6C1,stroke:#000,stroke-width:1px,color:#000

    A[".scheme<br/><code>https</code>"]:::scheme
    B[".netloc<br/>username:password\@www‎.example.com:8080"]:::netloc
    C[".path<br/><code>/path/to/resource</code>"]:::path
    D[".query<br/><code>query=param</code>"]:::query
    E[".fragment<br/><code>fragment</code>"]:::fragment

    A --> B --> C --> D --> E
```

To access the components of the URL, you can use the `HttpPath.parsed_url` property:

```python
my_path = HttpPath("http://username:password@www.example.com:8080/path/to/resource?query=param#fragment")

print(my_path.parsed_url.scheme)  # "http"
print(my_path.parsed_url.netloc)  # "username:password@www.example.com:8080"
print(my_path.parsed_url.path)    # "/path/to/resource"
print(my_path.parsed_url.query)   # "query=param"
print(my_path.parsed_url.fragment)  # "fragment"

# extra properties that are subcomponents of `netloc`
print(my_path.parsed_url.username)  # "username"
print(my_path.parsed_url.password)  # "password"
print(my_path.parsed_url.hostname)  # "www.example.com"
print(my_path.parsed_url.port)      # "8080"
```

### Preservation and Joining Behavior

 - **Params, query, and fragment** are part of the URL, but be aware that when you perform operations that return a new path (e.g., joining `my_path / "subdir"`, walking directories, fetching parents, etc.), these modifiers will be discarded unless you explicitly preserve them, since we operate under the assumption that these modifiers are tied to the specific URL.
 - **netloc (including the subcomponents, username, password, hostname, port) and scheme** are preserved when joining. They are derived from the main portion of the URL (e.g., `http://username:password@www.example.com:8080`).

### The `HttpPath.anchor` Property

Because of naming conventions inherited from Python's `pathlib`, the "anchor" in a CloudPath (e.g., `my_path.anchor`) refers to `<scheme>://<netloc>/`. It does **not** include the "fragment" portion of a URL (which is sometimes also called the "anchor" in HTML contexts since it can refer to a `<a>` tag). In other words, `.anchor` returns something like `https://www.example.com/`, not `...#fragment`. To get the fragment, use `my_path.parsed_url.fragment`.

## Required serverside HTTP verbs support

Some operations require that the server support specific HTTP verbs. If your server does not support these verbs, the operation will fail.

Your server needs to support these operations for them to succeed:

 - If your server does not allow `DELETE`, you will not be able to remove files via `HttpPath.unlink()` or `HttpPath.remove()`.
 - If your server does not allow `PUT` (or `POST`, see next bullet), you won't be able to upload files.
 - By default, we use `PUT` for creating or replacing a file. If you need `POST` for uploads, you can override the behavior by passing `write_file_http_method="POST"` to the `HttpClient` constructor.

### Making requests with the `HttpPath` object

`HttpPath` and `HttpsPath` expose direct methods to perform the relevant HTTP verbs:

```python
response, content = my_path.get()     # issues a GET
response, content = my_path.put()     # issues a PUT
response, content = my_path.post()    # issues a POST
response, content = my_path.delete()  # issues a DELETE
response, content = my_path.head()    # issues a HEAD
```

These methods are thin wrappers around the client's underlying `request(...)` method, so you can pass any arguments that [`urllib.request.Request`](https://docs.python.org/3/library/urllib.request.html#urllib.request.Request) supports, so you can pass content via `data=` and headers via `headers=`.

## Authentication

By default, `HttpClient` will build a simple opener with `urllib.request.build_opener()`, which typically handles no or basic system-wide HTTP auth. However, you can pass an implementation of `urllib.request.BaseHandler` (e.g., an `HTTPBasicAuthHandler`) to the `HttpClient` of `HttpsClient` constructors to handle authentication:

```python
import urllib.request

auth_handler = urllib.request.HTTPBasicAuthHandler()
auth_handler.add_password(
    realm="Some Realm",
    uri="http://www.example.com",
    user="username",
    passwd="password"
)

client = HttpClient(auth=auth_handler)
my_path = client.CloudPath("http://www.example.com/secret/data.txt")

# Now GET requests will include basic auth headers
content = my_path.read_text()
```

This can be extended to more sophisticated authentication approaches (e.g., OAuth, custom headers) by providing your own `BaseHandler` implementation. There are examples on the internet of handlers for most common authentication schemes.

## Directory Assumptions

Directories are handled differently from other `CloudPath` implementations:

 - By default, a URL is considered a directory if it **ends with a slash**. For example, `http://example.com/somedir/`.
 - If you call `HttpPath.is_dir()`, it checks `my_url.endswith("/")` by default. You can override this with a custom function by passing `custom_dir_matcher` to `HttpClient`. This will allow you to implement custom logic for determining if a URL is a directory. The `custom_dir_matcher` will receive a string representing the URL, so if you need to interact with the server, you will need to make those requests within your `custom_dir_matcher` implementation.

### Listing the Contents of a Directory

We attempt to parse directory listings by calling `GET` on the directory URL (which presumably returns an HTML page that has a directory index). Our default parser looks for `<a>` tags and yields them, assuming they are children. You can override this logic with `custom_list_page_parser` if your server's HTML or API returns a different listing format. For example:

```python
def my_parser(html_content: str) -> Iterable[str]:
    # for example, just get a with href and class "file-link"
    # using beautifulsoup
    soup = BeautifulSoup(html_content, "html.parser")
    for link in soup.find_all("a", class_="file-link"):
        yield link.get("href")

client = HttpClient(custom_list_page_parser=my_parser)
my_dir = client.CloudPath("http://example.com/public/")

for subpath, is_dir in my_dir.list_dir(recursive=False):
    print(subpath, "dir" if is_dir else "file")
```

**Note**: If your server doesn't provide an HTML index or a suitable listing format that we can parse, you will see:

```
NotImplementedError("Unable to parse response as a listing of files; please provide a custom parser as `custom_list_page_parser`.")
```

In that case, you must provide a custom parser or avoid directory-listing operations altogether.

## HTTP or HTTPS

There are separate classes for `HttpClient`/`HttpPath` for `http://` and `HttpsClient`/`HttpsPath` for `https://`. However, from a usage standpoint, you can use either `CloudPath` or `AnyPath` to dispatch to the right subclass.

```python
from cloudpathlib import AnyPath, CloudPath

# AnyPath will automatically detect "http://" or "https://" (or local file paths)
my_path = AnyPath("https://www.example.com/files/info.txt")

# CloudPath will dispatch to the correct subclass
my_path = CloudPath("https://www.example.com/files/info.txt")
```

If you explicitly instantiate a `HttpClient`, it will only handle `http://` paths. If you instantiate a `HttpsClient`, it will only handle `https://` paths. But `AnyPath` and `CloudPath` will route to the correct client class automatically.

In general, you should use `HttpsClient` and work with `https://` urls wherever possible.

## Additional Notes

 - **Caching**: This implementation uses the same local file caching mechanics as other CloudPathLib providers, controlled by `file_cache_mode` and `local_cache_dir`. However, for static HTTP servers, re-downloading or re-checking may not be as efficient as with typical cloud storages that return robust metadata.  
 - **"Move" or "Rename"**: The `_move_file` operation is implemented as an upload followed by a delete. This will fail if your server does not allow both `PUT` and `DELETE`.

