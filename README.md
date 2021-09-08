![](https://raw.githubusercontent.com/drivendataorg/cloudpathlib/master/docs/docs/logo.svg)

<h1></h1>

[![Docs Status](https://img.shields.io/badge/docs-stable-informational)](https://cloudpathlib.drivendata.org/)
[![PyPI](https://img.shields.io/pypi/v/cloudpathlib.svg)](https://pypi.org/project/cloudpathlib/)
[![conda-forge](https://img.shields.io/conda/vn/conda-forge/cloudpathlib.svg)](https://anaconda.org/conda-forge/cloudpathlib)
[![tests](https://github.com/drivendataorg/cloudpathlib/workflows/tests/badge.svg?branch=master)](https://github.com/drivendataorg/cloudpathlib/actions?query=workflow%3Atests+branch%3Amaster)
[![codecov](https://codecov.io/gh/drivendataorg/cloudpathlib/branch/master/graph/badge.svg)](https://codecov.io/gh/drivendataorg/cloudpathlib)

> Our goal is to be the meringue of file management libraries: the subtle sweetness of `pathlib` working in harmony with the ethereal lightness of the cloud.

A Python library with classes that mimic `pathlib.Path`'s interface for URIs from different cloud storage services.

```python
with CloudPath("s3://bucket/filename.txt").open("w+") as f:
    f.write("Send my changes to the cloud!")
```

## Why use cloudpathlib?

 - **Familiar**: If you know how to interact with `Path`, you know how to interact with `CloudPath`. All of the cloud-relevant `Path` methods are implemented.
 - **Supported clouds**: AWS S3, Google Cloud Storage, and Azure Blob Storage are implemented. FTP is on the way.
 - **Extensible**: The base classes do most of the work generically, so implementing two small classes `MyPath` and `MyClient` is all you need to add support for a new cloud storage service.
 - **Read/write support**: Reading just works. Using the `write_text`, `write_bytes` or `.open('w')` methods will all upload your changes to cloud storage without any additional file management as a developer.
 - **Seamless caching**: Files are downloaded locally only when necessary. You can also easily pass a persistent cache folder so that across processes and sessions you only re-download what is necessary.
 - **Tested**: Comprehensive test suite and code coverage.
 - **Testability**: Local filesystem implementations that can be used to easily mock cloud storage in your unit tests.


## Installation

`cloudpathlib` depends on the cloud services' SDKs (e.g., `boto3`, `google-cloud-storage`, `azure-storage-blob`) to communicate with their respective storage service. If you try to use cloud paths for a cloud service for which you don't have dependencies installed, `cloudpathlib` will error and let you know what you need to install.

To install a cloud service's SDK dependency when installing `cloudpathlib`, you need to specify it using pip's ["extras"](https://packaging.python.org/tutorials/installing-packages/#installing-setuptools-extras) specification. For example:

```bash
pip install cloudpathlib[s3,gs,azure]
```

Currently supported cloud storage services are: `azure`, `gs`, `s3`. You can also use `all` to install all available services' dependencies.

If you do not specify any extras or separately install any cloud SDKs, you will only be able to develop with the base classes for rolling your own cloud path class.

### conda

`cloudpathlib` is also available using `conda` from conda-forge. Note that to install the necessary cloud service SDK dependency, you should include the appropriate suffix in the package name. For example:

```bash
conda install cloudpathlib-s3 -c conda-forge
```

If no suffix is used, only the base classes will be usable. See the [conda-forge/cloudpathlib-feedstock](https://github.com/conda-forge/cloudpathlib-feedstock) for all installation options.

### Development version

You can get latest development version from GitHub:

```bash
pip install https://github.com/drivendataorg/cloudpathlib.git#egg=cloudpathlib[all]
```

Note that you similarly need to specify cloud service dependencies, such as `all` in the above example command.

## Quick usage

Here's an example to get the gist of using the package. By default, `cloudpathlib` authenticates with the environment variables supported by each respective cloud service SDK. For more details and advanced authentication options, see the ["Authentication"](https://cloudpathlib.drivendata.org/stable/authentication/) documentation.

```python
from cloudpathlib import CloudPath

# dispatches to S3Path based on prefix
root_dir = CloudPath("s3://drivendata-public-assets/")
root_dir
#> S3Path('s3://drivendata-public-assets/')

# there's only one file, but globbing works in nested folder
for f in root_dir.glob('**/*.txt'):
    text_data = f.read_text()
    print(f)
    print(text_data)
#> s3://drivendata-public-assets/odsc-west-2019/DATA_DICTIONARY.txt
#> Eviction Lab Data Dictionary
#>
#> Additional information in our FAQ evictionlab.org/help-faq/
#> Full methodology evictionlab.org/methods/
#>
#> ... (additional text output truncated)

# use / to join paths (and, in this case, create a new file)
new_file_copy = root_dir / "nested_dir/copy_file.txt"
new_file_copy
#> S3Path('s3://drivendata-public-assets/nested_dir/copy_file.txt')

# show things work and the file does not exist yet
new_file_copy.exists()
#> False

# writing text data to the new file in the cloud
new_file_copy.write_text(text_data)
#> 6933

# file now listed
list(root_dir.glob('**/*.txt'))
#> [S3Path('s3://drivendata-public-assets/nested_dir/copy_file.txt'),
#>  S3Path('s3://drivendata-public-assets/odsc-west-2019/DATA_DICTIONARY.txt')]

# but, we can remove it
new_file_copy.unlink()

# no longer there
list(root_dir.glob('**/*.txt'))
#> [S3Path('s3://drivendata-public-assets/odsc-west-2019/DATA_DICTIONARY.txt')]
```

## Supported methods and properties

Most methods and properties from `pathlib.Path` are supported except for the ones that don't make sense in a cloud context. There are a few additional methods or properties that relate to specific cloud services or specifically for cloud paths.

| Methods + properties   | `AzureBlobPath`   | `S3Path`   | `GSPath`   |
|:-----------------------|:------------------|:-----------|:-----------|
| `anchor`               | ✅                | ✅         | ✅         |
| `as_uri`               | ✅                | ✅         | ✅         |
| `drive`                | ✅                | ✅         | ✅         |
| `exists`               | ✅                | ✅         | ✅         |
| `glob`                 | ✅                | ✅         | ✅         |
| `is_dir`               | ✅                | ✅         | ✅         |
| `is_file`              | ✅                | ✅         | ✅         |
| `iterdir`              | ✅                | ✅         | ✅         |
| `joinpath`             | ✅                | ✅         | ✅         |
| `match`                | ✅                | ✅         | ✅         |
| `mkdir`                | ✅                | ✅         | ✅         |
| `name`                 | ✅                | ✅         | ✅         |
| `open`                 | ✅                | ✅         | ✅         |
| `parent`               | ✅                | ✅         | ✅         |
| `parents`              | ✅                | ✅         | ✅         |
| `parts`                | ✅                | ✅         | ✅         |
| `read_bytes`           | ✅                | ✅         | ✅         |
| `read_text`            | ✅                | ✅         | ✅         |
| `rename`               | ✅                | ✅         | ✅         |
| `replace`              | ✅                | ✅         | ✅         |
| `rglob`                | ✅                | ✅         | ✅         |
| `rmdir`                | ✅                | ✅         | ✅         |
| `samefile`             | ✅                | ✅         | ✅         |
| `stat`                 | ✅                | ✅         | ✅         |
| `stem`                 | ✅                | ✅         | ✅         |
| `suffix`               | ✅                | ✅         | ✅         |
| `suffixes`             | ✅                | ✅         | ✅         |
| `touch`                | ✅                | ✅         | ✅         |
| `unlink`               | ✅                | ✅         | ✅         |
| `with_name`            | ✅                | ✅         | ✅         |
| `with_suffix`          | ✅                | ✅         | ✅         |
| `write_bytes`          | ✅                | ✅         | ✅         |
| `write_text`           | ✅                | ✅         | ✅         |
| `absolute`             | ❌                | ❌         | ❌         |
| `as_posix`             | ❌                | ❌         | ❌         |
| `chmod`                | ❌                | ❌         | ❌         |
| `cwd`                  | ❌                | ❌         | ❌         |
| `expanduser`           | ❌                | ❌         | ❌         |
| `group`                | ❌                | ❌         | ❌         |
| `home`                 | ❌                | ❌         | ❌         |
| `is_absolute`          | ❌                | ❌         | ❌         |
| `is_block_device`      | ❌                | ❌         | ❌         |
| `is_char_device`       | ❌                | ❌         | ❌         |
| `is_fifo`              | ❌                | ❌         | ❌         |
| `is_mount`             | ❌                | ❌         | ❌         |
| `is_relative_to`       | ❌                | ❌         | ❌         |
| `is_reserved`          | ❌                | ❌         | ❌         |
| `is_socket`            | ❌                | ❌         | ❌         |
| `is_symlink`           | ❌                | ❌         | ❌         |
| `lchmod`               | ❌                | ❌         | ❌         |
| `link_to`              | ❌                | ❌         | ❌         |
| `lstat`                | ❌                | ❌         | ❌         |
| `owner`                | ❌                | ❌         | ❌         |
| `readlink`             | ❌                | ❌         | ❌         |
| `relative_to`          | ❌                | ❌         | ❌         |
| `resolve`              | ❌                | ❌         | ❌         |
| `root`                 | ❌                | ❌         | ❌         |
| `symlink_to`           | ❌                | ❌         | ❌         |
| `with_stem`            | ❌                | ❌         | ❌         |
| `cloud_prefix`         | ✅                | ✅         | ✅         |
| `copy`                 | ✅                | ✅         | ✅         |
| `copytree`             | ✅                | ✅         | ✅         |
| `download_to`          | ✅                | ✅         | ✅         |
| `etag`                 | ✅                | ✅         | ✅         |
| `fspath`               | ✅                | ✅         | ✅         |
| `is_valid_cloudpath`   | ✅                | ✅         | ✅         |
| `rmtree`               | ✅                | ✅         | ✅         |
| `upload_from`          | ✅                | ✅         | ✅         |
| `blob`                 | ✅                | ❌         | ✅         |
| `bucket`               | ❌                | ✅         | ✅         |
| `container`            | ✅                | ❌         | ❌         |
| `key`                  | ❌                | ✅         | ❌         |
| `md5`                  | ✅                | ❌         | ❌         |

----

<sup>Icon made by <a href="https://www.flaticon.com/authors/srip" title="srip">srip</a> from <a href="https://www.flaticon.com/" title="Flaticon">www.flaticon.com</a>.</sup>
<br /><sup>Sample code block generated using the [reprexpy package](https://github.com/crew102/reprexpy).</sup>
