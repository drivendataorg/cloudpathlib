![](docs/docs/logo.svg)

> Our goal is to be the meringue of file management libraries: the subtle sweetness of `pathlib` working in harmony with the ethereal lightness of the cloud.

A library that implements (nearly all) of the pathlib.Path methods for URIs for different cloud providers.

## Supported methods and properties

Most methods and properties from `pathlib.Path` are supported except for the ones that don't make sense in a cloud context. There are a few additional methods or properties that relate to specific cloud services or specifically for cloud paths.

| Methods + properties   | `AzureBlobPath`   | `S3Path`   |
|:-----------------------|:------------------|:-----------|
| `anchor`               | ✅                | ✅         |
| `as_uri`               | ✅                | ✅         |
| `drive`                | ✅                | ✅         |
| `exists`               | ✅                | ✅         |
| `glob`                 | ✅                | ✅         |
| `is_dir`               | ✅                | ✅         |
| `is_file`              | ✅                | ✅         |
| `iterdir`              | ✅                | ✅         |
| `joinpath`             | ✅                | ✅         |
| `match`                | ✅                | ✅         |
| `mkdir`                | ✅                | ✅         |
| `name`                 | ✅                | ✅         |
| `open`                 | ✅                | ✅         |
| `parent`               | ✅                | ✅         |
| `parents`              | ✅                | ✅         |
| `parts`                | ✅                | ✅         |
| `read_bytes`           | ✅                | ✅         |
| `read_text`            | ✅                | ✅         |
| `rename`               | ✅                | ✅         |
| `replace`              | ✅                | ✅         |
| `rglob`                | ✅                | ✅         |
| `rmdir`                | ✅                | ✅         |
| `samefile`             | ✅                | ✅         |
| `stat`                 | ✅                | ✅         |
| `stem`                 | ✅                | ✅         |
| `suffix`               | ✅                | ✅         |
| `suffixes`             | ✅                | ✅         |
| `touch`                | ✅                | ✅         |
| `unlink`               | ✅                | ✅         |
| `with_name`            | ✅                | ✅         |
| `with_suffix`          | ✅                | ✅         |
| `write_bytes`          | ✅                | ✅         |
| `write_text`           | ✅                | ✅         |
| `absolute`             | ❌                | ❌         |
| `as_posix`             | ❌                | ❌         |
| `chmod`                | ❌                | ❌         |
| `cwd`                  | ❌                | ❌         |
| `expanduser`           | ❌                | ❌         |
| `group`                | ❌                | ❌         |
| `home`                 | ❌                | ❌         |
| `is_absolute`          | ❌                | ❌         |
| `is_block_device`      | ❌                | ❌         |
| `is_char_device`       | ❌                | ❌         |
| `is_fifo`              | ❌                | ❌         |
| `is_mount`             | ❌                | ❌         |
| `is_reserved`          | ❌                | ❌         |
| `is_socket`            | ❌                | ❌         |
| `is_symlink`           | ❌                | ❌         |
| `lchmod`               | ❌                | ❌         |
| `link_to`              | ❌                | ❌         |
| `lstat`                | ❌                | ❌         |
| `owner`                | ❌                | ❌         |
| `relative_to`          | ❌                | ❌         |
| `resolve`              | ❌                | ❌         |
| `root`                 | ❌                | ❌         |
| `symlink_to`           | ❌                | ❌         |
| `cloud_prefix`         | ✅                | ✅         |
| `download_to`          | ✅                | ✅         |
| `etag`                 | ✅                | ✅         |
| `is_valid_cloudpath`   | ✅                | ✅         |
| `blob`                 | ✅                | ❌         |
| `bucket`               | ❌                | ✅         |
| `container`            | ✅                | ❌         |
| `key`                  | ❌                | ✅         |
| `md5`                  | ✅                | ❌         |


Here's an example to get the gist:

```python
from cloudpathlib import S3Path

root_dir = S3Path("s3://drivendata-public-assets/")

# there's only one file, but globbing works in nested folder
for f in root_dir.glob('**/*.txt'):
    text_data = f.read_text()
    print(f)
    print(text_data)

# use / to join paths (and, in this case, create a new file)
new_file_copy = root_dir / "nested_dir/copy_file.txt"

# show things work and the file does not exist yet
print(new_file_copy)
print(new_file_copy.exists())

# writing text data to the new file in the cloud
new_file_copy.write_text(text_data)

# file now listed
print(list(root_dir.glob('**/*.txt')))

# but, we can remove it
new_file_copy.unlink()

# no longer there
print(list(root_dir.glob('**/*.txt')))

```

Results in printing the following:

```
s3://drivendata-public-assets/odsc-west-2019/DATA_DICTIONARY.txt
Eviction Lab Data Dictionary

Additional information in our FAQ evictionlab.org/help-faq/
Full methodology evictionlab.org/methods/

Notes:
...  [MORE TEXT EXCISED]

s3://drivendata-public-assets/nested_dir/copy_file.txt
False
[S3Path('s3://drivendata-public-assets/nested_dir/copy_file.txt'), S3Path('s3://drivendata-public-assets/odsc-west-2019/DATA_DICTIONARY.txt')]
[S3Path('s3://drivendata-public-assets/odsc-west-2019/DATA_DICTIONARY.txt')]

```
