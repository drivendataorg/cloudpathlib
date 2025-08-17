#!/usr/bin/env python
# coding: utf-8

# # Compatibility

# ## Patching Python builtins (third-party library compatibility)
# 
# Not every Python library in the broad universe of Python libraries is implemented to accept pathlib-compatible objects like those implemented by cloudpathlib. Many libraries will only accept strings as filepaths. These libraries internally use `open`, functions from `os` and `os.path`, or other core library modules like `glob` to navigate paths and manipulate them.
# 
# This means that out-of-the-box you can't just pass a `CloudPath` object to any library. For those implemented with `pathlib`, this will work. For anything else the code will throw an exception at some point.
# 
# The long-term solution is to ask developers to implement their library to support either (1) pathlib-compatible objects for files and directories, or (2) file-like objects passed directly (e.g., so you could call `CloudPath.open` in your code and pass the the file-like object to the library).
# 
# The near-term workaround that will be compatible with some libraries is to patch the builtins to make `open`, `os`, `os.path`, and `glob` work with `CloudPath` objects. Because this overrides default Python functionality, this is not on by default. When patched, these functions will use the `CloudPath` version if they are passed a `CloudPath` and will fallback to their normal implementations otherwise.
# 
# There are three ways to enable these patches: environment variables, globally with a function call, or just in a specific context with a context manager.
# 
# ## Differences in reading versus writing to `CloudPath`
# 
# A major reason to patch these builtins is if you want to write to a `CloudPath` with a third party library. For scenarios where you are reading files, you may not need to do any patching. Many python libraries support using [`__fspath__`](https://docs.python.org/3/library/os.html#os.PathLike.__fspath__) to get the location of a file on disk.
# 
# We implement `CloudPath.__fspath__`, which will cache the file to the local disk and provide that file path as a string to any library that uses `fspath`. This works well for reading files, but not for writing them. Because there is no callback to our code once that filepath gets written to, we can't see changes and then push those changes from the cache back to the cloud (see related discussions in [#73](https://github.com/drivendataorg/cloudpathlib/issues/73), [#128](https://github.com/drivendataorg/cloudpathlib/issues/128), [#140](https://github.com/drivendataorg/cloudpathlib/pull/140)). In many scenarios our code will never get called again.
# 
# For this reason, it is better to patch the built-in functions to handle `CloudPath` objects rather than rely on `__fspath__`, especially if you are writing to these files.
# 
# 
# ## Setting with environment variables
# 
# These methods can be enabled by setting the following environment variables:
#  - `CLOUDPATHLIB_PATCH_ALL=1` - patch all the builtins we implement: `open`, `os` functions, and `glob`
#  - `CLOUDPATHLIB_PATCH_OPEN=1` - patch the builtin `open` method
#  - `CLOUDPATHLIB_PATCH_OS_FUNCTIONS=1` - patch the `os` functions
#  - `CLOUDPATHLIB_PATCH_GLOB=1` - patch the `glob` module
# 
# You can set environment variables in many ways, but it is common to either pass it at the command line with something like `CLOUDPATHLIB_PATCH_ALL=1 python my_script.py` or to set it in your Python script with `os.environ['CLOUDPATHLIB_PATCH_ALL'] = 1`. Note, these _must_ be set before any `cloudpathlib` methods are imported.
# 
# ## Setting with patch methods globally
# 
# Instead of setting environment variables, you can call methods to patch the functions. For example, you may call these at import time in your application or script. This will use the patched methods throughout your application.
# 
# ```python
# from cloudpathlib import patch_all_builtins, patch_open, patch_os_functions, patch_glob
# 
# # patch the builtins your code or a library that you call uses
# patch_open()
# patch_os_functions()
# patch_glob()
# 
# # or, if you want all of these at once
# patch_all_builtins()
# ```
# 
# ## Setting with a context manager
# 
# Finally, you can control the scope which the patch is used with a context manager. For example, you may have just one call to an external library that is failing to accept `CloudPath`. You can limit the patch effect to that call by using a context manager, which will remove the patch at the end of the block. This is useful if you want to patch the functions for a specific block of code but not for the rest of the application.
# 
# ```python
# from cloudpathlib import patch_all_builtins
# 
# with patch_all_builtins():
#     with open(cloud_path) as f:
#         data = f.read()
# ```
# 
# This is the narrowest, most targeted way to update the builtin Python methods that don't just work with `CloudPath` objects.
# 
# Next, we'll walk through some examples of patching and using these methods.
# 

# We can see a similar result for patching the functions in the `os` module.

# ## Patching `open`
# 
# Sometimes code uses the Python built-in `open` to open files and operate on them. In those cases, passing a `CloudPath` will fail. You can patch the built-in `open` so that when a `CloudPath` is provided it uses `CloudPath.open`, otherwise defers to the original behavior.
# 
# Here's an example that would not work unless you patch the built-ins (for example, if you depend on a third-party library that calls `open`).
# 
# It will fail with an `OverwriteNewerLocalError` because `read_text` tries to download from the cloud to a cache path that has been updated locally (but, crucially, not rewritten back to the cloud).
# 

# Imagine that deep in a third-party library a function is implemented like this
def library_function(filepath: str):
    with open(filepath, "w") as f:
        f.write("hello!")


from cloudpathlib import CloudPath

# create file to read
cp = CloudPath("s3://cloudpathlib-test-bucket/patching_builtins/new_file.txt")

try:
    library_function(cp)

    # read the text that was written
    assert cp.read_text() == "hello!"
except Exception as e:
    print(type(e))
    print(e)


# ### Patching `open` in Jupyter notebooks
# 
# Since this documentation runs as a Jupyter notebook, there is an extra step to patch `open`. Jupyter notebooks inject their own `open` into the user namespace. After enabling the patch, ensure the notebook's `open` refers to the patched built-in:
# 
# ```python
# from cloudpathlib import patch_open
# 
# open = patch_open().patched   # rebind notebook's open to the patched version
# ```

from cloudpathlib import CloudPath, patch_open

# enable patch and rebind notebook's open
open = patch_open().patched

# create file to read
cp = CloudPath("s3://cloudpathlib-test-bucket/patching_builtins/file.txt")

library_function(cp)
assert cp.read_text() == "hello!"
print("Succeeded!")


# ## Examples: os.path functions with CloudPath
# 
# The snippet below demonstrates common `os.path` functions when patched to accept `CloudPath` values. These calls work for `CloudPath` and still behave normally for string paths.
# 

import os

from cloudpathlib import patch_os_functions, CloudPath

cp = CloudPath("s3://cloudpathlib-test-bucket/patching_builtins/file.txt")
folder = cp.parent

try:
    print(os.path.isdir(folder))
except Exception as e:
    print("Unpatched version fails:")
    print(e)


with patch_os_functions():
    result = os.path.isdir(folder)
    print("Patched version of `os.path.isdir` returns: ", result)

    print("basename:", os.path.basename(cp))

    print("dirname:", os.path.dirname(cp))

    joined = os.path.join(folder, "dir", "sub", "name.txt")
    print("join:", joined)


# ## Examples: glob with CloudPath
# 
# The snippet below demonstrates `glob.glob` and `glob.iglob` working with `CloudPath` as the pattern or `root_dir` when patched.
# 

from glob import glob

from cloudpathlib import patch_glob, CloudPath

try:
    glob(CloudPath("s3://cloudpathlib-test-bucket/manual-tests/**/*dir*/**"))
except Exception as e:
    print("Unpatched version fails:")
    print(e)


with patch_glob():
    print("Patched succeeds:")
    print(glob(CloudPath("s3://cloudpathlib-test-bucket/manual-tests/**/*dir*/**/*")))

    # or equivalently
    print(glob("**/*dir*/**/*", root_dir=CloudPath("s3://cloudpathlib-test-bucket/manual-tests/")))


# # Examples with third party libraries
# 
# Here we show that third party libraries, like Pillow, that don't work as expected without patching the built-ins.
# 
# However, if we patch built-ins, we can see the functions work as expected.

# ## Pillow example

from cloudpathlib import CloudPath, patch_all_builtins
from PIL import Image


base = CloudPath("s3://cloudpathlib-test-bucket/patching_builtins/third_party/")

img_path = base / "pillow_demo.png"

# Unpatched: using CloudPath directly fails
try:
    Image.new("RGB", (10, 10), color=(255, 0, 0)).save(img_path)
except Exception as e:
    print("Pillow without patch: FAILED:", e)


# Patched: success with patching builtins
with patch_all_builtins():
    Image.new("RGB", (10, 10), color=(255, 0, 0)).save(img_path)

    assert img_path.read_bytes()
    print("With patches, Pillow successfully writes to a CloudPath")


# ## Caveat: Some libraries still do not work
# 
# Even with patches, some libraries will not work. For example, writing directly to a `CloudPath` with `pandas` is not possible because `pandas` has a complex set of IO checks it does in its own codebase.
# 
# For many of these libraries (including `pandas`) using `CloudPath.open` and then passing the buffer to the functions that can read and write to those buffers is usually the cleanest workaround.
# 
# For example, here is the best way to write to a `CloudPath` with `pandas`:

import pandas as pd

df = pd.DataFrame([[0, 1], [2, 3]], columns=["a", "b"])

cloud_path = base / "data.csv"

try:
    df.to_csv(cloud_path)
except Exception as e:
    print("Could not write with `to_csv` because error: ", e)


# instead, use .open
with cloud_path.open("w") as f:
    df.to_csv(f)

assert cloud_path.exists()
print("Successfully wrote to ", cloud_path)

