#!/usr/bin/env python
# coding: utf-8

# # Testing code that uses cloudpathlib

# Testing code that interacts with external resources can be a pain. For automated unit tests, the best practice is to mock connections. We provide some tools in cloudpathlib to make mocking easier.

# ## cloudpathlib.local module
# 
# In the `cloudpathlib.local` module, we provide "Local" classes that use the local filesystem in place of cloud storage. These classes are drop-in replacements for the normal cloud path classes, with the intent that you can use them as mock or monkeypatch substitutes in your tests. 
# 
# We also provide `CloudImplementation` objects which can be used to replace a registered implementation in the `cloudpathlib.implementation_registry` dictionary. Replacing the registered implementation will make `CloudPath`'s automatic dispatching use the replacement. 
# 
# See the examples below for how to use these replacements in your tests.
# 
# | Cloud Provider | Standard Classes | Local Classes | Local Implementation Object |
# |:-|:-|:-|:-|
# | Azure Blob Storage | `AzureBlobClient`<br>`AzureBlobPath` | `LocalAzureBlobCient`<br>`LocalAzureBlobPath` | `local_azure_blob_implementation` |
# | Google Cloud Storage | `GSClient`<br>`GSPath` | `LocalGSClient`<br>`LocalGSPath` | `local_gs_implementation` |
# | Amazon S3 | `S3Client`<br>`S3Path` | `LocalS3Client`<br>`LocalS3Path` | `local_s3_implementation` |
# 

# ## Examples: Monkeypatching in pytest

# In this section, we will show a few examples of how to mock cloudpathlib classes in the popular [pytest](https://docs.pytest.org/en/stable/contents.html) framework using its [monkeypatch](https://docs.pytest.org/en/stable/monkeypatch.html) feature. The general principles should work equivalently if you are using [unittest.mock](https://docs.python.org/3/library/unittest.mock.html) from the Python standard library. If you are new to mocking or having trouble applying it, we recommend you read and understand ["Where to patch"](https://docs.python.org/3/library/unittest.mock.html#where-to-patch).

import ipytest

ipytest.autoconfig()


# ### Patching direct instantiation

# In this example, we are testing a function `write` that directly instantiates a path using the `S3Path` constructor. 
# 
# Normally, calling `write` would either write to the real S3 bucket if able to authenticate, or it would fail with an error like `botocore.exceptions.NoCredentialsError`.
# 
# We use `monkeypatch` to replace the reference to `S3Path` being used with `LocalS3Path`. Our write succeeds (despite not being authenticated), and we can double-check that the cloud path object returned is actually an instance of `LocalS3Path`. 
# 
# Note that if you are writing tests for a package, and you import `write` from another module, you should patch the reference to `S3Path` from that module instead. 

get_ipython().run_cell_magic('run_pytest[clean]', '', '\nimport cloudpathlib\nfrom cloudpathlib.local import LocalS3Path\n\n\ndef write(uri: str):\n    """Function that uses S3Path."""\n    cloud_path = cloudpathlib.S3Path(uri)\n    cloud_path.write_text("cumulonimbus")\n    return cloud_path\n\n\ndef test_write_monkeypatch(monkeypatch):\n    """Testing function using S3Path, patching with LocalS3Path."""\n\n    monkeypatch.setattr(cloudpathlib, "S3Path", LocalS3Path)\n\n    cloud_path = write("s3://cloudpathlib-test-bucket/cumulonimbus.txt")\n    assert isinstance(cloud_path, LocalS3Path)\n    assert cloud_path.read_text() == "cumulonimbus"\n')


# ### Patching CloudPath dispatch
# 
# In this example, we are testing a function `write_with_dispatch` that uses the `CloudPath` constructor which dispatches to `S3Path` based on the `"s3://"` URI scheme. 
# 
# In order to change the dispatch behavior, we need to patch the cloudpathlib `implementation_registry`. The registry object is a dictionary (actually `defaultdict`) that holds meta `CloudImplementation` objects for each cloud storage service. 

from cloudpathlib import implementation_registry

implementation_registry


# We use `monkeypatch` to replace the `CloudImplementation` object in the registry that is keyed to `"s3"` with the `local_s3_implementation` object that we import from the `cloudpathlib.local` module. Our write succeeds, and we can double-check that the created cloud path object is indeed a `LocalS3Path` instance. 

get_ipython().run_cell_magic('run_pytest[clean]', '', '\nfrom cloudpathlib import CloudPath, implementation_registry\nfrom cloudpathlib.local import LocalS3Path, local_s3_implementation\n\n\ndef write_with_dispatch(uri: str):\n    """Function that uses CloudPath to dispatch to S3Path."""\n    cloud_path = CloudPath(uri)\n    cloud_path.write_text("cirrocumulus")\n    return cloud_path\n\n\ndef test_write_with_dispatch_monkeypatch(monkeypatch):\n    """Testing function using CloudPath dispatch, patching registered implementation. Will pass."""\n\n    monkeypatch.setitem(implementation_registry, "s3", local_s3_implementation)\n\n    cloud_path = write_with_dispatch("s3://cloudpathlib-test-bucket/cirrocumulus.txt")\n    assert isinstance(cloud_path, LocalS3Path)\n    assert cloud_path.read_text() == "cirrocumulus"\n')


# ### Setting up test assets

# In this example, we set up test assets in a pytest fixture before running our tests. (We also do the monkeypatching in the fixture—a code pattern for better reuse.)
# 
# There are two options for interacting with the storage backend for the local path classes. The example fixture below shows both options in action.
# 
# ####  1. Use local path class methods. 
# 
# This is the easiest and most direct approach. For example, `LocalS3Path` is fully functional and implements the same methods as `S3Path`. 
#  
# #### 2. Get a `pathlib.Path` object that points to the local storage directory. 
# 
# Each `LocalClient` class has a `TemporaryDirectory` instance that serves as its default local storage location. This is stored as an attribute of the class so that it persists across client instances. (For real cloud clients, authenticating multiple times to the same storage location doesn't affect the contents.)
# 
# You can use the `get_default_storage_dir` class method to get back a `pathlib.Path` object for that directory. Then you can use whatever `pathlib` or `shutil` functions to interact with it. 
# 
# ---
# 
# Finally, the `reset_default_storage_dir` class method will clean up the current local storage temporary directory and set up a new one. We recommend you do this in the teardown of the test fixture. 

get_ipython().run_cell_magic('run_pytest[clean]', '', '\nimport pytest\n\nfrom cloudpathlib import CloudPath, implementation_registry\nfrom cloudpathlib.local import LocalS3Client, LocalS3Path, local_s3_implementation\n\n\n@pytest.fixture\ndef cloud_asset_file(monkeypatch):\n    """Fixture that patches CloudPath dispatch and also sets up test assets in LocalS3Client\'s\n    local storage directory."""\n\n    monkeypatch.setitem(implementation_registry, "s3", local_s3_implementation)\n\n    # Option 1: Use LocalS3Path to set up test assets directly\n    local_cloud_path = LocalS3Path("s3://cloudpathlib-test-bucket/altostratus.txt")\n    local_cloud_path.write_text("altostratus")\n    \n    # Option 2: Use the pathlib.Path object that points to the local storage directory\n    local_pathlib_path: Path = (\n        LocalS3Client.get_default_storage_dir() / "cloudpathlib-test-bucket" / "nimbostratus.txt"\n    )\n    local_pathlib_path.parent.mkdir(exist_ok=True, parents=True)\n    local_pathlib_path.write_text("nimbostratus")\n\n    yield\n\n    LocalS3Client.reset_default_storage_dir()  # clean up temp directory and replace with new one\n\n\ndef test_with_assets(cloud_asset_file):\n    """Testing that a patched CloudPath finds the test asset created in the fixture."""\n\n    cloud_path_1 = CloudPath("s3://cloudpathlib-test-bucket/altostratus.txt")\n    assert isinstance(cloud_path_1, LocalS3Path)\n    assert cloud_path_1.exists()\n    assert cloud_path_1.read_text() == "altostratus"\n    \n    cloud_path_2 = CloudPath("s3://cloudpathlib-test-bucket/nimbostratus.txt")\n    assert isinstance(cloud_path_2, LocalS3Path)\n    assert cloud_path_2.exists()\n    assert cloud_path_2.read_text() == "nimbostratus"\n')

