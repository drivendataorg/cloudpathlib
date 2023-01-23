import os

import pytest

from cloudpathlib.enums import FileCacheMode
from cloudpathlib.exceptions import InvalidConfigurationException
from tests.conftest import CloudProviderTestRig


def assert_cache(
    cloudpath=None,
    path=None,
    client=None,
    client_path=None,
    exists=True,
    check_cloudpath=True,
    check_client_folder=True,
):
    if cloudpath:
        path = cloudpath._local

    if client:
        client_path = client._local_cache_dir
    elif cloudpath:
        client_path = cloudpath.client._local_cache_dir

    if check_cloudpath and path:
        assert path.exists() == exists

    if check_client_folder and client_path:
        assert client_path.exists() == exists


def test_defaults_work_as_expected(rig: CloudProviderTestRig):
    # use client that we can delete rather than default
    client = rig.client_class(**rig.required_client_kwargs)

    cp = rig.create_cloud_path("dir_0/file0_0.txt", client=client)

    # default should be tmp_dir
    assert cp.client.file_cache_mode == FileCacheMode.tmp_dir

    # download from cloud into the cache
    with cp.open("r") as f:
        _ = f.read()

    assert_cache(cloudpath=cp, exists=True, check_cloudpath=True, check_client_folder=True)

    cache_path = cp._local
    del cp

    assert_cache(path=cache_path, exists=True, check_cloudpath=True, check_client_folder=True)

    del client

    assert_cache(path=cache_path, exists=False, check_cloudpath=True, check_client_folder=True)


def test_close_file_mode(rig: CloudProviderTestRig):
    # use client that we can delete rather than default
    client = rig.client_class(
        file_cache_mode=FileCacheMode.close_file, **rig.required_client_kwargs
    )

    cp = rig.create_cloud_path("dir_0/file0_0.txt", client=client)

    # default should be tmp_dir
    assert cp.client.file_cache_mode == FileCacheMode.close_file

    # download from cloud into the cache
    # must use open for close_file mode
    with cp.open("r") as f:
        _ = f.read()

    assert_cache(cloudpath=cp, exists=False, check_cloudpath=True, check_client_folder=False)

    # download from cloud into the cache with different methods
    for method, method_args in [
        (cp.read_text, tuple()),
        (cp.read_bytes, tuple()),
        (cp.write_text, ("text",)),
        (cp.write_bytes, (b"bytes",)),
    ]:
        assert not cp._local.exists()
        method(*method_args)
        assert_cache(cloudpath=cp, exists=False, check_cloudpath=True, check_client_folder=False)

    cache_path = cp._local
    del cp

    assert_cache(path=cache_path, exists=False, check_cloudpath=True, check_client_folder=False)

    del client

    assert_cache(path=cache_path, exists=False, check_cloudpath=True, check_client_folder=True)


def test_cloudpath_object_mode(rig: CloudProviderTestRig):
    # use client that we can delete rather than default
    client = rig.client_class(
        file_cache_mode=FileCacheMode.cloudpath_object, **rig.required_client_kwargs
    )

    cp = rig.create_cloud_path("dir_0/file0_0.txt", client=client)

    assert cp.client.file_cache_mode == FileCacheMode.cloudpath_object

    # download from cloud into the cache
    with cp.open("r") as f:
        _ = f.read()

    assert_cache(cloudpath=cp, exists=True, check_cloudpath=True, check_client_folder=True)

    cache_path = cp._local
    del cp

    assert_cache(path=cache_path, exists=False, check_cloudpath=True, check_client_folder=False)

    del client

    assert_cache(path=cache_path, exists=False, check_cloudpath=True, check_client_folder=True)


def test_tmp_dir_mode(rig: CloudProviderTestRig):
    # use client that we can delete rather than default
    client = rig.client_class(file_cache_mode=FileCacheMode.tmp_dir, **rig.required_client_kwargs)

    cp = rig.create_cloud_path("dir_0/file0_0.txt", client=client)

    # default should be tmp_dir
    assert cp.client.file_cache_mode == FileCacheMode.tmp_dir

    # download from cloud into the cache
    with cp.open("r") as f:
        _ = f.read()

    assert_cache(cloudpath=cp, exists=True, check_cloudpath=True, check_client_folder=True)

    cache_path = cp._local
    del cp

    assert_cache(path=cache_path, exists=True, check_cloudpath=True, check_client_folder=True)

    del client

    assert_cache(path=cache_path, exists=False, check_cloudpath=True, check_client_folder=True)


def test_persistent_mode(rig: CloudProviderTestRig, tmpdir):
    client = rig.client_class(
        file_cache_mode=FileCacheMode.persistent,
        local_cache_dir=tmpdir,
        **rig.required_client_kwargs,
    )

    cp = rig.create_cloud_path("dir_0/file0_0.txt", client=client)

    assert cp.client.file_cache_mode == FileCacheMode.persistent

    # download from cloud into the cache
    with cp.open("r") as f:
        _ = f.read()

    assert_cache(cloudpath=cp, exists=True, check_cloudpath=True, check_client_folder=True)

    cache_path = cp._local
    del cp

    assert_cache(path=cache_path, exists=True, check_cloudpath=True, check_client_folder=True)

    del client

    assert_cache(path=cache_path, exists=True, check_cloudpath=True, check_client_folder=True)


def test_interaction_with_local_cache_dir(rig: CloudProviderTestRig, tmpdir):
    # cannot instantiate persistent without local file dir
    with pytest.raises(InvalidConfigurationException):
        client = rig.client_class(
            file_cache_mode=FileCacheMode.persistent, **rig.required_client_kwargs
        )

    # automatically set to persitent if not specified
    client = rig.client_class(local_cache_dir=tmpdir, **rig.required_client_kwargs)
    assert client.file_cache_mode == FileCacheMode.persistent

    # setting close_file still works
    client = rig.client_class(
        local_cache_dir=tmpdir,
        file_cache_mode=FileCacheMode.close_file,
        **rig.required_client_kwargs,
    )
    cp = rig.create_cloud_path("dir_0/file0_0.txt", client=client)
    assert cp.client.file_cache_mode == FileCacheMode.close_file

    # download from cloud into the cache
    # must use open for close_file mode
    with cp.open("r") as f:
        _ = f.read()

    assert_cache(cloudpath=cp, exists=False, check_cloudpath=True, check_client_folder=False)

    # setting cloudpath_object still works
    client = rig.client_class(
        local_cache_dir=tmpdir,
        file_cache_mode=FileCacheMode.cloudpath_object,
        **rig.required_client_kwargs,
    )
    cp = rig.create_cloud_path("dir_0/file0_0.txt", client=client)
    assert cp.client.file_cache_mode == FileCacheMode.cloudpath_object

    # download from cloud into the cache
    with cp.open("r") as f:
        _ = f.read()

    assert_cache(cloudpath=cp, exists=True, check_cloudpath=True, check_client_folder=True)

    cache_path = cp._local
    del cp

    assert_cache(path=cache_path, exists=False, check_cloudpath=True, check_client_folder=False)

    # setting tmp_dir still works
    client = rig.client_class(
        local_cache_dir=tmpdir, file_cache_mode=FileCacheMode.tmp_dir, **rig.required_client_kwargs
    )
    cp = rig.create_cloud_path("dir_0/file0_0.txt", client=client)
    assert cp.client.file_cache_mode == FileCacheMode.tmp_dir

    # download from cloud into the cache
    with cp.open("r") as f:
        _ = f.read()

    assert_cache(cloudpath=cp, exists=True, check_cloudpath=True, check_client_folder=True)

    cache_path = cp._local
    del cp

    assert_cache(path=cache_path, exists=True, check_cloudpath=True, check_client_folder=True)

    del client

    assert_cache(path=cache_path, exists=False, check_cloudpath=True, check_client_folder=True)


def test_string_instantiation(rig: CloudProviderTestRig, tmpdir):
    # string instantiation
    for v in FileCacheMode:
        local = tmpdir if v == FileCacheMode.persistent else None
        client = rig.client_class(
            file_cache_mode=v.value, local_cache_dir=local, **rig.required_client_kwargs
        )
        assert client.file_cache_mode == v


def test_environment_variable_instantiation(rig: CloudProviderTestRig, tmpdir):
    # environment instantiation
    original_env_setting = os.environ.get("CLOUPATHLIB_FILE_CACHE_MODE", "")

    try:
        for v in FileCacheMode:
            os.environ["CLOUPATHLIB_FILE_CACHE_MODE"] = v.value
            local = tmpdir if v == FileCacheMode.persistent else None
            client = rig.client_class(local_cache_dir=local, **rig.required_client_kwargs)
            assert client.file_cache_mode == v

    finally:
        os.environ["CLOUPATHLIB_FILE_CACHE_MODE"] = original_env_setting
