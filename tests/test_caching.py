import os
from time import sleep
from pathlib import Path

import pytest

from cloudpathlib.enums import FileCacheMode
from cloudpathlib.exceptions import InvalidConfigurationException
from tests.conftest import CloudProviderTestRig


def test_defaults_work_as_expected(rig: CloudProviderTestRig):
    # use client that we can delete rather than default
    client = rig.client_class(**rig.required_client_kwargs)

    cp = rig.create_cloud_path("dir_0/file0_0.txt", client=client)

    # default should be tmp_dir
    assert cp.client.file_cache_mode == FileCacheMode.tmp_dir

    # download from cloud into the cache
    with cp.open("r") as f:
        _ = f.read()

    # both exist
    assert cp._local.exists()
    assert cp.client._local_cache_dir.exists()

    cache_path = cp._local
    client_cache_dir = cp.client._local_cache_dir
    del cp

    # both exist
    assert cache_path.exists()
    assert client_cache_dir.exists()

    del client

    # cleaned up because client out of scope
    assert not cache_path.exists()
    assert not client_cache_dir.exists()


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

    # file cache does not exist, but client folder may still be around
    assert not cp._local.exists()
    assert cp.client._local_cache_dir.exists()

    methods_to_test = [
        (cp.read_text, tuple()),
        (cp.read_bytes, tuple()),
        (cp.write_text, ("text",)),
        (cp.write_bytes, (b"bytes",)),
    ]

    # download from cloud into the cache with different methods
    for method, method_args in methods_to_test:
        assert not cp._local.exists()
        method(*method_args)

        # file cache does not exist, but client folder may still be around
        assert not cp._local.exists()
        assert cp.client._local_cache_dir.exists()

        sleep(0.1)  # writing twice in a row too quickly can trigger `OverwriteNewerCloudError`


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

    # both exist
    assert cp._local.exists()
    assert cp.client._local_cache_dir.exists()

    cache_path = cp._local
    client_cache_dir = cp.client._local_cache_dir
    del cp

    assert not cache_path.exists()
    assert client_cache_dir.exists()

    del client

    assert not cache_path.exists()
    assert not client_cache_dir.exists()


def test_tmp_dir_mode(rig: CloudProviderTestRig):
    # use client that we can delete rather than default
    client = rig.client_class(file_cache_mode=FileCacheMode.tmp_dir, **rig.required_client_kwargs)

    cp = rig.create_cloud_path("dir_0/file0_0.txt", client=client)

    # default should be tmp_dir
    assert cp.client.file_cache_mode == FileCacheMode.tmp_dir

    # download from cloud into the cache
    with cp.open("r") as f:
        _ = f.read()

    # both exist
    assert cp._local.exists()
    assert cp.client._local_cache_dir.exists()

    cache_path = cp._local
    client_cache_dir = cp.client._local_cache_dir
    del cp

    # both exist
    assert cache_path.exists()
    assert client_cache_dir.exists()

    del client

    # cleaned up because client out of scope
    assert not cache_path.exists()
    assert not client_cache_dir.exists()


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

    # both exist
    assert cp._local.exists()
    assert cp.client._local_cache_dir.exists()

    cache_path = cp._local
    client_cache_dir = cp.client._local_cache_dir
    del cp

    # both exist
    assert cache_path.exists()
    assert client_cache_dir.exists()

    del client

    # both exist
    assert cache_path.exists()
    assert client_cache_dir.exists()


def test_interaction_with_local_cache_dir(rig: CloudProviderTestRig, tmpdir):
    default_sleep = 0.5  # sometimes GH runners are slow and fail saying dir doesn't exist

    # cannot instantiate persistent without local file dir
    with pytest.raises(InvalidConfigurationException):
        client = rig.client_class(
            file_cache_mode=FileCacheMode.persistent, **rig.required_client_kwargs
        )

    # automatically set to persitent if not specified
    client = rig.client_class(local_cache_dir=tmpdir, **rig.required_client_kwargs)
    assert client.file_cache_mode == FileCacheMode.persistent

    # test setting close_file explicitly works
    client = rig.client_class(
        local_cache_dir=tmpdir,
        file_cache_mode=FileCacheMode.close_file,
        **rig.required_client_kwargs,
    )
    cp = rig.create_cloud_path("dir_0/file0_0.txt", client=client)
    assert cp.client.file_cache_mode == FileCacheMode.close_file

    # download from cloud into the cache
    # must use open for close_file mode
    sleep(default_sleep)
    with cp.open("r") as f:
        _ = f.read()

    assert not cp._local.exists()

    # setting cloudpath_object still works
    client = rig.client_class(
        local_cache_dir=tmpdir,
        file_cache_mode=FileCacheMode.cloudpath_object,
        **rig.required_client_kwargs,
    )
    cp = rig.create_cloud_path("dir_0/file0_0.txt", client=client)
    assert cp.client.file_cache_mode == FileCacheMode.cloudpath_object

    # download from cloud into the cache
    sleep(default_sleep)  # test can be flaky saying that the cache dir doesn't exist yet
    with cp.open("r") as f:
        _ = f.read()

    assert cp._local.exists()

    cache_path = cp._local
    del cp

    assert not cache_path.exists()

    # setting tmp_dir still works
    client = rig.client_class(
        local_cache_dir=tmpdir, file_cache_mode=FileCacheMode.tmp_dir, **rig.required_client_kwargs
    )
    cp = rig.create_cloud_path("dir_0/file0_0.txt", client=client)
    assert cp.client.file_cache_mode == FileCacheMode.tmp_dir

    # download from cloud into the cache
    sleep(default_sleep)  # test can be flaky saying that the cache dir doesn't exist yet
    with cp.open("r") as f:
        _ = f.read()

    # both exist
    assert cp._local.exists()
    assert cp.client._local_cache_dir.exists()

    cache_path = cp._local
    client_cache_dir = cp.client._local_cache_dir
    del cp

    # both exist
    assert cache_path.exists()
    assert client_cache_dir.exists()

    del client

    # cleaned up because client out of scope
    assert not cache_path.exists()
    assert not client_cache_dir.exists()


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


def test_environment_variable_local_cache_dir(rig: CloudProviderTestRig, tmpdir):
    # environment instantiation
    original_env_setting = os.environ.get("CLOUDPATHLIB_LOCAL_CACHE_DIR", "")

    try:
        os.environ["CLOUDPATHLIB_LOCAL_CACHE_DIR"] = tmpdir.strpath
        client = rig.client_class(**rig.required_client_kwargs)
        assert client._local_cache_dir == Path(tmpdir.strpath)

        cp = rig.create_cloud_path("dir_0/file0_0.txt", client=client)
        cp.fspath  # download from cloud into the cache
        assert (tmpdir / cp._no_prefix).exists()

        # "" treated as None; falls back to temp dir for cache
        os.environ["CLOUDPATHLIB_LOCAL_CACHE_DIR"] = ""
        client = rig.client_class(**rig.required_client_kwargs)
        assert client._cache_tmp_dir is not None

    finally:
        os.environ["CLOUDPATHLIB_LOCAL_CACHE_DIR"] = original_env_setting


def test_manual_cache_clearing(rig: CloudProviderTestRig):
    # use client that we can delete rather than default
    client = rig.client_class(**rig.required_client_kwargs)

    cp = rig.create_cloud_path("dir_0/file0_0.txt", client=client)

    # default should be tmp_dir
    assert cp.client.file_cache_mode == FileCacheMode.tmp_dir

    # download from cloud into the cache
    with cp.open("r") as f:
        _ = f.read()

    # both exist
    assert cp._local.exists()
    assert cp.client._local_cache_dir.exists()

    # clears the file itself, but not the containg folder
    cp.clear_cache()

    assert not cp._local.exists()
    assert cp.client._local_cache_dir.exists()

    # test removing parent directory
    cp.fspath
    assert cp._local.exists()
    assert cp.parent._local.exists()

    cp.parent.clear_cache()

    assert not cp._local.exists()
    assert not cp.parent._local.exists()

    # download two files from cloud into the cache
    cp.fspath
    rig.create_cloud_path("dir_0/file0_1.txt", client=client).fspath

    # 2 files present in cache folder
    assert len(list(filter(lambda x: x.is_file(), client._local_cache_dir.rglob("*")))) == 2

    # clears all files inside the folder, but containing folder still exists
    client.clear_cache()

    assert len(list(filter(lambda x: x.is_file(), client._local_cache_dir.rglob("*")))) == 0

    # also removes containing folder on client cleanted up
    local_cache_path = cp._local
    client_cache_folder = client._local_cache_dir
    del cp
    del client

    assert not local_cache_path.exists()
    assert not client_cache_folder.exists()


def test_reuse_cache_after_manual_cache_clear(rig: CloudProviderTestRig):
    # use client that we can delete rather than default
    client = rig.client_class(**rig.required_client_kwargs)

    cp = rig.create_cloud_path("dir_0/file0_0.txt", client=client)

    # default should be tmp_dir
    assert cp.client.file_cache_mode == FileCacheMode.tmp_dir

    # download from cloud into the cache
    with cp.open("r") as f:
        _ = f.read()

    cp.clear_cache()
    assert not cp._local.exists()

    # re-download from cloud into the cache
    with cp.open("r") as f:
        _ = f.read()

    client.clear_cache()
    assert not cp._local.exists()

    # re-download from cloud into the cache, no error
    with cp.open("r") as f:
        _ = f.read()

    assert cp._local.exists()
