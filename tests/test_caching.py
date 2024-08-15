import gc
import os
import sys
from time import sleep
from pathlib import Path

import pytest
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_random_exponential,
)

from cloudpathlib.enums import FileCacheMode
from cloudpathlib.exceptions import (
    InvalidConfigurationException,
    OverwriteNewerCloudError,
    OverwriteNewerLocalError,
)
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


def test_loc_dir(rig: CloudProviderTestRig, tmpdir):
    """Tests that local cache dir is used when specified and works'
    with the different cache modes.

    Used to be called `test_interaction_with_local_cache_dir` but
    maybe that test name caused problems (see #382).
    """
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


def test_environment_variable_contentious_instantiation(rig: CloudProviderTestRig, tmpdir):
    # environment instantiation
    original_typo_env_setting = os.environ.get("CLOUPATHLIB_FILE_CACHE_MODE", "")
    original_env_setting = os.environ.get("CLOUDPATHLIB_FILE_CACHE_MODE", "")

    v_old = FileCacheMode.persistent
    try:
        for v in FileCacheMode:
            os.environ["CLOUPATHLIB_FILE_CACHE_MODE"] = v_old.value
            os.environ["CLOUDPATHLIB_FILE_CACHE_MODE"] = v.value
            local = tmpdir if v == FileCacheMode.persistent else None
            client = rig.client_class(local_cache_dir=local, **rig.required_client_kwargs)
            assert client.file_cache_mode == v

    finally:
        os.environ["CLOUPATHLIB_FILE_CACHE_MODE"] = original_typo_env_setting
        os.environ["CLOUDPATHLIB_FILE_CACHE_MODE"] = original_env_setting


def test_environment_variable_old_instantiation(rig: CloudProviderTestRig, tmpdir):
    # environment instantiation
    original_typo_env_setting = os.environ.get("CLOUPATHLIB_FILE_CACHE_MODE", "")
    original_env_setting = os.environ.get("CLOUDPATHLIB_FILE_CACHE_MODE", "")

    try:
        os.environ["CLOUDPATHLIB_FILE_CACHE_MODE"] = ""
        for v in FileCacheMode:
            os.environ["CLOUPATHLIB_FILE_CACHE_MODE"] = v.value
            local = tmpdir if v == FileCacheMode.persistent else None
            client = rig.client_class(local_cache_dir=local, **rig.required_client_kwargs)
            assert client.file_cache_mode == v

    finally:
        os.environ["CLOUPATHLIB_FILE_CACHE_MODE"] = original_typo_env_setting
        os.environ["CLOUDPATHLIB_FILE_CACHE_MODE"] = original_env_setting


def test_environment_variable_instantiation(rig: CloudProviderTestRig, tmpdir):
    # environment instantiation
    original_env_setting = os.environ.get("CLOUDPATHLIB_FILE_CACHE_MODE", "")

    try:
        for v in FileCacheMode:
            os.environ["CLOUDPATHLIB_FILE_CACHE_MODE"] = v.value
            local = tmpdir if v == FileCacheMode.persistent else None
            client = rig.client_class(local_cache_dir=local, **rig.required_client_kwargs)
            assert client.file_cache_mode == v

    finally:
        os.environ["CLOUDPATHLIB_FILE_CACHE_MODE"] = original_env_setting


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


def test_environment_variables_force_overwrite_from(rig: CloudProviderTestRig, tmpdir):
    # environment instantiation
    original_env_setting = os.environ.get("CLOUDPATHLIB_FORCE_OVERWRITE_FROM_CLOUD", "")

    try:
        # explicitly false overwrite
        os.environ["CLOUDPATHLIB_FORCE_OVERWRITE_FROM_CLOUD"] = "False"

        p = rig.create_cloud_path("dir_0/file0_0.txt")
        p._refresh_cache()  # dl to cache
        p._local.touch()  # update mod time

        with pytest.raises(OverwriteNewerLocalError):
            p._refresh_cache()

        for val in ["1", "True", "TRUE"]:
            os.environ["CLOUDPATHLIB_FORCE_OVERWRITE_FROM_CLOUD"] = val

            p = rig.create_cloud_path("dir_0/file0_0.txt")

            orig_mod_time = p.stat().st_mtime

            p._refresh_cache()  # dl to cache
            p._local.touch()  # update mod time

            new_mod_time = p._local.stat().st_mtime

            p._refresh_cache()
            assert p._local.stat().st_mtime == orig_mod_time
            assert p._local.stat().st_mtime < new_mod_time

    finally:
        os.environ["CLOUDPATHLIB_FORCE_OVERWRITE_FROM_CLOUD"] = original_env_setting


def test_environment_variables_force_overwrite_to(rig: CloudProviderTestRig, tmpdir):
    # environment instantiation
    original_env_setting = os.environ.get("CLOUDPATHLIB_FORCE_OVERWRITE_TO_CLOUD", "")

    try:
        # explicitly false overwrite
        os.environ["CLOUDPATHLIB_FORCE_OVERWRITE_TO_CLOUD"] = "False"

        p = rig.create_cloud_path("dir_0/file0_0.txt")

        new_local = Path((tmpdir / "new_content.txt").strpath)
        new_local.write_text("hello")
        new_also_cloud = rig.create_cloud_path("dir_0/another_cloud_file.txt")
        new_also_cloud.write_text("newer")

        # make cloud newer than local or other cloud file
        os.utime(new_local, (new_local.stat().st_mtime - 2, new_local.stat().st_mtime - 2))

        p.write_text("updated")

        with pytest.raises(OverwriteNewerCloudError):
            p._upload_file_to_cloud(new_local)

        with pytest.raises(OverwriteNewerCloudError):
            # copy short-circuits upload if same client, so we test separately

            # raises if destination is newer
            new_also_cloud.write_text("newest")
            sleep(0.01)
            p.copy(new_also_cloud)

        for val in ["1", "True", "TRUE"]:
            os.environ["CLOUDPATHLIB_FORCE_OVERWRITE_TO_CLOUD"] = val

            p = rig.create_cloud_path("dir_0/file0_0.txt")

            new_local.write_text("updated")

            # make cloud newer than local
            os.utime(new_local, (new_local.stat().st_mtime - 2, new_local.stat().st_mtime - 2))

            p.write_text("updated")

            orig_cloud_mod_time = p.stat().st_mtime

            assert p.stat().st_mtime >= new_local.stat().st_mtime

            # would raise if not set
            sleep(1.01)  # give time so not equal when rounded
            p._upload_file_to_cloud(new_local)
            assert p.stat().st_mtime > orig_cloud_mod_time  # cloud now overwritten

            new_also_cloud = rig.create_cloud_path("dir_0/another_cloud_file.txt")
            sleep(1.01)  # give time so not equal when rounded
            new_also_cloud.write_text("newer")

            new_cloud_mod_time = new_also_cloud.stat().st_mtime

            assert p.stat().st_mtime < new_cloud_mod_time  # would raise if not set
            p.copy(new_also_cloud)
            assert new_also_cloud.stat().st_mtime >= new_cloud_mod_time

    finally:
        os.environ["CLOUDPATHLIB_FORCE_OVERWRITE_TO_CLOUD"] = original_env_setting


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

    # Enable debugging for garbage collection
    gc.callbacks.append(lambda event, args: print(f"GC {event} - {args}"))

    # also removes containing folder on client cleanted up
    local_cache_path = cp._local
    client_cache_folder = client._local_cache_dir
    del cp
    del client

    def _debug(path):
        import subprocess
        import psutil

        # file handles on windows
        if sys.platform == "win32":
            import subprocess

            result = subprocess.run(["handle.exe", path], capture_output=True, text=True)
            print(f" HANDLES FOR {path}")
            print(result.stdout)

        # processes with open files
        open_files = []
        for proc in psutil.process_iter(["pid", "name", "open_files"]):
            for file in proc.info["open_files"] or []:
                if path in file.path:
                    open_files.append((proc.info["pid"], proc.info["name"], file.path))

        print(f" OPEN FILES INFO FOR {path}")
        print(open_files)

    # in CI there can be a lag before the cleanup actually happens
    @retry(
        retry=retry_if_exception_type(AssertionError),
        wait=wait_random_exponential(multiplier=0.5, max=5),
        stop=stop_after_attempt(10),
        reraise=True,
    )
    def _resilient_assert():
        _debug(str(local_cache_path.resolve()))
        _debug(str(client_cache_folder.resolve()))

        gc.collect()  # force gc before asserting

        assert not local_cache_path.exists()
        assert not client_cache_folder.exists()

    _resilient_assert()

    gc.callbacks.pop()


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
