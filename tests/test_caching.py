import os
from pathlib import Path

from google.api_core.exceptions import TooManyRequests
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
    NoStatError,
    OverwriteNewerCloudError,
    OverwriteDirtyFileError,
    OverwriteNewerLocalError,
)
from tests.conftest import CloudProviderTestRig
from tests.utils import (
    _sync_filesystem,
    assert_collected_and_cleaned,
    weakrefs_to,
)


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

    refs = weakrefs_to(cp, client)
    del cp

    # both exist; in tmp_dir mode cleanup happens when the client goes away, not the path
    assert cache_path.exists()
    assert client_cache_dir.exists()

    del client

    # cleaned up because client out of scope
    assert_collected_and_cleaned(refs, gone=[cache_path, client_cache_dir])


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

    cp_refs = weakrefs_to(cp)
    del cp

    # cloudpath_object mode clears the cached file when the path object is collected
    assert_collected_and_cleaned(cp_refs, gone=[cache_path])
    assert client_cache_dir.exists()

    client_refs = weakrefs_to(client)
    del client

    assert_collected_and_cleaned(client_refs, gone=[client_cache_dir])
    assert not cache_path.exists()


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

    refs = weakrefs_to(cp, client)
    del cp

    # both exist
    assert cache_path.exists()
    assert client_cache_dir.exists()

    del client

    # cleaned up because client out of scope
    assert_collected_and_cleaned(refs, gone=[cache_path, client_cache_dir])


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

    refs = weakrefs_to(cp, client)
    del cp
    del client

    # nothing is cleaned up in persistent mode, even once both objects are collected
    assert_collected_and_cleaned(refs)
    assert cache_path.exists()
    assert client_cache_dir.exists()


# downloads into the cache dir under tmpdir occasionally fail with a FileNotFoundError
# against live backends (see the `wait_for_mkdir` fixture and #382); retry like the other
# live-flaky tests in this module
@pytest.mark.flaky(reruns=3, reruns_delay=1, condition=os.getenv("USE_LIVE_CLOUD") == "1")
def test_loc_dir(rig: CloudProviderTestRig, tmpdir, wait_for_mkdir):
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

    # automatically set to persistent if not specified
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
    cp_refs = weakrefs_to(cp)
    del cp

    assert_collected_and_cleaned(cp_refs, gone=[cache_path])

    # setting tmp_dir still works
    client = rig.client_class(
        local_cache_dir=tmpdir, file_cache_mode=FileCacheMode.tmp_dir, **rig.required_client_kwargs
    )
    cp = rig.create_cloud_path("dir_0/file0_0.txt", client=client)
    assert cp.client.file_cache_mode == FileCacheMode.tmp_dir

    # download from cloud into the cache
    _sync_filesystem()
    with cp.open("r") as f:
        _ = f.read()

    # both exist
    assert cp._local.exists()
    assert cp.client._local_cache_dir.exists()

    cache_path = cp._local
    client_cache_dir = cp.client._local_cache_dir

    refs = weakrefs_to(cp, client)
    del cp

    # both exist
    assert cache_path.exists()
    assert client_cache_dir.exists()

    del client

    # cleaned up because client out of scope
    assert_collected_and_cleaned(refs, gone=[cache_path, client_cache_dir])


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


@pytest.mark.flaky(reruns=3, reruns_delay=1, condition=os.getenv("USE_LIVE_CLOUD") == "1")
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


@pytest.mark.flaky(reruns=3, reruns_delay=1, condition=os.getenv("USE_LIVE_CLOUD") == "1")
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

            # equal timestamps also raise for cloud-to-cloud copy
            new_also_cloud.write_text("newest")
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
            @retry(
                retry=retry_if_exception_type((AssertionError, TooManyRequests)),
                wait=wait_random_exponential(multiplier=0.5, max=5),
                stop=stop_after_attempt(10),
                reraise=True,
            )
            def _wait_for_cloud_newer():
                p._upload_file_to_cloud(new_local)
                assert p.stat().st_mtime > orig_cloud_mod_time  # cloud now overwritten

            _wait_for_cloud_newer()

            new_also_cloud = rig.create_cloud_path("dir_0/another_cloud_file.txt")

            @retry(
                retry=retry_if_exception_type(
                    (OverwriteNewerLocalError, AssertionError, TooManyRequests)
                ),
                wait=wait_random_exponential(multiplier=0.5, max=5),
                stop=stop_after_attempt(10),
                reraise=True,
            )
            def _retry_write_until_old_enough():
                new_also_cloud.write_text("newer")
                new_cloud_mod_time = new_also_cloud.stat().st_mtime
                assert p.stat().st_mtime < new_cloud_mod_time  # would raise if not set
                return new_cloud_mod_time

            new_cloud_mod_time = _retry_write_until_old_enough()

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

    # clears the file itself, but not the containing folder
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

    refs = weakrefs_to(cp, client)
    del cp
    del client

    assert_collected_and_cleaned(refs, gone=[local_cache_path, client_cache_folder])


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


def test_write_mtime_tie_does_not_raise(rig: CloudProviderTestRig):
    """A save that leaves the cache file's mtime exactly equal to the cloud version's
    (coarse-resolution filesystems, no-op writes) must bump the mtime and upload rather
    than raise OverwriteNewerCloudError."""
    client = rig.client_class(**rig.required_client_kwargs)
    cp = rig.create_cloud_path("dir_0/file0_0.txt", client=client)

    cp.write_text("v1")
    _sync_filesystem()

    # re-sync the cache from the cloud so the cache file's mtime equals the cloud mtime
    cp.clear_cache()
    cp.read_text()
    cloud_mtime = cp.stat().st_mtime

    with cp.open("w") as f:
        f.write("v2")
        f.flush()
        # simulate a write that leaves the mtime unchanged (e.g. same-second write on a
        # coarse-resolution filesystem)
        os.utime(cp._local, times=(cloud_mtime, cloud_mtime))

    assert cp.read_text() == "v2"


def test_streaming_append_fallback_cache_cleaned_up(rig: CloudProviderTestRig):
    """Append/update modes fall back to the cache in streaming mode; like `close_file`,
    the cache file is removed as soon as the handle is closed and uploaded."""
    client = rig.client_class(
        file_cache_mode=FileCacheMode.streaming, **rig.required_client_kwargs
    )
    cp = rig.create_cloud_path("dir_0/file0_0.txt", client=client)
    original = cp.read_text()

    with pytest.warns(UserWarning, match="downloads the whole object"):
        f = cp.open("a")
    with f:
        f.write("appended")
        assert cp._local.exists()  # the fallback works on a real cache file

    assert not cp._local.exists()
    assert cp.read_text() == original + "appended"


@pytest.mark.parametrize("mode", ["x", "a"])
def test_unlink_clears_cache_before_recreating(rig: CloudProviderTestRig, mode):
    cp = rig.create_cloud_path("unlink-cache.txt")
    cp.write_text("old content")
    assert cp._local.exists()

    cp.unlink()

    assert not cp.exists()
    assert not cp._local.exists()
    with cp.open(mode) as handle:
        handle.write("new content")
    assert cp.read_text() == "new content"


@pytest.mark.parametrize("method", ["rename", "replace"])
@pytest.mark.parametrize("target_exists", [False, True])
def test_move_clears_source_and_target_caches(rig: CloudProviderTestRig, method, target_exists):
    source = rig.create_cloud_path("move-source.txt")
    target = rig.create_cloud_path("move-target.txt")
    source.write_text("source content")
    target.write_text("old target content")
    if not target_exists:
        # Model a remote deletion made outside cloudpathlib, leaving a stale target cache.
        target.client._remove(target)
    assert source._local.exists()
    assert target._local.exists()

    result = getattr(source, method)(target)

    assert result == target
    assert not source.exists()
    assert not source._local.exists()
    assert not target._local.exists()
    assert target.read_text() == "source content"
    with source.open("x") as handle:
        handle.write("recreated source")
    assert source.read_text() == "recreated source"


@pytest.mark.parametrize("method", ["rename", "replace"])
def test_move_to_same_path_preserves_cache(rig: CloudProviderTestRig, method):
    cp = rig.create_cloud_path("same-path.txt")
    cp.write_text("unchanged")

    assert getattr(cp, method)(cp) == cp

    assert cp._local.exists()
    assert cp.read_text() == "unchanged"


def test_rmtree_clears_only_its_cache_subtree(rig: CloudProviderTestRig):
    directory = rig.create_cloud_path("delete-tree/")
    child = directory / "nested" / "file.txt"
    sibling = rig.create_cloud_path("keep-tree/file.txt")
    child.write_text("delete")
    sibling.write_text("keep")
    assert child._local.exists()
    assert sibling._local.exists()

    directory.rmtree()

    assert not directory.exists()
    assert not directory._local.exists()
    assert sibling._local.exists()
    assert sibling.read_text() == "keep"


def test_rmdir_clears_stale_cached_children(rig: CloudProviderTestRig, monkeypatch):
    directory = rig.create_cloud_path("empty-directory/")
    child = directory / "file.txt"
    child.write_text("stale")
    child.client._remove(child)
    assert child._local.exists()
    # Object stores need not retain an empty directory; some mocks cannot list it.
    monkeypatch.setattr(directory, "iterdir", lambda: iter(()))

    directory.rmdir()

    assert not directory._local.exists()


@pytest.mark.parametrize("method", ["unlink", "rename"])
def test_failed_mutation_preserves_source_cache(rig: CloudProviderTestRig, method, monkeypatch):
    cp = rig.create_cloud_path("failed-mutation.txt")
    cp.write_text("preserved")

    def fail(*args, **kwargs):
        raise RuntimeError("simulated cloud failure")

    if method == "unlink":
        monkeypatch.setattr(cp.client, "_remove", fail)
        args = ()
    else:
        monkeypatch.setattr(cp.client, "_move_file", fail)
        args = (rig.create_cloud_path("failed-move-target.txt"),)

    with pytest.raises(RuntimeError, match="simulated cloud failure"):
        getattr(cp, method)(*args)

    assert cp.exists()
    assert cp._local.read_text() == "preserved"


@pytest.mark.parametrize("method", ["move", "move_into"])
def test_native_move_invalidates_both_caches(rig: CloudProviderTestRig, method):
    source = rig.create_cloud_path("native-move-source.txt")
    target = rig.create_cloud_path(
        "native-move-target/" if method == "move_into" else "native-move-target.txt"
    )
    destination = target / source.name if method == "move_into" else target
    source.write_text("source content")
    destination.write_text("old target content")
    # A stale destination cache must be invalidated even if its remote file is gone.
    destination.client._remove(destination)
    assert source.client is destination.client
    assert source._local.exists()
    assert destination._local.exists()

    result = getattr(source, method)(target, force_overwrite_to_cloud=True)

    assert result == destination
    assert not source.exists()
    assert not source._local.exists()
    assert not destination._local.exists()
    assert result.read_text() == "source content"
    with source.open("x") as handle:
        handle.write("recreated")
    assert source.read_text() == "recreated"


@pytest.mark.parametrize("mode", ["x", "a"])
def test_external_deletion_discards_clean_cache(rig: CloudProviderTestRig, mode):
    cp = rig.create_cloud_path("externally-deleted.txt")
    cp.write_text("old content")
    cp.client._remove(cp)
    assert cp._local.exists()

    with cp.open(mode) as handle:
        handle.write("new content")

    assert cp.read_text() == "new content"


@pytest.mark.parametrize("existed", [False, True])
def test_unlink_preserves_only_missing_dirty_cache(
    rig: CloudProviderTestRig, existed, monkeypatch
):
    cp = rig.create_cloud_path("dirty-unlink.txt")
    if existed:
        cp.write_text("cloud content")

    def fail_upload(*args, **kwargs):
        raise RuntimeError("simulated upload failure")

    with monkeypatch.context() as patch:
        patch.setattr(cp.client, "_upload_file", fail_upload)
        with pytest.raises(RuntimeError, match="simulated upload failure"):
            cp.write_text("pending local content")
    assert cp._dirty
    # The failed upload closed the buffer; avoid retrying it during fixture cleanup.
    assert cp._handle.closed
    cp._handle = None

    cp.unlink()

    assert not cp.exists()
    if existed:
        assert not cp._local.exists()
    else:
        assert cp._local.read_text() == "pending local content"


def test_refresh_preserves_missing_dirty_cache(rig: CloudProviderTestRig, monkeypatch):
    cp = rig.create_cloud_path("dirty-refresh.txt")

    def fail_upload(*args, **kwargs):
        raise RuntimeError("simulated upload failure")

    with monkeypatch.context() as patch:
        patch.setattr(cp.client, "_upload_file", fail_upload)
        with pytest.raises(RuntimeError, match="simulated upload failure"):
            cp.write_text("pending local content")
    assert cp._dirty
    # The failed upload closed the buffer; avoid retrying it during fixture cleanup.
    assert cp._handle.closed
    cp._handle = None

    cp._refresh_cache()

    assert cp._local.read_text() == "pending local content"


def test_refresh_preserves_directory_cache(rig: CloudProviderTestRig, monkeypatch):
    directory = rig.create_cloud_path("refresh-directory/")
    child = directory / "file.txt"
    child.write_text("keep cached child")

    def no_stat():
        raise NoStatError("directory has no file stat")

    monkeypatch.setattr(directory, "stat", no_stat)
    directory._refresh_cache()

    assert child._local.read_text() == "keep cached child"


@pytest.mark.parametrize("method", ["rename", "replace"])
def test_adls_directory_move_invalidates_nested_caches(azure_gen2_rig, method):
    source = azure_gen2_rig.create_cloud_path("cached-directory")
    source.mkdir()
    (source / "nested").mkdir()
    child = source / "nested" / "file.txt"
    child.write_text("nested source content")
    target = azure_gen2_rig.create_cloud_path("renamed-directory")
    target._local.mkdir(parents=True, exist_ok=True)
    (target._local / "stale.txt").write_text("stale destination cache")
    assert child._local.exists()

    result = getattr(source, method)(target)

    assert result == target
    assert not source.exists()
    assert not source._local.exists()
    assert not target._local.exists()
    assert (target / "nested" / "file.txt").read_text() == "nested source content"


@pytest.mark.parametrize("method", ["move", "move_into"])
def test_failed_native_move_preserves_both_caches(rig: CloudProviderTestRig, method, monkeypatch):
    source = rig.create_cloud_path("failed-native-source.txt")
    target = rig.create_cloud_path(
        "failed-native-target/" if method == "move_into" else "failed-native-target.txt"
    )
    destination = target / source.name if method == "move_into" else target
    source.write_text("source content")
    destination.write_text("target content")

    def fail(*args, **kwargs):
        raise RuntimeError("simulated move failure")

    monkeypatch.setattr(source.client, "_move_file", fail)
    with pytest.raises(RuntimeError, match="simulated move failure"):
        getattr(source, method)(target, force_overwrite_to_cloud=True)

    assert source.exists()
    assert destination.exists()
    assert source._local.read_text() == "source content"
    assert destination._local.read_text() == "target content"


def test_failed_adls_directory_move_preserves_caches(azure_gen2_rig, monkeypatch):
    source = azure_gen2_rig.create_cloud_path("failed-directory")
    source.mkdir()
    child = source / "file.txt"
    child.write_text("cached source content")
    target = azure_gen2_rig.create_cloud_path("failed-directory-target")
    target._local.mkdir(parents=True, exist_ok=True)
    stale_target = target._local / "stale.txt"
    stale_target.write_text("cached target content")

    def fail(*args, **kwargs):
        raise RuntimeError("simulated directory move failure")

    monkeypatch.setattr(source.client, "_move_file", fail)
    with pytest.raises(RuntimeError, match="simulated directory move failure"):
        source.rename(target)

    assert source.exists()
    assert child._local.read_text() == "cached source content"
    assert stale_target.read_text() == "cached target content"


@pytest.mark.parametrize("operation", ["refresh", "unlink"])
def test_clean_alias_preserves_pending_cache(rig: CloudProviderTestRig, operation, monkeypatch):
    owner = rig.create_cloud_path("aliased-pending.txt")

    def fail_upload(*args, **kwargs):
        raise RuntimeError("simulated upload failure")

    with monkeypatch.context() as patch:
        patch.setattr(owner.client, "_upload_file", fail_upload)
        with pytest.raises(RuntimeError, match="simulated upload failure"):
            owner.write_text("pending local content")
    assert owner._dirty
    assert owner._handle.closed
    owner._handle = None
    alias = rig.create_cloud_path("aliased-pending.txt", client=owner.client)
    assert alias is not owner
    assert not alias._dirty
    assert alias._local == owner._local

    if operation == "refresh":
        alias._refresh_cache()
    else:
        alias.unlink()

    assert owner._local.read_text() == "pending local content"
    # The original owner can still retry the failed upload successfully.
    owner._upload_local_to_cloud()
    assert owner.read_text() == "pending local content"


@pytest.mark.parametrize("cleanup", ["path", "client"])
def test_explicit_cache_cleanup_releases_pending_tracking(
    rig: CloudProviderTestRig, cleanup, monkeypatch
):
    owner = rig.create_cloud_path("clear-pending-cache.txt")

    def fail_upload(*args, **kwargs):
        raise RuntimeError("simulated upload failure")

    with monkeypatch.context() as patch:
        patch.setattr(owner.client, "_upload_file", fail_upload)
        with pytest.raises(RuntimeError, match="simulated upload failure"):
            owner.write_text("pending local content")
    assert owner._handle.closed
    owner._handle = None
    alias = rig.create_cloud_path("clear-pending-cache.txt", client=owner.client)
    assert alias._cache_is_dirty()

    if cleanup == "path":
        alias.clear_cache()
    else:
        alias.client.clear_cache()

    assert not alias._cache_is_dirty()
    with alias.open("x") as handle:
        handle.write("fresh content")
    assert not alias._cache_is_dirty()
    assert alias.read_text() == "fresh content"


def test_upload_keeps_other_writer_protection(rig: CloudProviderTestRig, monkeypatch):
    owners = [rig.create_cloud_path("multiple-pending.txt") for _ in range(2)]

    def fail_upload(*args, **kwargs):
        raise RuntimeError("simulated upload failure")

    for index, owner in enumerate(owners):
        with monkeypatch.context() as patch:
            patch.setattr(owner.client, "_upload_file", fail_upload)
            with pytest.raises(RuntimeError, match="simulated upload failure"):
                owner.write_text(f"pending {index}")
        assert owner._handle.closed
        owner._handle = None
    alias = rig.create_cloud_path("multiple-pending.txt", client=owners[0].client)

    owners[0]._upload_local_to_cloud()
    alias.client._remove(alias)
    alias._refresh_cache()
    assert alias._local.read_text() == "pending 1"

    owners[1]._upload_local_to_cloud()
    alias.client._remove(alias)
    alias._refresh_cache()
    assert not alias._local.exists()


@pytest.mark.parametrize("method", ["rename", "replace", "move", "move_into"])
@pytest.mark.parametrize("pending_side", ["source", "destination"])
def test_move_rejects_pending_cached_writes(
    rig: CloudProviderTestRig, method, pending_side, monkeypatch
):
    source = rig.create_cloud_path("pending-move-source.txt")
    target = rig.create_cloud_path(
        "pending-move-target/" if method == "move_into" else "pending-move-target.txt"
    )
    destination = target / source.name if method == "move_into" else target
    source.write_text("cloud source")
    destination.write_text("cloud destination")
    owner_key = (
        "pending-move-source.txt"
        if pending_side == "source"
        else (
            "pending-move-target/pending-move-source.txt"
            if method == "move_into"
            else "pending-move-target.txt"
        )
    )
    owner = rig.create_cloud_path(owner_key, client=source.client)

    def fail_upload(*args, **kwargs):
        raise RuntimeError("simulated upload failure")

    with monkeypatch.context() as patch:
        patch.setattr(owner.client, "_upload_file", fail_upload)
        with pytest.raises(RuntimeError, match="simulated upload failure"):
            owner.write_text("pending local changes")
    assert owner._handle.closed
    owner._handle = None
    kwargs = {"force_overwrite_to_cloud": True} if method in ("move", "move_into") else {}

    with pytest.raises(OverwriteDirtyFileError, match="pending writes"):
        getattr(source, method)(target, **kwargs)

    assert source.exists()
    assert destination.exists()
    assert owner._local.read_text() == "pending local changes"
    owner._upload_local_to_cloud(force_overwrite_to_cloud=True)
    assert owner.read_text() == "pending local changes"


@pytest.mark.parametrize("pending_side", ["source", "destination"])
def test_adls_directory_move_rejects_pending_descendants(
    azure_gen2_rig, pending_side, monkeypatch
):
    source = azure_gen2_rig.create_cloud_path("pending-directory-source")
    target = azure_gen2_rig.create_cloud_path("pending-directory-target")
    source.mkdir()
    target.mkdir()
    owner = (source if pending_side == "source" else target) / "file.txt"
    owner.write_text("cloud content")

    def fail_upload(*args, **kwargs):
        raise RuntimeError("simulated upload failure")

    with monkeypatch.context() as patch:
        patch.setattr(owner.client, "_upload_file", fail_upload)
        with pytest.raises(RuntimeError, match="simulated upload failure"):
            owner.write_text("pending directory content")
    assert owner._handle.closed
    owner._handle = None

    with pytest.raises(OverwriteDirtyFileError, match="pending writes"):
        source.rename(target)

    assert source.exists()
    assert target.exists()
    assert owner._local.read_text() == "pending directory content"


def test_adls_directory_self_rename_preserves_cache(azure_gen2_rig):
    directory = azure_gen2_rig.create_cloud_path("same-directory")
    directory.mkdir()
    child = directory / "file.txt"
    child.write_text("cached contents")

    assert directory.rename(directory) == directory
    assert child._local.read_text() == "cached contents"


@pytest.mark.parametrize("cleanup", ["path", "client"])
def test_original_writer_can_move_after_explicit_cleanup(
    rig: CloudProviderTestRig, cleanup, monkeypatch
):
    source = rig.create_cloud_path("cleanup-then-move.txt")
    source.write_text("cloud content")

    def fail_upload(*args, **kwargs):
        raise RuntimeError("simulated upload failure")

    with monkeypatch.context() as patch:
        patch.setattr(source.client, "_upload_file", fail_upload)
        with pytest.raises(RuntimeError, match="simulated upload failure"):
            source.write_text("discarded pending content")
    assert source._handle.closed

    if cleanup == "path":
        source.clear_cache()
    else:
        source.client.clear_cache()
    # A retry of the closed handle must not re-upload explicitly discarded data.
    source._handle.close()
    target = rig.create_cloud_path("cleanup-move-target.txt")
    result = source.rename(target)

    assert result == target
    assert not source.exists()
    assert target.read_text() == "cloud content"
