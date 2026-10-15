from types import TracebackType

import pytest

from cloudpathlib.exceptions import CloudPathNotADirectoryError


@pytest.fixture(params=["is_file", "remove"])
def failed_rmtree(request, rig, monkeypatch):
    path = rig.create_cloud_path("dir_1/")
    error = RuntimeError("cloud operation failed")

    def fail(*args, **kwargs):
        raise error

    if request.param == "is_file":
        monkeypatch.setattr(path, "is_file", fail)
    else:
        monkeypatch.setattr(path.client, "_remove", fail)
    return path, error


@pytest.mark.parametrize("options", [{}, {"ignore_errors": False}])
def test_rmtree_propagates_errors_by_default(failed_rmtree, options):
    path, error = failed_rmtree
    with pytest.raises(RuntimeError) as exc:
        path.rmtree(**options)
    assert exc.value is error


def test_rmtree_ignore_errors(failed_rmtree):
    path, _ = failed_rmtree
    assert path.rmtree(ignore_errors=True) is None
    assert path.exists()


@pytest.mark.parametrize("handler", ["onerror", "onexc", "both", "ignore_errors"])
def test_rmtree_error_callbacks(failed_rmtree, handler):
    path, error = failed_rmtree
    calls = []

    def onerror(function, failed_path, exc_info):
        calls.append(("onerror", function, failed_path, exc_info))

    def onexc(function, failed_path, exception):
        calls.append(("onexc", function, failed_path, exception))

    options = {}
    if handler in ("onerror", "both", "ignore_errors"):
        options["onerror"] = onerror
    if handler in ("onexc", "both", "ignore_errors"):
        options["onexc"] = onexc
    if handler == "ignore_errors":
        options["ignore_errors"] = True

    assert path.rmtree(**options) is None
    if handler == "ignore_errors":
        assert calls == []
    else:
        assert len(calls) == 1
        name, function, failed_path, detail = calls[0]
        assert name == ("onerror" if handler == "onerror" else "onexc")
        assert function is type(path).rmtree
        assert failed_path is path
        if name == "onerror":
            assert detail[:2] == (RuntimeError, error)
            assert isinstance(detail[2], TracebackType)
        else:
            assert detail is error
    assert path.exists()


@pytest.mark.parametrize("handler", ["onerror", "onexc"])
def test_rmtree_callback_errors_propagate(failed_rmtree, handler):
    path, _ = failed_rmtree
    callback_error = ValueError("callback failed")

    def callback(*args):
        raise callback_error

    with pytest.raises(ValueError) as exc:
        path.rmtree(**{handler: callback})
    assert exc.value is callback_error


@pytest.mark.parametrize("error_type", [KeyboardInterrupt, SystemExit])
def test_rmtree_ignore_errors_does_not_suppress_control_flow(rig, monkeypatch, error_type):
    path = rig.create_cloud_path("dir_1/")
    error = error_type()

    def fail(*args):
        raise error

    monkeypatch.setattr(path.client, "_remove", fail)
    with pytest.raises(error_type) as exc:
        path.rmtree(ignore_errors=True)
    assert exc.value is error


@pytest.mark.parametrize("option", ["ignore_errors", "onerror", "onexc"])
def test_rmtree_success_with_error_handling(rig, option):
    path = rig.create_cloud_path("dir_1/")

    def unexpected_callback(*args):
        pytest.fail("The callback should not be called after a successful removal")

    options = {option: True if option == "ignore_errors" else unexpected_callback}
    assert path.rmtree(**options) is None
    assert not path.exists()


@pytest.mark.parametrize("ignore_errors", [False, True])
def test_rmtree_file_error(rig, ignore_errors):
    path = rig.create_cloud_path("dir_0/file0_0.txt")
    if ignore_errors:
        assert path.rmtree(ignore_errors=True) is None
    else:
        with pytest.raises(CloudPathNotADirectoryError):
            path.rmtree(ignore_errors=False)
    assert path.exists()


@pytest.mark.parametrize("handler", ["onerror", "onexc"])
def test_rmtree_callback_can_retry(rig, monkeypatch, handler):
    path = rig.create_cloud_path("dir_1/")
    remove = path.client._remove
    attempts = []

    def fail_once(failed_path):
        attempts.append(failed_path)
        if len(attempts) == 1:
            raise PermissionError("temporary removal failure")
        return remove(failed_path)

    def retry(function, failed_path, error):
        function(failed_path)

    monkeypatch.setattr(path.client, "_remove", fail_once)
    path.rmtree(**{handler: retry})
    assert attempts == [path, path]
    assert not path.exists()
