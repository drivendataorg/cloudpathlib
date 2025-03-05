import urllib

from tests.conftest import CloudProviderTestRig


def test_https(https_rig: CloudProviderTestRig):
    """Basic tests for https"""
    existing_file = https_rig.create_cloud_path("dir_0/file0_0.txt")

    # existence and listing
    assert existing_file.exists()
    assert existing_file.parent.exists()
    assert existing_file.name in [f.name for f in existing_file.parent.iterdir()]

    # root level checks
    root = list(existing_file.parents)[-1]
    assert root.exists()
    assert len(list(root.iterdir())) > 0

    # reading and wrirting
    existing_file.write_text("Hello from 0")
    assert existing_file.read_text() == "Hello from 0"

    # creating new files
    not_existing_file = https_rig.create_cloud_path("dir_0/new_file.txt")

    assert not not_existing_file.exists()

    not_existing_file.upload_from(existing_file)

    assert not_existing_file.read_text() == "Hello from 0"

    # deleteing
    not_existing_file.unlink()
    assert not not_existing_file.exists()

    # metadata
    assert existing_file.stat().st_mtime != 0


def test_http_verbs(http_like_rig: CloudProviderTestRig):
    """Test that the http verbs work"""
    p = http_like_rig.create_cloud_path("dir_0/file0_0.txt")

    # test put
    p.put(data="Hello from 0".encode("utf-8"), headers={"Content-Type": "text/plain"})

    # test get
    resp, data = p.get()
    assert resp.status == 200
    assert data.decode("utf-8") == "Hello from 0"

    # post
    import json

    post_payload = {"key": "value"}
    resp, data = p.post(
        data=json.dumps(post_payload).encode(), headers={"Content-Type": "application/json"}
    )
    assert resp.status == 200
    assert json.loads(data.decode("utf-8")) == post_payload

    # head
    resp, data = p.head()
    assert resp.status == 200
    assert data == b""

    # delete
    p.delete()
    assert not p.exists()


def test_http_parsed_url(http_like_rig: CloudProviderTestRig):
    """Test that the parsed_url property works"""
    p = http_like_rig.create_cloud_path("dir_0/file0_0.txt")
    assert p.parsed_url.scheme == http_like_rig.cloud_prefix.split("://")[0]
    assert p.parsed_url.netloc == http_like_rig.drive
    assert p.parsed_url.path == str(p).split(http_like_rig.drive)[1]


def test_http_url_decorations(http_like_rig: CloudProviderTestRig):
    def _test_preserved_properties(base_url, returned_url):
        parsed_base = urllib.parse.urlparse(str(base_url))
        parsed_returned = urllib.parse.urlparse(str(returned_url))

        assert parsed_returned.scheme == parsed_base.scheme
        assert parsed_returned.netloc == parsed_base.netloc
        assert parsed_returned.username == parsed_base.username
        assert parsed_returned.password == parsed_base.password
        assert parsed_returned.hostname == parsed_base.hostname
        assert parsed_returned.port == parsed_base.port

    p = http_like_rig.create_cloud_path("dir_0/file0_0.txt")
    p.write_text("Hello!")

    # add some properties to the url
    new_url = p.parsed_url._replace(
        params="param=value", query="query=value&query2=value2", fragment="fragment-value"
    )
    p = http_like_rig.path_class(urllib.parse.urlunparse(new_url))

    # operations that should preserve properties of the original url and need to hit the server
    # glob, iterdir, walk
    _test_preserved_properties(p, next(p.parent.glob("*.txt")))
    _test_preserved_properties(p, next(p.parent.iterdir()))
    _test_preserved_properties(p, next(p.parent.walk())[0])

    # rename and replace?
    new_location = p.with_name("other_file.txt")
    _test_preserved_properties(p, p.rename(new_location))
    _test_preserved_properties(p, new_location.replace(p))

    # operations that should preserve properties of the original url and don't hit the server
    # so that we can add some other properties (e.g., username, password)
    new_url = p.parsed_url._replace(netloc="user:pass@example.com:8000")
    p = http_like_rig.path_class(urllib.parse.urlunparse(new_url))

    # parent
    _test_preserved_properties(p, p.parent)

    # joining / and joinpath
    _test_preserved_properties(p, p.parent / "other_file.txt")
    _test_preserved_properties(p, p.parent.joinpath("other_file.txt"))

    # with_name, with_suffix, with_stem
    _test_preserved_properties(p, p.with_name("other_file.txt"))
    _test_preserved_properties(p, p.with_suffix(".txt"))
    _test_preserved_properties(p, p.with_stem("other_file"))
