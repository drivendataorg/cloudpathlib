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
