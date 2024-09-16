from tests.conftest import CloudProviderTestRig


def test_https(https_rig: CloudProviderTestRig):
    """Basic tests for https; we run the full suite against the http_rig"""
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
