from cloudpathlib import CloudPath


def test_default_client_instantiation(rig):
    rig.client_class._default_client = None

    # CloudPath dispatch
    p = CloudPath(f"{rig.cloud_prefix}{rig.drive}/{rig.test_dir}/dir_0/file0_0.txt")
    # Explicit class
    p2 = rig.create_cloud_path("dir_0/file0_0.txt")
    # Default client CloudPath constructor
    p3 = rig.client_class.get_default_client().CloudPath(
        f"{rig.cloud_prefix}{rig.drive}/{rig.test_dir}/dir_0/file0_0.txt"
    )
    # Default client path-class-name constructor
    p4 = getattr(rig.client_class.get_default_client(), rig.path_class.__name__)(
        f"{rig.cloud_prefix}{rig.drive}/{rig.test_dir}/dir_0/file0_0.txt"
    )

    # Check that paths are the same
    assert p == p2 == p3 == p4

    # Check that client is the same instance
    assert p.client is p2.client is p3.client is p4.client

    # Check the file content is the same
    assert p.read_bytes() == p2.read_bytes() == p3.read_bytes() == p4.read_bytes()

    # should be using same instance of client, so cache should be the same
    assert p._local == p2._local == p3._local == p4._local


def test_different_clients(rig):
    p = rig.create_cloud_path("dir_0/file0_0.txt")

    new_client = rig.client_class()
    p2 = new_client.CloudPath(f"{rig.cloud_prefix}{rig.drive}/{rig.test_dir}/dir_0/file0_0.txt")

    assert p.client is not p2.client
    assert p._local is not p2._local
