import pickle

from cloudpathlib import CloudPath


def test_pickle(rig, tmpdir):
    p = rig.create_cloud_path("dir_0/file0_0.txt")

    with (tmpdir / "test.pkl").open("wb") as f:
        pickle.dump(p, f)

    with (tmpdir / "test.pkl").open("rb") as f:
        pickled = pickle.load(f)

    # test a call to the network
    assert pickled.exists()

    # check we unpickled, and that client is the default client
    assert str(pickled) == str(p)
    assert pickled.client == p.client
    assert rig.client_class._default_client == pickled.client


def test_pickle_roundtrip():
    path1 = CloudPath("s3://bucket/key")
    pkl1 = pickle.dumps(path1)

    path2 = pickle.loads(pkl1)
    pkl2 = pickle.dumps(path2)

    assert path1 == path2
    assert pkl1 == pkl2
