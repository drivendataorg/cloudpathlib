import pickle

from cloudpathlib import CloudPath


def test_pickle_roundtrip():
    path1 = CloudPath("s3://bucket/key")
    pkl1 = pickle.dumps(path1)

    path2 = pickle.loads(pkl1)
    pkl2 = pickle.dumps(path2)

    assert path1 == path2
    assert pkl1 == pkl2
