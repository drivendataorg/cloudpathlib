from cloudpathlib import GSPath


def test_gspath_properties(gs_rig):
    p = GSPath(f"gs://{gs_rig.drive}")
    assert p.blob == ""
    assert p.bucket == gs_rig.drive

    p2 = GSPath(f"gs://{gs_rig.drive}/")
    assert p2.blob == ""
    assert p2.bucket == gs_rig.drive
