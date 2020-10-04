from cloudpathlib import AzureBlobPath


def test_azureblobpath_properties(azure_rig):
    p = AzureBlobPath(f"az://{azure_rig.drive}")
    assert p.blob == ""
    assert p.container == azure_rig.drive

    p2 = AzureBlobPath(f"az://{azure_rig.drive}/")
    assert p2.blob == ""
    assert p2.container == azure_rig.drive
