import os
from cloudpathlib import CloudPath, patch_open, patch_os_functions


def hello(cp):
    with open(cp, "a") as f:
        f.write(" written")


if __name__ == "__main__":
    patch_open()

    cp = CloudPath("s3://cloudpathlib-test-bucket/manual/text_file.txt")
    cp.write_text("yah")

    hello(cp)

    print(cp.read_text())
    cp.unlink()

    patch_os_functions()

    print(list(os.walk(".")))
    print(list(cp.parent.client._list_dir(cp.parent, recursive=True)))
    print(list(os.walk(cp.parent)))
