""" Cloud implementations of shutil functions. """
import os
import shutil
from pathlib import Path
from cloudpathlib import CloudPath


def _local_to_cloud_copy(src: Path, dst: CloudPath):
    if dst.is_dir():
        dst.client._upload_file(local_path=src, cloud_path=dst / src.name)
    else:
        dst.client._upload_file(local_path=src, cloud_path=dst)


def _cloud_to_cloud_copy(src: CloudPath, dst: CloudPath):
    temp_dir = src.client._local_cache_dir
    src.download_to(temp_dir)
    if dst.is_dir():
        dst.client._upload_file(local_path=temp_dir / src.name, cloud_path=dst / src.name)
    else:
        dst.client._upload_file(local_path=temp_dir / src.name, cloud_path=dst)
    (temp_dir / src.name).unlink()


def copy(src: os.PathLike, dst: os.PathLike, *, follow_symlinks: bool = True):
    """
    Cloud implementation of shutil.copy.

    Parameters
    ----------
    src : os.PathLike
        Source file.
    dst : os.PathLike
        Destination file or folder. If folder, the source file will be copied with its name.
    follow_symlinks: bool, default = True
        Passed only if both source and destination are local, in which case shutil.copy is used.
    """
    if not src.is_file():
        raise ValueError('src must be a file.')
    if isinstance(src, CloudPath) & isinstance(dst, CloudPath):
        _cloud_to_cloud_copy(src, dst)
    elif isinstance(src, Path) & isinstance(dst, CloudPath):
        _local_to_cloud_copy(src, dst)
    elif isinstance(src, CloudPath) & isinstance(dst, Path):
        src.download_to(dst)
    elif isinstance(src, Path) & isinstance(dst, Path):
        shutil.copy(src, dst, follow_symlinks=follow_symlinks)
    else:
        raise TypeError(f'Types {type(src)} and {type(src)} not valid, they must be os.PathLike.')
