""" Cloud implementations of shutil functions. """
import shutil
from os import PathLike
from pathlib import Path
from cloudpathlib import CloudPath


def _local_to_cloud_copy(src: Path, dst: CloudPath):
    if dst.is_dir():
        dst.client._upload_file(local_path=src, cloud_path=dst / src.name)
    else:
        dst.client._upload_file(local_path=src, cloud_path=dst)


def _cloud_to_cloud_copy(src: CloudPath, dst: CloudPath):
    temp_file = src._local
    temp_file.parent.mkdir(parents=True)
    src.download_to(temp_file)
    _local_to_cloud_copy(temp_file, dst)


def copy(src: PathLike, dst: PathLike, *, follow_symlinks: bool = True):
    """
    Cloud implementation of shutil.copy.

    Parameters
    ----------
    src : PathLike
        Source file.
    dst : PathLike
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
        raise TypeError(f'Types {type(src)} and {type(src)} not valid, they must be PathLike.')


def copytree(src: PathLike, dst: PathLike):
    """
    Cloud implementation of shutil.copy.

    Parameters
    ----------
    src : PathLike
        Source folder.
    dst : PathLike
        Destination folder.
    """
    if not src.is_dir():
        raise ValueError('src must be a directory. To copy a file use cloupathlib.copy.')
    if not dst.is_dir():
        raise ValueError('dst of copytree must be a directory.')
    for subpath in src.iterdir():
        if subpath.is_file():
            copy(subpath, dst / subpath.name)
        elif subpath.is_dir():
            copytree(subpath, dst / subpath.name)
