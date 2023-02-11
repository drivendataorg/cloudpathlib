import builtins
import os
import os.path

from .cloudpath import CloudPath


def _check_first_arg(*args, **kwargs):
    return isinstance(args[0], CloudPath)


def _check_first_arg_first_index(*args, **kwargs):
    return isinstance(args[0][0], CloudPath)


def _patch_factory(original_version, cpl_version, cpl_check=_check_first_arg):
    _original = original_version

    def _patched_version(*args, **kwargs):
        if cpl_check(*args, **kwargs):
            return cpl_version(*args, **kwargs)
        else:
            return _original(*args, **kwargs)

    original_version = _patched_version
    return _patched_version


def patch_open():
    patched = _patch_factory(
        builtins.open,
        CloudPath.open,
    )
    builtins.open = patched
    return patched


def _cloudpath_os_listdir(path="."):
    return list(path.iterdir())


def _cloudpath_lstat(path, *, dir_fd=None):
    return path.stat()


def _cloudpath_mkdir(path, *, dir_fd=None):
    return path.mkdir()


def _cloudpath_os_makedirs(name, mode=0o777, exist_ok=False):
    return CloudPath.mkdir(name, parents=True, exist_ok=exist_ok)


def _cloudpath_os_remove(path, *, dir_fd=None):
    return path.unlink()


def _cloudpath_os_removedirs(name):
    for d in name.parents:
        d.rmdir()


def _cloudpath_os_rename(src, dst, *, src_dir_fd=None, dst_dir_fd=None):
    return src.rename(dst)


def _cloudpath_os_renames(old, new):
    old.rename(new)  # move file
    _cloudpath_os_removedirs(old)  # remove previous directories if empty


def _cloudpath_os_replace(src, dst, *, src_dir_fd=None, dst_dir_fd=None):
    return src.rename(dst)


def _cloudpath_os_rmdir(path, *, dir_fd=None):
    return path.rmdir()


def _cloudpath_os_scandir(path="."):
    return path.iterdir()


def _cloudpath_os_stat(path, *, dir_fd=None, follow_symlinks=True):
    return path.stat()


def _cloudpath_os_unlink(path, *, dir_fd=None):
    return path.unlink()


def _cloudpath_os_walk(top, topdown=True, onerror=None, followlinks=False):
    try:
        dirs, files = [], []
        for p in top.iterdir():
            dirs.append(p) if p.is_dir() else files.append(p)

        if topdown:
            yield (top, files, dirs)

        for d in dirs:
            yield from _cloudpath_os_walk(d, topdown=topdown, onerror=onerror)

        if not topdown:
            yield (top, files, dirs)

    except Exception as e:
        if onerror is not None:
            onerror(e)
        else:
            raise


def _cloudpath_os_path_basename(path):
    return path.name


def __common(parts):
    i = 0

    try:
        while all(item[i] == parts[0][i] for item in parts[1:]):
            i += 1
    except IndexError:
        pass

    return parts[0][:i]


def _cloudpath_os_path_commonpath(paths):
    common = __common([p.parts for p in paths])
    return paths[0].client.CloudPath(*common)


def _cloudpath_os_path_commonprefix(list):
    common = __common([str(p) for p in list])
    return common


def _cloudpath_os_path_dirname(path):
    return path.parent


def _cloudpath_os_path_getatime(path):
    return (path.stat().st_atime,)


def _cloudpath_os_path_getmtime(path):
    return (path.stat().st_mtime,)


def _cloudpath_os_path_getctime(path):
    return (path.stat().st_ctime,)


def _cloudpath_os_path_getsize(path):
    return (path.stat().st_size,)


def _cloudpath_os_path_join(path, *paths):
    for p in paths:
        path /= p
    return path


def _cloudpath_os_path_split(path):
    return path.parent, path.name


def _cloudpath_os_path_splitext(path):
    return str(path)[: -len(path.suffix)], path.suffix


def patch_os_functions():
    os.listdir = _patch_factory(os.listdir, _cloudpath_os_listdir)
    os.lstat = _patch_factory(os.lstat, _cloudpath_lstat)
    os.mkdir = _patch_factory(os.mkdir, _cloudpath_mkdir)
    os.makedirs = _patch_factory(os.makedirs, _cloudpath_os_makedirs)
    os.remove = _patch_factory(os.remove, _cloudpath_os_remove)
    os.removedirs = _patch_factory(os.removedirs, _cloudpath_os_removedirs)
    os.rename = _patch_factory(os.rename, _cloudpath_os_rename)
    os.renames = _patch_factory(os.renames, _cloudpath_os_renames)
    os.replace = _patch_factory(os.replace, _cloudpath_os_replace)
    os.rmdir = _patch_factory(os.rmdir, _cloudpath_os_rmdir)
    os.scandir = _patch_factory(os.scandir, _cloudpath_os_scandir)
    os.stat = _patch_factory(os.stat, _cloudpath_os_stat)
    os.unlink = _patch_factory(os.unlink, _cloudpath_os_unlink)
    os.walk = _patch_factory(os.walk, _cloudpath_os_walk)

    os.path.basename = _patch_factory(os.path.basename, _cloudpath_os_path_basename)
    os.path.commonpath = _patch_factory(
        os.path.commonpath, _cloudpath_os_path_commonpath, cpl_check=_check_first_arg_first_index
    )
    os.path.commonprefix = _patch_factory(
        os.path.commonprefix,
        _cloudpath_os_path_commonprefix,
        cpl_check=_check_first_arg_first_index,
    )
    os.path.dirname = _patch_factory(os.path.dirname, _cloudpath_os_path_dirname)
    os.path.exists = _patch_factory(os.path.exists, CloudPath.exists)
    os.path.getatime = _patch_factory(os.path.getatime, _cloudpath_os_path_getatime)
    os.path.getmtime = _patch_factory(os.path.getmtime, _cloudpath_os_path_getmtime)
    os.path.getctime = _patch_factory(os.path.getctime, _cloudpath_os_path_getctime)
    os.path.getsize = _patch_factory(os.path.getsize, _cloudpath_os_path_getsize)
    os.path.isfile = _patch_factory(os.path.isfile, CloudPath.is_file)
    os.path.isdir = _patch_factory(os.path.isdir, CloudPath.is_dir)
    os.path.join = _patch_factory(os.path.join, _cloudpath_os_path_join)
    os.path.split = _patch_factory(os.path.split, _cloudpath_os_path_split)
    os.path.splitext = _patch_factory(os.path.splitext, _cloudpath_os_path_splitext)
