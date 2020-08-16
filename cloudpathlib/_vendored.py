## Vendored functions
##
## There are a few functions that we vendor the bulk of the logic from the
## pathlib source since they are not available on the objects that we use; :'(
##   

# The function resolve is not available on Pure paths because it removes relative
# paths and symlinks. We _just_ want the relative path resolution for
# cloud paths, so the other logic is removed.  Also, we can assume that
# cloud paths are absolute.
#
# Left as much as possible unchanged
#
# Adapted from:
# https://github.com/python/cpython/blob/3.8/Lib/pathlib.py#L316-L359
def resolve(path, strict=False):
    # sep = self.sep
    sep = "/"
    # accessor = path._accessor
    # seen = {}

    def _resolve(path, rest):
        if rest.startswith(sep):
            path = ''

        for name in rest.split(sep):
            if not name or name == '.':
                # current dir
                continue
            if name == '..':
                # parent dir
                path, _, _ = path.rpartition(sep)
                continue
            newpath = path + sep + name
            # if newpath in seen:
            #     # Already seen this path
            #     path = seen[newpath]
            #     if path is not None:
            #         # use cached value
            #         continue
            #     # The symlink is not resolved, so we must have a symlink loop.
            #     raise RuntimeError("Symlink loop from %r" % newpath)
            # # Resolve the symbolic link
            # try:
            #     target = accessor.readlink(newpath)
            # except OSError as e:
            #     if e.errno != EINVAL and strict:
            #         raise
            #     # Not a symlink, or non-strict mode. We just leave the path
            #     # untouched.
            path = newpath
            # else:
            #     seen[newpath] = None # not resolved symlink
            #      path = _resolve(path, newpath)
            #     seen[newpath] = path # resolved symlink

        return path

    # NOTE: according to POSIX, getcwd() cannot contain path components
    # which are symlinks.
    return _resolve('', str(path)) or sep
