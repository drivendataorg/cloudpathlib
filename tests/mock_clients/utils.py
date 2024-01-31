from pathlib import Path


def delete_empty_parents_up_to_root(path: Path, root: Path):
    for parent in path.parents:
        if parent == root:
            return
        try:
            next(parent.iterdir())
            return
        except StopIteration:
            parent.rmdir()
