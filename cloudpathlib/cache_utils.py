from pathlib import Path
import shutil


def _clear_cache(_local: Path) -> None:
    if _local.exists():
        if _local.is_file():
            _local.unlink()
        else:
            shutil.rmtree(_local)
