from cloudpathlib.cache.base import FileCache


class FileSystemCache(FileCache):
    def __init__(
            self,
            cache_path: str,
            validate: Callable[[Any], bool] = ,
            max_size_btyes: Optional[int] = None
        ):
        self.path = path

    def get(self, key: Any) -> Any:
        pass

    def put(self, key: Any, value: Any) -> None: