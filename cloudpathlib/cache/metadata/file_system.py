import dbm


from cloudpathlib.cache.base import MetadataCache


class FileSystemMetadataCache(MetadataCache):
    def __init__(self, cache_path: str):
        self.path = cache_path
        self.db = dbm.open(self.path, "c")

    def get(self, key: str) -> Dict[str, Any]:
        return self.db[key]

    def put(self, key: str, value: Dict[str, Any]) -> None:
        self.db[key] = value