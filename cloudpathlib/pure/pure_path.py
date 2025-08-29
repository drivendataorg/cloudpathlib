from cloudpathlib import CloudPath
from cloudpathlib.cloudpath import CloudImplementation
from cloudpathlib.client import Client


class DummyClient(Client):
    _error_message = "PureCloudPath does not support calls that require a client."
    
    def __init__(self):
        pass

    def __del__(self):
        # override as no-op
        pass

    def __getattr__(self, item):
        raise NotImplementedError(self._error_message)

    def _download_file(self, cloud_path, local_path):
        raise NotImplementedError(self._error_message)
    
    def _exists(self, cloud_path):
        raise NotImplementedError(self._error_message)
    
    def _generate_presigned_url(self, cloud_path, expire_seconds=3600):
        raise NotImplementedError(self._error_message)
    
    def _get_public_url(self, cloud_path):
        raise NotImplementedError(self._error_message)
    
    def _list_dir(self, cloud_path, recursive):
        raise NotImplementedError(self._error_message)
    
    def _move_file(self, src, dst, remove_src=True):
        raise NotImplementedError(self._error_message)
    
    def _remove(self, path, missing_ok=True):
        raise NotImplementedError(self._error_message)
    
    def _upload_file(self, local_path, cloud_path):
        raise NotImplementedError(self._error_message)
    

class PureCloudPath(CloudPath):
    cloud_prefix = ""  # instantiated on init and never changed
    
    def __init__(self, path: str):
        self.cloud_prefix = path.split("://", 1)[0] + "://"
        self._client = DummyClient()
        super().__init__(path)

    @property
    def drive(self):
        return self.parts[1]  # first after anchor
    
    def mkdir(self, parents: bool = False, exist_ok: bool = False):
        return self.client.mkdir(self, parents, exist_ok)
    
    def touch(self, exist_ok: bool = True):
        return self.client.touch(self, exist_ok)
    

pure_cloud_meta = CloudImplementation()
pure_cloud_meta.name = "pure"
pure_cloud_meta._client_class = DummyClient
pure_cloud_meta._path_class = PureCloudPath

PureCloudPath._cloud_meta = pure_cloud_meta

DummyClient.CloudPath = PureCloudPath