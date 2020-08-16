from ...cloudpath import CloudPath
from .s3backend import S3Backend

# FEATUREDS
#   - div left and div right
#   - local cache


class S3Path(CloudPath):
    backend_class = S3Backend
    path_prefix = "s3://"

    @property
    def bucket(self):
        return ""

    @property
    def key(self):
        return ""

    @property
    def etag(self):
        return ""
