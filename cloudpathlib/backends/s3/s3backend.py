from boto3.session import Session

from ..base import Backend
import cloudpathlib


class S3Backend(Backend):
    path_class = cloudpathlib.S3Path

    def __init__(self, aws_secret_access_key=None, aws_region=None, aws_profile=None):
        self.sess = Session()
        self.s3 = self.sess.resource("s3")
        self.client = self.sess.client("s3")

    def get_metadata(self, cloud_path):
        data = self.s3.ObjectSummary(cloud_path.bucket, cloud_path.key,).get()

        return {
            "last_modified": data["LastModified"],
            "size": data["ContentLength"],
            "etag": data["ETag"],
            "mime": data["ContentType"],
            "extra": data["Metadata"],
        }

    def download_file(self, cloud_path, local_path):
        obj = self.s3.Object(cloud_path.bucket, cloud_path.key,)

        obj.download_file(str(local_path))
        return local_path

    def is_file_or_dir(self, cloud_path):
        # short-circuit the root-level bucket
        if not cloud_path.key:
            return "dir"

        try:
            obj = self.s3.ObjectSummary(cloud_path.bucket, cloud_path.key,)
            obj.get()
            return "file"
        except self.client.exceptions.NoSuchKey:
            prefix = cloud_path.key
            if prefix and not prefix.endswith("/"):
                prefix += "/"

            # not a file, see if it is a directory
            f = self.s3.Bucket(cloud_path.bucket,).objects.filter(Prefix=prefix)

            # at least one key with the prefix of the directory
            if bool([_ for _ in f.limit(1)]):
                return "dir"
            else:
                return None

    def exists(self, cloud_path):
        return self.is_file_or_dir(cloud_path) in ["file", "dir"]

    def list_dir(self, cloud_path, recursive=False):
        bucket = self.s3.Bucket(cloud_path.bucket)

        prefix = cloud_path.key
        if prefix and not prefix.endswith("/"):
            prefix += "/"

        if recursive:
            for o in bucket.objects.filter(Prefix=prefix):
                yield self.path_class(
                    f"s3://{o.bucket_name}/{o.key}",
                    backend=self,
                    local_cache_dir=cloud_path._local_cache_dir,
                )
        else:
            # non recursive is best done with old client API rather than resource
            paginator = self.client.get_paginator("list_objects")

            for result in paginator.paginate(
                Bucket=cloud_path.bucket, Prefix=prefix, Delimiter="/"
            ):

                # sub directory names
                for prefix in result.get("CommonPrefixes", []):
                    yield self.path_class(
                        f"s3://{cloud_path.bucket}/{prefix.get('Prefix')}",
                        backend=self,
                        local_cache_dir=cloud_path._local_cache_dir,
                    )

                # files in the directory
                for key in result.get("Contents", []):
                    yield self.path_class(
                        f"s3://{cloud_path.bucket}/{key.get('Key')}",
                        backend=self,
                        local_cache_dir=cloud_path._local_cache_dir,
                    )

    def move_file(self, src, dst):
        # just a touch, so "REPLACE" metadata
        if src == dst:
            o = self.s3.Object(src.bucket, src.key)
            o.copy_from(
                CopySource={"Bucket": src.bucket, "Key": src.key},
                Metadata=self.get_metadata(src).get("extra", {}),
                MetadataDirective="REPLACE",
            )

        else:
            target = self.s3.Object(dst.bucket, dst.key,)
            target.copy({"Bucket": src.bucket, "Key": src.key})

            self.remove(src)
        return dst

    def remove(self, cloud_path):
        try:
            obj = self.s3.Object(cloud_path.bucket, cloud_path.key,)

            # will throw if not a file
            obj.get()

            resp = obj.delete()
            assert resp.get("HTTPStatusCode") == 204

        except self.client.exceptions.NoSuchKey:
            # try to delete as a direcotry instead
            bucket = self.s3.Bucket(cloud_path.bucket)

            prefix = cloud_path.key
            if prefix and not prefix.endswith("/"):
                prefix += "/"

            resp = bucket.objects.filter(Prefix=prefix).delete()

            # ensure directory deleted; if cloud_path did not exist at all
            # resp will be [], so no need to check success
            if resp:
                assert resp[0].get("ResponseMetadata").get("HTTPStatusCode") == 200

    def upload_file(self, local_path, cloud_path):
        obj = self.s3.Object(cloud_path.bucket, cloud_path.key,)

        obj.upload_file(str(local_path))
        return cloud_path
