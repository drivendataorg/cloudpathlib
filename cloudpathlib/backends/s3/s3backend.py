import os
from pathlib import Path, PurePosixPath
from tempfile import TemporaryDirectory
from typing import Optional

from boto3.session import Session
import botocore.session

from ...cloudpath import Backend, CloudPath, register_backend_class, register_path_class


@register_backend_class("s3")
class S3Backend(Backend):
    """Backend for AWS S3.
    """

    def __init__(
        self,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        botocore_session: Optional[botocore.session.Session] = None,
        profile_name: Optional[str] = None,
        boto3_session: Optional[Session] = None,
    ):
        """Class constructor. Sets up a boto3 [`Session`][boto3.session.Session]. Directly supports
        the same authentication interface, as well as the same environment variables supported by
        boto3. See [boto3 Session documentation](
        https://boto3.amazonaws.com/v1/documentation/api/latest/guide/session.html).

        Parameters
        ----------
        aws_access_key_id : Optional[str], optional
            AWS access key ID, by default None.
        aws_secret_access_key : Optional[str], optional
            AWS secret access key, by default None.
        aws_session_token : Optional[str], optional
            Session key for your AWS account. This is only needed when you are using temporary
            credentials. By default None.
        botocore_session : Optional[botocore.session.Session], optional
            An already instantiated botocore Session, by default None.
        profile_name : Optional[str], optional
            Profile name of a profile in a shared credentials file, by default None.
        boto3_session : Optional[boto3.session.Session], optional
            An already instantiated boto3 Session, by default None.
        """
        if boto3_session is not None:
            self.sess = boto3_session
        else:
            self.sess = Session(
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                aws_session_token=aws_session_token,
                botocore_session=botocore_session,
                profile_name=profile_name,
            )
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

        yielded_dirs = set()

        if recursive:
            for o in bucket.objects.filter(Prefix=prefix):
                # get directory from this path
                for parent in PurePosixPath(o.key[len(prefix) :]).parents:
                    parent = str(parent)

                    # if we haven't surfaced their directory already
                    if parent not in yielded_dirs and parent != ".":
                        yield f"s3://{cloud_path.bucket}/{prefix}{parent}"
                        yielded_dirs.add(parent)

                yield f"s3://{o.bucket_name}/{o.key}"
        else:
            # non recursive is best done with old client API rather than resource
            paginator = self.client.get_paginator("list_objects")

            for result in paginator.paginate(
                Bucket=cloud_path.bucket, Prefix=prefix, Delimiter="/"
            ):

                # sub directory names
                for prefix in result.get("CommonPrefixes", []):
                    yield f"s3://{cloud_path.bucket}/{prefix.get('Prefix')}"

                # files in the directory
                for key in result.get("Contents", []):
                    yield f"s3://{cloud_path.bucket}/{key.get('Key')}"

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
            assert resp.get("ResponseMetadata").get("HTTPStatusCode") == 204

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


S3Backend.S3Path = S3Backend.CloudPath


@register_path_class("s3")
class S3Path(CloudPath):
    cloud_prefix = "s3://"

    @property
    def drive(self):
        return self.bucket

    def is_dir(self):
        return self.backend.is_file_or_dir(self) == "dir"

    def is_file(self):
        return self.backend.is_file_or_dir(self) == "file"

    def mkdir(self, parents=False, exist_ok=False):
        # not possible to make empty directory on s3
        pass

    def touch(self):
        if self.exists():
            self.backend.move_file(self, self)
        else:
            tf = TemporaryDirectory()
            p = Path(tf.name) / "empty"
            p.touch()

            self.backend.upload_file(p, self)

            tf.cleanup()

    def stat(self):
        meta = self.backend.get_metadata(self)

        return os.stat_result(
            (
                None,  # mode
                None,  # ino
                self.cloud_prefix,  # dev,
                None,  # nlink,
                None,  # uid,
                None,  # gid,
                meta.get("size", 0),  # size,
                None,  # atime,
                meta.get("last_modified", 0).timestamp(),  # mtime,
                None,  # ctime,
            )
        )

    @property
    def bucket(self):
        return self._no_prefix.split("/", 1)[0]

    @property
    def key(self):
        key = self._no_prefix_no_drive

        # key should never have starting slash for
        # use with boto, etc.
        if key.startswith("/"):
            key = key[1:]

        return key

    @property
    def etag(self):
        return self.backend.get_metadata(self).get("etag")
