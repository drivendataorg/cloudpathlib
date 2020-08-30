import os
from pathlib import PurePosixPath
from typing import Any, Dict, Iterable, Optional, Union

from boto3.session import Session
import botocore.session

from ..base import Backend, register_backend_class
from .s3path import S3Path


@register_backend_class("s3")
class S3Backend(Backend):
    """Backend for AWS S3."""

    def __init__(
        self,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        botocore_session: Optional[botocore.session.Session] = None,
        profile_name: Optional[str] = None,
        boto3_session: Optional[Session] = None,
        local_cache_dir: Optional[Union[str, os.PathLike]] = None,
    ):
        """Class constructor. Sets up a boto3 [`Session`](
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html).
        Directly supports the same authentication interface, as well as the same environment
        variables supported by boto3. See [boto3 Session documentation](
        https://boto3.amazonaws.com/v1/documentation/api/latest/guide/session.html).

        Args:
            aws_access_key_id (Optional[str]): AWS access key ID.
            aws_secret_access_key (Optional[str]): AWS secret access key.
            aws_session_token (Optional[str]): Session key for your AWS account. This is only
                needed when you are using temporarycredentials.
            botocore_session (Optional[botocore.session.Session]): An already instantiated botocore
                Session.
            profile_name (Optional[str]): Profile name of a profile in a shared credentials file.
            boto3_session (Optional[Session]): An already instantiated boto3 Session.
            local_cache_dir (Optional[Union[str, os.PathLike]]): Path to directory to use as cache
                for downloaded files. If None, will use a temporary directory.
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

        super().__init__(local_cache_dir=local_cache_dir)

    def _get_metadata(self, cloud_path: S3Path) -> Dict[str, Any]:
        """

        Args:
          cloud_path: S3Path:

        Returns:

        """
        data = self.s3.ObjectSummary(cloud_path.bucket, cloud_path.key).get()

        return {
            "last_modified": data["LastModified"],
            "size": data["ContentLength"],
            "etag": data["ETag"],
            "mime": data["ContentType"],
            "extra": data["Metadata"],
        }

    def _download_file(
        self, cloud_path: S3Path, local_path: Union[str, os.PathLike]
    ) -> Union[str, os.PathLike]:
        """

        Args:
          cloud_path: S3Path:
          local_path: Union[str:
          os.PathLike]:

        Returns:

        """
        obj = self.s3.Object(cloud_path.bucket, cloud_path.key)

        obj.download_file(str(local_path))
        return local_path

    def _is_file_or_dir(self, cloud_path: S3Path) -> Optional[str]:
        """

        Args:
          cloud_path: S3Path:

        Returns:

        """
        # short-circuit the root-level bucket
        if not cloud_path.key:
            return "dir"

        try:
            obj = self.s3.ObjectSummary(cloud_path.bucket, cloud_path.key)
            obj.get()
            return "file"
        except self.client.exceptions.NoSuchKey:
            prefix = cloud_path.key
            if prefix and not prefix.endswith("/"):
                prefix += "/"

            # not a file, see if it is a directory
            f = self.s3.Bucket(cloud_path.bucket).objects.filter(Prefix=prefix)

            # at least one key with the prefix of the directory
            if bool([_ for _ in f.limit(1)]):
                return "dir"
            else:
                return None

    def _exists(self, cloud_path: S3Path) -> bool:
        """

        Args:
          cloud_path: S3Path:

        Returns:

        """
        return self._is_file_or_dir(cloud_path) in ["file", "dir"]

    def _list_dir(self, cloud_path: S3Path, recursive=False) -> Iterable[S3Path]:
        """

        Args:
          cloud_path: S3Path:
          recursive:  (Default value = False)

        Returns:

        """
        bucket = self.s3.Bucket(cloud_path.bucket)

        prefix = cloud_path.key
        if prefix and not prefix.endswith("/"):
            prefix += "/"

        yielded_dirs = set()

        if recursive:
            for o in bucket.objects.filter(Prefix=prefix):
                # get directory from this path
                for parent in PurePosixPath(o.key[len(prefix) :]).parents:
                    # if we haven't surfaced their directory already
                    if parent not in yielded_dirs and str(parent) != ".":
                        yield self.CloudPath(f"s3://{cloud_path.bucket}/{prefix}{parent}")
                        yielded_dirs.add(parent)

                yield self.CloudPath(f"s3://{o.bucket_name}/{o.key}")
        else:
            # non recursive is best done with old client API rather than resource
            paginator = self.client.get_paginator("list_objects")

            for result in paginator.paginate(
                Bucket=cloud_path.bucket, Prefix=prefix, Delimiter="/"
            ):

                # sub directory names
                for result_prefix in result.get("CommonPrefixes", []):
                    yield self.CloudPath(f"s3://{cloud_path.bucket}/{result_prefix.get('Prefix')}")

                # files in the directory
                for result_key in result.get("Contents", []):
                    yield self.CloudPath(f"s3://{cloud_path.bucket}/{result_key.get('Key')}")

    def _move_file(self, src: S3Path, dst: S3Path) -> S3Path:
        """

        Args:
          src: S3Path:
          dst: S3Path:

        Returns:

        """
        # just a touch, so "REPLACE" metadata
        if src == dst:
            o = self.s3.Object(src.bucket, src.key)
            o.copy_from(
                CopySource={"Bucket": src.bucket, "Key": src.key},
                Metadata=self._get_metadata(src).get("extra", {}),
                MetadataDirective="REPLACE",
            )

        else:
            target = self.s3.Object(dst.bucket, dst.key)
            target.copy({"Bucket": src.bucket, "Key": src.key})

            self._remove(src)
        return dst

    def _remove(self, cloud_path: S3Path) -> None:
        """

        Args:
          cloud_path: S3Path:

        Returns:

        """
        try:
            obj = self.s3.Object(cloud_path.bucket, cloud_path.key)

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

    def _upload_file(self, local_path: Union[str, os.PathLike], cloud_path: S3Path) -> S3Path:
        """

        Args:
          local_path: Union[str:
          os.PathLike]:
          cloud_path: S3Path:

        Returns:

        """
        obj = self.s3.Object(cloud_path.bucket, cloud_path.key)

        obj.upload_file(str(local_path))
        return cloud_path


S3Backend.S3Path = S3Backend.CloudPath
