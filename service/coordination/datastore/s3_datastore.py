import io
import json
import re
import tempfile
from contextlib import contextmanager
from datetime import datetime
from typing import Literal, Generator, Iterable, Any

import boto3
from botocore.exceptions import ClientError
from botocore.response import StreamingBody
from flask.typing import ResponseReturnValue

from . import Datastore
from ..model import ElasticsearchJob


name_re = re.compile(r"(.*)\.(?:(output)|(input)|json|exclude)$")


class S3Datastore(Datastore):
    bucket: str
    prefix: str

    def __init__(self, bucket, prefix=""):
        self.bucket = bucket
        if prefix and (not prefix.endswith("/")):
            prefix = f"{prefix}/"
        self.prefix = prefix
        self.s3 = boto3.client("s3")
        self.list_paginator = self.s3.get_paginator("list_objects_v2")

    def _job_path(self, job_id):
        return f"{self.prefix}{job_id[:2]}/{job_id}"

    def is_complete(self, job_id) -> bool:
        try:
            self.s3.head_object(
                Bucket=self.bucket, Key=f"{self._job_path(job_id)}.output"
            )
            return True
        except ClientError:
            # probably a 404 meaning object doesn't exist
            pass
        return False

    def store_input_file(
        self, job_id, file_path, file_type: Literal["input", "exclude"] = "input"
    ) -> None:
        self.s3.upload_file(
            file_path, self.bucket, f"{self._job_path(job_id)}.{file_type}"
        )

    def store_input_data(
        self, job_id, data: bytes, file_type: Literal["input", "exclude"] = "input"
    ) -> None:
        self.s3.upload_fileobj(
            io.BytesIO(data), self.bucket, f"{self._job_path(job_id)}.{file_type}"
        )

    def fetch_elasticsearch_job(self, job_id) -> ElasticsearchJob | None:
        try:
            resp = self.s3.get_object(
                Bucket=self.bucket, Key=f"{self._job_path(job_id)}.input"
            )
            with resp["Body"] as b:
                job_config_dict = json.load(b)

            return ElasticsearchJob.from_dict(job_config_dict)
        except Exception:
            return None

    def fetch_output_for_client(
        self, job_id, file_type: Literal["output", "json"] = "output"
    ) -> ResponseReturnValue | None:
        mimetype = "text/csv"
        extension = "csv"
        if file_type == "json":
            mimetype = "application/json"
            extension = "json"
        try:
            resp = self.s3.get_object(
                Bucket=self.bucket, Key=f"{self._job_path(job_id)}.{file_type}"
            )
            body: StreamingBody = resp["Body"]
            return iter(body), {
                "Content-Type": mimetype,
                "Content-Disposition": f"attachment; filename={job_id}.{extension}"
            }
        except ClientError:
            # probably 404 if the file doesn't exist
            pass
        return None

    @contextmanager
    def csv_job_files(self, job_id) -> Generator[tuple[str, str, str, str], Any, None]:
        url_input = f"s3://{self.bucket}/{self._job_path(job_id)}.input"
        url_exclude = f"s3://{self.bucket}/{self._job_path(job_id)}.exclude"
        with (
            tempfile.NamedTemporaryFile(suffix=".json") as out_json,
            tempfile.NamedTemporaryFile(suffix=".output") as out_output,
        ):
            yield (url_input, url_exclude, out_json.name, out_output.name)

            # Upload the output files
            self.s3.upload_file(
                out_json.name, self.bucket, f"{self._job_path(job_id)}.json"
            )
            self.s3.upload_file(
                out_output.name, self.bucket, f"{self._job_path(job_id)}.output"
            )

    @contextmanager
    def elastic_job_files(
        self, job_id
    ) -> Generator[tuple[ElasticsearchJob, str, str], Any, None]:
        resp = self.s3.get_object(
            Bucket=self.bucket, Key=f"{self._job_path(job_id)}.input"
        )
        with resp["Body"] as b:
            job_config_dict = json.load(b)

        job_config = ElasticsearchJob.from_dict(job_config_dict)
        with (
            tempfile.NamedTemporaryFile(suffix=".json") as out_json,
            tempfile.NamedTemporaryFile(suffix=".output") as out_output,
        ):
            yield (job_config, out_json.name, out_output.name)

            # Upload the output files
            self.s3.upload_file(
                out_json.name, self.bucket, f"{self._job_path(job_id)}.json"
            )
            self.s3.upload_file(
                out_output.name, self.bucket, f"{self._job_path(job_id)}.output"
            )

    def _walk_tree(self, prefix: str) -> Iterable[dict]:
        list_kwargs = {"Bucket": self.bucket}
        if prefix:
            list_kwargs["Prefix"] = prefix
        for page in self.list_paginator.paginate(**list_kwargs):
            if page.get("Contents"):
                yield from page["Contents"]
            if page.get("CommonPrefixes"):
                for sub_prefix in page["CommonPrefixes"]:
                    yield from self._walk_tree(sub_prefix["Prefix"])

    def find_expired(self, expire_threshold: datetime) -> tuple[set[str], set[str]]:
        all_completed = set()
        expired_completed = set()
        potentially_hung = set()

        for item in self._walk_tree(self.prefix):
            k: str = item["Key"]
            lastmod: datetime = item["LastModified"]
            _, _, item_name = k.rpartition("/")
            match = name_re.search(item_name)
            if match:
                job_id = match.group(1)
                if match.group(2):  # this is an output file
                    all_completed.add(job_id)
                    if lastmod < expire_threshold:
                        expired_completed.add(job_id)
                elif match.group(3):  # this is an input file
                    if lastmod < expire_threshold:
                        potentially_hung.add(job_id)

        # Jobs that have completed cannot be hung
        potentially_hung -= all_completed

        return expired_completed, potentially_hung

    def delete_job(self, job_id) -> None:
        # rather than issuing a (relatively expensive) list call to
        # determine which files we need to delete, just try to delete
        # all the possible file names for this job ID and ignore errors
        job_path = self._job_path(job_id)
        try:
            self.s3.delete_objects(
                Bucket=self.bucket,
                Delete={
                    "Objects": [
                        {"Key": f"{job_path}.{extension}"}
                        for extension in (
                            "input",
                            "exclude",
                            "output",
                            "json",
                            "embeddings",
                            "faiss_index",
                            "clusters",
                        )
                    ]
                },
            )
        except ClientError:
            # This is only a best-effort delete anyway
            pass
