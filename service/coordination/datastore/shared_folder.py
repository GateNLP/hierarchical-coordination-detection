import os
import pathlib
import re
import shutil
from contextlib import contextmanager
from datetime import datetime
from typing import Generator, Literal, Any

from flask import send_from_directory
from flask.typing import ResponseReturnValue

from ..model import ElasticsearchJob
from . import Datastore


name_re = re.compile(r"(.*)\.(?:(output)|(input)|json|exclude)$")


class SharedFolderDatastore(Datastore):
    path: str

    def __init__(self, path: str):
        assert path is not None
        if path and (not path.endswith("/")):
            path = f"{path}/"
        self.path = path

    def is_complete(self, job_id) -> bool:
        """
        A job is complete if its output file exists in the shared folder.
        """
        return os.path.exists(f"{self.path}{job_id}.output")

    def store_input_file(self, job_id, file_path, file_type="input") -> None:
        """
        We can store the input for a job simply by copying the source file
        to the relevant named file in the shared folder.
        """
        target_file = f"{self.path}{job_id}.{file_type}"
        shutil.copyfile(file_path, f"{target_file}.tmp")
        os.replace(f"{target_file}.tmp", target_file)

    def store_input_data(
        self, job_id, data: bytes, file_type: Literal["input", "exclude"] = "input"
    ) -> None:
        """
        Write the given data to the relevant file in the shared folder.
        """
        target_file = f"{self.path}{job_id}.{file_type}"
        with open(f"{target_file}.tmp", "wb") as input_file:
            input_file.write(data)

        # Atomically rename
        os.replace(f"{target_file}.tmp", target_file)

    def fetch_elasticsearch_job(self, job_id) -> ElasticsearchJob | None:
        path_input = f"{self.path}{job_id}.input"

        try:
            return ElasticsearchJob.from_json_file(path_input)
        except Exception:
            return None

    def fetch_output_for_client(
        self, job_id, file_type="output"
    ) -> ResponseReturnValue | None:
        if os.path.exists(f"{self.path}{job_id}.{file_type}"):
            mimetype = "text/csv"
            extension = "csv"
            if file_type == "json":
                mimetype = "application/json"
                extension = "json"

            return send_from_directory(
                self.path,
                f"{job_id}.{file_type}",
                mimetype=mimetype,
                as_attachment=True,
                download_name=f"{job_id}.{extension}",
            )

        return None

    @contextmanager
    def csv_job_files(self, job_id) -> Generator[tuple[str, str, str, str], Any, None]:
        path_input = f"{self.path}{job_id}.input"
        path_useless_hashtags = f"{self.path}{job_id}.exclude"
        path_output = f"{self.path}{job_id}.output"
        path_output_tmp = f"{path_output}.tmp"
        path_json = f"{self.path}{job_id}.json"
        path_json_tmp = f"{path_json}.tmp"

        yield (path_input, path_useless_hashtags, path_json_tmp, path_output_tmp)

        # Atomic rename the output files to their final names, so the frontend never
        # sees a partially-written file
        os.rename(path_json_tmp, path_json)
        os.rename(path_output_tmp, path_output)

    @contextmanager
    def elastic_job_files(
        self, job_id
    ) -> Generator[tuple[ElasticsearchJob, str, str], Any, None]:
        path_input = f"{self.path}{job_id}.input"
        path_output = f"{self.path}{job_id}.output"
        path_output_tmp = f"{path_output}.tmp"
        path_json = f"{self.path}{job_id}.json"
        path_json_tmp = f"{path_json}.tmp"

        job_config = ElasticsearchJob.from_json_file(path_input)
        yield (job_config, path_json_tmp, path_output_tmp)

        # Atomic rename the output files to their final names, so the frontend never
        # sees a partially-written file
        os.rename(path_json_tmp, path_json)
        os.rename(path_output_tmp, path_output)

    def find_expired(self, expire_threshold: datetime) -> tuple[set[str], set[str]]:
        expire_mtime = expire_threshold.timestamp()

        all_completed = set()
        expired_completed = set()
        potentially_hung = set()
        with os.scandir(self.path) as scanner:
            for entry in scanner:
                match = name_re.search(entry.name)
                if match:
                    job_id = match.group(1)
                    if match.group(2):  # this is an output file
                        all_completed.add(job_id)
                        if entry.stat().st_mtime < expire_mtime:
                            expired_completed.add(job_id)
                    elif match.group(3):  # this is an input file
                        if entry.stat().st_mtime < expire_mtime:
                            potentially_hung.add(job_id)

        # Jobs that have completed cannot be hung
        potentially_hung -= all_completed

        return expired_completed, potentially_hung

    def delete_job(self, job_id) -> None:
        data_dir_path = pathlib.Path(self.path)
        for file in data_dir_path.glob(f"{job_id}.*"):
            file.unlink(missing_ok=True)
