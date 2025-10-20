from contextlib import AbstractContextManager
from datetime import datetime
from typing import Literal

from flask.typing import ResponseReturnValue

from ..model import ElasticsearchJob


class Datastore:
    def is_complete(self, job_id) -> bool:
        """
        Determines whether the given job ID is complete, based on the
        existence or otherwise of an output file for that job in the
        datastore.
        :param job_id: the job ID
        :return: ``True`` if an output file for that job ID is available,
                 ``False`` otherwise.
        """
        raise NotImplementedError()

    def store_input_file(
        self, job_id, file_path, file_type: Literal["input", "exclude"] = "input"
    ) -> None:
        """
        Atomically store the contents of the given file path as an input file for the
        given job ID.

        :param job_id: the job ID
        :param file_path: path to the file containing the input data
        :param file_type: whether this is the job's ``input`` file
                (applicable to all job types) or its ``exclude``
                file (applicable to CSV jobs only).
        """
        raise NotImplementedError()

    def store_input_data(
        self, job_id, data: bytes, file_type: Literal["input", "exclude"] = "input"
    ) -> None:
        """
        Atomically store the given binary data as an input file for the given job ID.

        :param job_id: the job ID
        :param data: the input data to save
        :param file_type: whether this is the job's ``input`` file
                (applicable to all job types) or its ``exclude``
                file (applicable to CSV jobs only).
        """
        raise NotImplementedError()

    def fetch_elasticsearch_job(self, job_id) -> ElasticsearchJob | None:
        """
        Given the ID of an Elasticsearch-based job, retrieve the input file
        (if present) from the datastore.

        :param job_id: the job ID
        :return: the parsed ElasticsearchJob of this job's input file,
                 if such exists, otherwise None
        """
        raise NotImplementedError()

    def fetch_output_for_client(
        self, job_id, file_type: Literal["output", "json"] = "output"
    ) -> ResponseReturnValue | None:
        """
        Attempt to fetch the output file for the given job ID in a form
        that can be returned to the client.

        :param job_id: ID of the job
        :param file_type: whether this is the job's
                ``output`` CSV file, or ``json`` graph file.
        :return: ``None`` if there is no output available for the given
                 job ID, otherwise a value suitable to be returned by
                 a flask app route function that will send the output
                 file to the HTTP response.
        """
        raise NotImplementedError()

    def csv_job_files(
        self, job_id
    ) -> AbstractContextManager[tuple[str, str, str, str]]:
        """
        Manage the files required to run a CSV-based job.  The return value of
        this method should be used as a context manager, which when entered will
        yield a tuple of four strings representing "file paths":

          - the input CSV file, which can be safely passed to ``pandas.read_csv``
          - the "exclude" file of link terms to ignore, which should be opened
            in text mode (utf-8 encoding) using ``fsspec.open`` - it may be a
            url rather than a local path
          - a local file to which the output JSON graph should be written
          - a local file to which the output CSV data should be written

        The two output files will be committed to the datastore upon exit from
        the context manager::

            with datastore.csv_job_files(job_id) as (in_csv, exclude, out_json, out_csv):
                # read the CSV
                # read the excludes
                # compute the graph
                # write JSON to out_json
                # write CSV to out_csv

        :param job_id: the ID of the job.
        :return: context manager yielding the relevant file paths.
        """
        raise NotImplementedError()

    def elastic_job_files(
        self, job_id
    ) -> AbstractContextManager[tuple[ElasticsearchJob, str, str]]:
        """
        Manage the files required to run an Elasticsearch-based job.  The return
        value of this method should be used as a context manager, which when
        entered will yield a tuple containing:

          - the ``ElasticsearchJob`` specification parsed from the input JSON
          - a local file to which the output JSON graph should be written
          - a local file to which the output CSV data should be written

        The two output files will be committed to the datastore upon exit from
        the context manager::

            with datastore.elastic_job_files(job_id) as (job_config, out_json, out_csv):
                # fetch the data from Elasticsearch
                # compute the graph
                # write JSON to out_json
                # write CSV to out_csv

        :param job_id: the ID of the job.
        :return: context manager yielding the relevant items.
        """
        raise NotImplementedError()

    def find_expired(self, expire_threshold: datetime) -> tuple[set[str], set[str]]:
        """
        Scans the datastore to find jobs that may have expired.  Returns a pair
        of sets of job IDs ``complete_and_expired, started_but_not_finished``, where
        the first set is the IDs of jobs whose output file was created before
        ``expire_threshold``, and the second set is the IDs of jobs whose *input*
        file was created before the ``expire_threshold`` but that don't have a
        corresponding *output* file - i.e. the job was started long ago and will
        likely never finish.

        :param expire_threshold: files older than this timestamp are potentially
                                 expired.
        :return: two sets of "complete-but-expired" and "maybe-crashed" job IDs
        """
        raise NotImplementedError()

    def delete_job(self, job_id) -> None:
        """
        Delete all files from the datastore that relate to the given job ID.

        :param job_id: the job to delete.
        """
        raise NotImplementedError()
