from flask import Blueprint, request
from ..model import ElasticsearchJob
from ..redis_connection import redis_connection
from ..api import calcCoordinationCSV, calcCoordinationES
from ..config import APP_CONFIG
from rq import Queue
import tempfile
import cgi
import hashlib

job_bp = Blueprint("jobs", __name__, url_prefix="/jobs")

q = Queue(connection=redis_connection)
datastore = APP_CONFIG.datastore.create_datastore()


def md5(file_or_bytes):
    hash_md5 = hashlib.md5()
    if isinstance(file_or_bytes, bytes):
        hash_md5.update(file_or_bytes)
    else:
        with open(str(file_or_bytes.name), "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
    return hash_md5.hexdigest()


def statusES(request):
    d = ElasticsearchJob.from_dict(request.get_json())

    job_id = md5(d.to_canonical_json())
    return getJob(job_id)


def processES(request):
    d = ElasticsearchJob.from_dict(request.get_json())

    job_data = d.to_canonical_json()

    # this isn't great as it doesn't take into
    # account time and that the same query could give different results
    # but... for now it's a start and lets me figure out the rest of
    # the integration.
    jobId = md5(job_data)

    # if there is an ouput file with this job ID then the same
    # file has been processed before so we can use that output
    # rather than re-calculating it from scratch
    if datastore.is_complete(jobId):
        # now return a JSON object describing the job
        return dict(jobID=jobId, status="finished")

    # Check if the same job is already running
    job = q.fetch_job(jobId)
    if job is not None and job.get_status(refresh=False) in ("queued", "started"):
        return dict(jobID=jobId, status=job.get_status())

    # now we have the job ID, write the job data to the
    # datastore so the workers can find it
    datastore.store_input_data(jobId, job_data, "input")

    # Check again if the same job is already running, in
    # case someone started it while we were writing the
    # input file to datastore
    job = q.fetch_job(jobId)
    if job is not None and job.get_status(refresh=False) in ("queued", "started"):
        return dict(jobID=jobId, status=job.get_status())

    # now we have all the info and files we need enqueue the job
    # one option we are giving is to keep the result for a day
    job = q.enqueue(
        calcCoordinationES, jobId, result_ttl=86400, job_id=jobId, job_timeout="5h"
    )

    # now return a JSON object describing the job
    return dict(jobID=job.id, status=job.get_status())


def statusCSV(request):
    options = request.args.get("options", default="1", type=str)

    try:
        # open a temp file we can write the post data into
        tempPosts = tempfile.NamedTemporaryFile()

        # save the input file into the temp file
        request.files["posts"].save(tempPosts)
        tempPosts.flush()

        tempHashtags = tempfile.NamedTemporaryFile()

        # save the input file into the temp file
        request.files["exclude"].save(tempHashtags)
        tempHashtags.flush()

        # use the md5 hash as the job idea
        return getJob(md5(tempPosts) + "_" + options + "_" + md5(tempHashtags))
    finally:
        tempPosts.close()
        tempHashtags.close()


def processCSV(request):
    options = request.args.get("options", default="3", type=str)

    try:
        # open a temp file we can write the post data into
        tempPosts = tempfile.NamedTemporaryFile()

        # save the input file into the temp file
        request.files["posts"].save(tempPosts)
        tempPosts.flush()

        tempHashtags = tempfile.NamedTemporaryFile()

        # save the input file into the temp file
        request.files["exclude"].save(tempHashtags)
        tempHashtags.flush()

        # use the md5 hash as the job idea
        jobId = md5(tempPosts) + "_" + options + "_" + md5(tempHashtags)

        # if there is an ouput file with this job ID then the same
        # file has been processed before so we can use that output
        # rather than re-calculating it from scratch
        if datastore.is_complete(jobId):
            # now return a JSON object describing the job
            return dict(jobID=jobId, status="finished")

        # Check if the same job is already running
        job = q.fetch_job(jobId)
        if job is not None and job.get_status(refresh=False) in ("queued", "started"):
            return dict(jobID=jobId, status=job.get_status())

        # now we have the job ID, store the temp files into the
        # datastore so the workers can find them
        datastore.store_input_file(jobId, tempPosts.name, "input")
        datastore.store_input_file(jobId, tempHashtags.name, "exclude")
    finally:
        tempPosts.close()
        tempHashtags.close()

    # Check again if the same job is already running, in
    # case someone started it while we were writing the
    # input file to datastore
    job = q.fetch_job(jobId)
    if job is not None and job.get_status(refresh=False) in ("queued", "started"):
        return dict(jobID=jobId, status=job.get_status())

    # now we have all the info and files we need enqueue the job
    # one option we are giving is to keep the result for a day
    job = q.enqueue(
        calcCoordinationCSV, jobId, result_ttl=86400, job_id=jobId, job_timeout="5h"
    )

    # now return a JSON object describing the job
    return dict(jobID=job.id, status=job.get_status())


@job_bp.route("/process", methods=["POST"])
def process():
    ctype, type_params = cgi.parse_header(request.content_type)

    if ctype == "application/json":
        return processES(request)
    else:
        return processCSV(request)


@job_bp.route("/status", methods=["POST"])
def status():
    ctype, type_params = cgi.parse_header(request.content_type)

    if ctype == "application/json":
        return statusES(request)
    else:
        return statusCSV(request)


@job_bp.route("/<job_id>")
def getJob(job_id):
    # if there is an ouput file with this job ID then the job
    # must have finished even if it's been removed from the queue
    if datastore.is_complete(job_id):
        return dict(jobID=job_id, status="finished")

    # see if the job is in the queue
    res = q.fetch_job(job_id)

    # if the job isn't in the queue then send an error
    if res is None:
        return dict(error="unknown job"), 404

    # return the status of the job from the queue
    return dict(jobID=job_id, status=res.get_status(), error=res.result)


@job_bp.route("/<job_id>/result")
def sendResult(job_id):
    # if there is an output file with this job ID then the job
    # must have finished even if it's been removed from the queue
    if (
        result_response := datastore.fetch_output_for_client(job_id, "output")
    ) is not None:
        return result_response

    # if there isn't an output file then there is no result
    # so we should probably just throw a 404
    return dict(error="unknown job"), 404


@job_bp.route("/<job_id>/graph")
def sendGraphResult(job_id):
    # if there is an output file with this job ID then the job
    # must have finished even if it's been removed from the queue
    if (
        result_response := datastore.fetch_output_for_client(job_id, "json")
    ) is not None:
        return result_response

    # if there isn't an output file then there is no result
    # so we should probably just throw a 404
    return dict(error="unknown job"), 404

@job_bp.route("/examples")
def sendSamples():

    samples = []

    for instance_name in APP_CONFIG.elasticsearch:
        instance = APP_CONFIG.elasticsearch[instance_name];
        
        for index_name, index_config in instance.indexes.items():
            if index_config.examples:
                for label, config in index_config.examples.items():
                    samples.append({
                        "label": label,
                        "config": {
                            **config,
                            "elasticsearch": instance_name,
                            "index": index_name
                        }
                    })
    
    return samples
