import re
from datetime import datetime, timedelta, timezone
import os

from coordination.config import APP_CONFIG
from coordination.redis_connection import redis_connection
from rq import Queue

duration_re = re.compile(r"(?:(?P<days>\d+)d)?(?:(?P<hours>\d+)h)?(?:(?P<minutes>\d+)m)?(?:(?P<seconds>\d+)s?)?")


def parse_duration(src, default=None):
    """
    Parse a duration expression into a timedelta.  Supported expressions are whole
    numbers of days, hours, minutes and seconds, in that order, but any of the parts
    may be omitted, e.g.::

        1d
        1d12h
        2h30m
        1d35m
        1m20s
        10s
        50

    The final "s" suffix is optional, so in particular a bare number will be
    parsed as that number of seconds.  Note that this means "1h30" is interpreted as
    one hour and 30 *seconds*, you must add the "m" if you mean "1h30m".

    :param src: duration string
    :param default: (optional) default duration returned if the string could not be parsed
    :return: equivalent timedelta
    """
    if src:
        match = duration_re.fullmatch(src)
        if match:
            return timedelta(**{unit: int(val) for unit, val in match.groupdict().items() if val is not None})

    print(f"Duration could not be parsed, returning default duration of {default}")
    return default


def main():
    q = Queue(connection=redis_connection)
    datastore = APP_CONFIG.datastore.create_datastore()

    # maximum age of a job before it is considered for expiry - default 36 hours
    max_age = parse_duration(os.environ.get("COORDINATION_JOB_EXPIRY"), default=timedelta(hours=36))
    expire_threshold = datetime.now(tz=timezone.utc) - max_age
    print(f"Looking for expired jobs that completed before {expire_threshold.isoformat()}")

    potentially_expired, potentially_hung = datastore.find_expired(expire_threshold)

    expired_jobs = set()
    for job_id in potentially_expired:
        if q.fetch_job(job_id):
            print(f"Job {job_id} has expired, but it is still known to the job queue, ignoring")
        else:
            print(f"Job {job_id} has expired")
            expired_jobs.add(job_id)

    for job_id in potentially_hung:
        if not q.fetch_job(job_id):
            print(f"Job {job_id} started more than {max_age} ago but never completed")
            expired_jobs.add(job_id)

    if not expired_jobs:
        print("No expired jobs found")
        return

    for job_id in expired_jobs:
        print(f"Removing files for job {job_id}")
        datastore.delete_job(job_id)


if __name__ == "__main__":
    main()