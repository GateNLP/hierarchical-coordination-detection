# Custom worker entrypoint, to re-use the same redis connection logic taking
# the host, port and credentials (if any) from environment.
from datetime import datetime, timezone, timedelta
import os
from coordination.redis_connection import redis_connection
from rq import Worker
import logging
import socket
from typing import Optional

# Configure basic logging
logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("worker")


class CustomWorker(Worker):
    """
    Extends the RQ Worker.
    """

    def heartbeat(self, timeout: Optional[int] = None, pipeline=None) -> None:
        super().heartbeat(timeout, pipeline)

        heartbeat_file = os.environ.get("WORKER_HEARTBEAT_FILE")

        if not heartbeat_file:
            return

        timeout = timeout or self.worker_ttl + 60
        expire_time = (
            datetime.now(timezone.utc) + timedelta(seconds=timeout)
        ).timestamp()
        try:
            os.utime(heartbeat_file, times=(expire_time, expire_time))
        except (IOError, OSError) as e:
            logger.warning(
                f"Could not set heartbeat timeout on file {heartbeat_file}: {e}"
            )


def main():
    """
    Setup and start the worker.
    """
    # Check for heartbeat file configuration
    heartbeat_file = os.environ.get("WORKER_HEARTBEAT_FILE")
    if heartbeat_file:
        with open(heartbeat_file, "w") as f:
            f.write(
                "This is a heartbeat file, the worker is dead if the file date is in the past\n"
            )
        worker_class = CustomWorker
    else:
        worker_class = Worker

    # Get worker name from environment or use hostname
    worker_name = os.environ.get("WORKER_NAME")
    if not worker_name:
        worker_name = socket.gethostname()

    # Create and start worker
    worker = worker_class(
        ["default"],
        connection=redis_connection,
        name=worker_name,
        default_worker_ttl=60,
    )
    worker.work()


if __name__ == "__main__":
    main()
