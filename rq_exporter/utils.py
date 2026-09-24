"""
RQ exporter utility functions.

"""

from datetime import datetime, timezone

from redis import Redis
from redis.sentinel import Sentinel
from rq import Queue, Worker
from rq.job import JobStatus
from rq.utils import as_text, utcparse


def get_redis_connection(host='localhost', port='6379', db='0', sentinel=None,
                         sentinel_port='26379', sentinel_master=None,
                         password=None, password_file=None, url=None):
    """Get the Redis connection instance.

    Note:
        If the `url` is provided, all the other options are ignored.
        If `password_file` is provided it will be used instead of `password.`

    Args:
        host (str): Redis hostname
        port (str, int): Redis server port number
        db (str, int): Redis database number
        sentinel (str): Redis sentinel
        sentinel_port (str, int): Redis sentinel port number
        sentinel_master (str): Redis sentinel master name
        password (str): Redis password
        password_file (str): Redis password file path
        url (str): Full Redis connection URL

    Returns:
        redis.Redis: Redis connection instance.

    Raises:
        IOError: On errors opening the password file.

    """
    if url:
        return Redis.from_url(url)

    # Use password file if provided
    if password_file:
        with open(password_file, 'r') as f:
            password = f.read().strip()

    if sentinel:
        addr_list = [
            (
                url.split(':')[0],
                url.split(':')[1] if ':' in url else sentinel_port
            )
            for url in sentinel.split(",")
        ]

        return Sentinel(
            addr_list,
            sentinel_kwargs={'password': password, 'socket_timeout': 1}
        ).master_for(sentinel_master, password=password, db=db, socket_timeout=1)

    return Redis(host=host, port=port, db=db, password=password)


def get_workers_stats(connection, worker_class=None):
    """Get the RQ workers stats.

    Args:
        connection (redis.Redis): Redis connection instance.
        worker_class (type): RQ Worker class

    Returns:
        list: List of worker stats as a dict {name, queues, state}

    Raises:
        redis.exceptions.RedisError: On Redis connection errors

    """
    worker_class = worker_class if worker_class is not None else Worker

    workers = worker_class.all(connection)

    return [
        {
            'name': w.name,
            'queues': w.queue_names(),
            'state': w.get_state(),
            'successful_job_count': w.successful_job_count,
            'failed_job_count': w.failed_job_count,
            'total_working_time': w.total_working_time
        }
        for w in workers
    ]


def get_queue_jobs(connection, queue_name, queue_class=None):
    """Get the jobs by status of a Queue.

    Args:
        connection (redis.Redis): Redis connection instance.
        queue_name (str): The RQ Queue name
        queue_class (type): RQ Queue class

    Returns:
        dict: Number of jobs by job status

    Raises:
        redis.exceptions.RedisError: On Redis connection errors

    """
    queue_class = queue_class if queue_class is not None else Queue

    queue = queue_class(connection=connection, name=queue_name)

    return {
        JobStatus.QUEUED: queue.count,
        JobStatus.STARTED: queue.started_job_registry.get_job_count(cleanup=False),
        JobStatus.FINISHED: queue.finished_job_registry.get_job_count(cleanup=False),
        JobStatus.FAILED: queue.failed_job_registry.get_job_count(cleanup=False),
        JobStatus.DEFERRED: queue.deferred_job_registry.get_job_count(cleanup=False),
        JobStatus.SCHEDULED: queue.scheduled_job_registry.get_job_count(cleanup=False)
    }


def get_jobs_by_queue(connection, queue_class=None):
    """Get the current jobs by queue.

    Args:
        connection (redis.Redis): Redis connection instance.
        queue_class (type): RQ Queue class

    Returns:
        dict: Dictionary of job count by status for each queue

    Raises:
        redis.exceptions.RedisError: On Redis connection errors

    """
    queue_class = queue_class if queue_class is not None else Queue

    queues = queue_class.all(connection)

    return {
        q.name: get_queue_jobs(connection, q.name, queue_class) for q in queues
    }


def get_oldest_job_ages(connection, queue_class=None):
    """Get the age of the oldest job waiting in each queue.

    RQ adds jobs to the end of a queue and dequeues from the front, so the
    front job is normally the oldest. A job enqueued with ``at_front=True``
    goes to the front instead, so the job at each end is checked.

    Args:
        connection (redis.Redis): Redis connection instance.
        queue_class (type): RQ Queue class

    Returns:
        dict: Age in seconds of the oldest queued job for each queue, or 0
            for an empty queue

    Raises:
        redis.exceptions.RedisError: On Redis connection errors

    """
    queue_class = queue_class if queue_class is not None else Queue

    now = datetime.now(timezone.utc)
    ages = {}

    for queue in queue_class.all(connection):
        job_ids = set(queue.get_job_ids(0, 1) + queue.get_job_ids(-1, 1))
        oldest = 0

        for job_id in job_ids:
            enqueued_at = connection.hget(
                queue.job_class.key_for(job_id), 'enqueued_at'
            )

            # The job may have been dequeued or deleted since the queue was read
            if not enqueued_at:
                continue

            enqueued_at = utcparse(as_text(enqueued_at))

            if enqueued_at.tzinfo is None:
                enqueued_at = enqueued_at.replace(tzinfo=timezone.utc)

            oldest = max(oldest, (now - enqueued_at).total_seconds())

        ages[queue.name] = oldest

    return ages
