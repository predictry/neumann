import ujson
import luigi
import stomp
import time
from neumann import Logger


class EventEmitter:

    def send(self, message):
        str_message = ujson.dumps(message)
        Logger.info('Sending [{0}] to OMS.REPLY'.format(str_message))
        conn = stomp.Connection()
        conn.start()
        conn.connect('admin', 'admin', wait=True)
        conn.send('/queue/OMS.REPLY', str_message, headers={'persistent': 'true'})
        conn.disconnect()


@luigi.Task.event_handler(luigi.event.Event.START)
def task_started(task):
    if isinstance(task, EventEmitter):
        Logger.info("[LUIGI EVENT] Starting task {0}.".format(task))
        task.send({
            "serviceProvider": "NEUMANN",
            "jobId": task.job_id,
            "event": "START",
            "time": time.strftime('%Y-%M-%dT%H-%M-%S')
        })


@luigi.Task.event_handler(luigi.event.Event.SUCCESS)
def task_success(task):
    if isinstance(task, EventEmitter):
        Logger.info("[LUIGI EVENT] Successfully executing task {0}".format(task))
        task.send({
            "serviceProvider": "NEUMANN",
            "jobId": task.job_id,
            "event": "SUCCESS",
            "time": time.strftime('%Y-%M-%dT%H-%M-%S')
        })


@luigi.Task.event_handler(luigi.event.Event.FAILURE)
def task_success(task, ex):
    if isinstance(task, EventEmitter):
        Logger.error("[LUIGI EVENT] Failure in  executing task {0} caused by {1}".format(task, ex))
        Logger.exception(ex)
        task.send({
            "serviceProvider": "NEUMANN",
            "jobId": task.job_id,
            "event": "FAILURE",
            "reason": str(ex),
            "time": time.strftime('%Y-%M-%dT%H-%M-%S')
        })


@luigi.Task.event_handler(luigi.event.Event.PROCESSING_TIME)
def task_processing_time(task, processing_time):
    if isinstance(task, EventEmitter):
        Logger.info("[LUIGI EVENT] Job {0} processing time is {1} seconds".format(task.job_id, processing_time))
        task.send({
            "serviceProvider": "NEUMANN",
            "jobId": task.job_id,
            "processing_time": processing_time
        })
