# -*- coding: utf-8 -*-

from proto.xspider_pb2_grpc import ScheduleServicer
from proto.xspider_pb2 import *

import logging
logger = logging.getLogger(__name__)


class ScheduleServer(ScheduleServicer):
    """Implement of grpc service Schedule."""

    def __init__(self, scheduler):
        super(ScheduleServer, self).__init__()
        self.queue = scheduler.rpc_queue
        self.scheduler = scheduler

    def add_task(self, request, context):
        logger.debug("receive add_task with %s" % str(request))
        self.queue.put(request)
        return TaskResponse(taskid=request.taskid.taskid, code=0)

    def add_fetcher(self, request, context):
        logger.debug("receive add_fetcher with %s" % str(request))
        self.queue.put(request)
        return TaskResponse(taskid="", code=0)

    def add_crawledtask(self, request, context):
        logger.debug("receive add_crawledtask with %s" % str(request))
        self.queue.put(request)
        return TaskResponse(taskid=request.taskid.taskid, code=0)

    def add_storedtask(self, request, context):
        logger.debug("receive add_storedtask with %s" % str(request))
        self.queue.put(request)
        return TaskResponse(taskid=request.taskid.taskid, code=0)

    def query_task(self, request, context):
        logger.debug("receive query_task with %s" % str(request))
        full_summary = self.scheduler.query_task(request)
        return full_summary

    def control(self, request, context):
        logger.debug("receive control with %s" % str(request))
        self.queue.put(request)
