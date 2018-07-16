# -*- coding: utf-8 -*-

from proto.xspider_pb2_grpc import HandleServicer
from proto.xspider_pb2 import *

import logging
logger = logging.getLogger(__name__)


class HandleServer(HandleServicer):
    """Implement of grpc service Handle."""

    name = "handler"

    def __init__(self, queue):
        super(HandleServer, self).__init__()
        self.queue = queue

    def add_crawldoc(self, request, context):
        logger.debug("receive add_crawldoc with %s" % str(request.url))
        self.queue.put(request)
        return TaskResponse(taskid=request.taskid.taskid)

    def ping(self, request, context):
        response = PingResponse(name=self.name, healthy=True)
        return response
