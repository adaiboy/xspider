# -*- coding: utf-8 -*-

from proto.xspider_pb2_grpc import FetchServicer
from proto.xspider_pb2 import *

import logging
logger = logging.getLogger(__name__)


class FetchServer(FetchServicer):
    """Implement of grpc service Fetch.

    Attributes:
        name: the name of this fetcher, needed when rpc to scheduler and handler
        queue: Queue object from scrapy, fetchServer send data to this queue
    """
    name = ""
    queue = None

    def __init__(self, name, queue):
        super(FetchServer, self).__init__()
        self.name = name
        self.queue = queue

    def _one_task(self, request):
        logger.debug("receive crawling task %s" % request.taskid.taskid)
        task = CrawlingTask()
        task.CopyFrom(request)
        for crawl_url in request.crawl_urls:
            task.ClearField("crawl_urls")
            task_url = task.crawl_urls.add()
            task_url.CopyFrom(crawl_url)
            logger.debug("generate ont task %s" % str(task))
            yield task

    def add_crawlingtask(self, request, context):
        logger.debug("receive crawling task %s" % str(request))

        response = TaskResponse()
        if not isinstance(request, CrawlingTask):
            response.code = -1
            response.msg = "request is not instance of CrawlingTask"
            return response

        response.taskid = request.taskid.taskid
        response.code = 0

        # split CrawlingTask
        for task in self._one_task(request):
            self.queue.put(task)

        return response

    def ping(self, request, context):
        logger.debug("receive ping %s" % str(request))
        response = PingResponse(name=self.name, healthy=True)
        return response
