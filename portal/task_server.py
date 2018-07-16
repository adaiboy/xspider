# -*- coding: utf-8 -*-

from proto.xspider_pb2_grpc import TaskInterfaceServicer
from proto.xspider_pb2 import *

from scheduler_client import SchedulerClient
from mongod_client import DBClient

import logging
logger = logging.getLogger(__name__)


class TaskServer(TaskInterfaceServicer):
    """Implement of grpc service Schedule."""

    def __init__(self, dbaddr, scheduler_addr):
        super(TaskServer, self).__init__()
        self.db = 'spider'
        self.usr = 'spider'
        self.pwd = 'spider1024'
        self._db_client = DBClient(dbaddr)
        self._scheduler = SchedulerClient(scheduler_addr)

    def add_task(self, request, context):
        logger.debug("receive add_task with %s" % str(request))
        # if not self._db_client.create_tasksummary(request):
        if not self._db_client.update_tasksummary_basic(request):
            logger.error("create task summary to db with taskid %s failed"
                         % request.taskid.taskid)
            return TaskStats(taskid=request.taskid.taskid)

        if len(request.crawl_urls) > 0:
            if not self._db_client.create_task_basic(request):
                logger.error("create task to db with taskid %s failed"
                             % request.taskid.taskid)
                return TaskStats(taskid=request.taskid.taskid)

        if not self._scheduler.add_task(request):
            logger.error("add basic task to scheduler with taskid %s failed"
                         % request.taskid.taskid)
            return TaskStats(taskid=request.taskid.taskid)
        return TaskStats(taskid=request.taskid.taskid)

    def _crawlstats_proto(self, summary):
        crawlstats = CrawlStats()
        crawlstats.total_url = summary.get("total_urls", 0)

        statistics = summary.get("statistics", {})
        crawlstats.success = statistics.get("success", 0)
        crawlstats.code400 = statistics.get("400", 0)
        crawlstats.code403 = statistics.get("403", 0)
        crawlstats.code404 = statistics.get("404", 0)
        crawlstats.code410 = statistics.get("410", 0)
        crawlstats.code500 = statistics.get("500", 0)
        crawlstats.code504 = statistics.get("504", 0)
        crawlstats.codexxx = statistics.get("other", 0)
        crawlstats.content_empty = statistics.get("empty", 0)
        return crawlstats

    def query_task(self, request, context):
        logger.debug("receive query task with %s" % str(request))
        summary_dict = self._db_client.query_tasksummary(request.taskid)
        stats = TaskStats(taskid=request.taskid)
        if summary_dict is None:
            return stats

        stats.name = summary_dict.get("task_name", "")
        stats.user = summary_dict.get("user", "")
        stats.start_time = summary_dict.get("started_time", "")
        stats.last_update = summary_dict.get("last_updated", "")
        stats.stats.CopyFrom(self._crawlstats_proto(summary_dict))

        return stats

    def query_runtime(self, request, context):
        logger.debug("receive query runtime with %s" % str(request))
        full_summary = self._scheduler.query_task(request)
        return full_summary

    def dump_bloomfilter(self, request, context):
        logger.debug("receive dump bloomfilter")
        return self._scheduler.dump_bloomfilter(request)
