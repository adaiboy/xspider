# -*- coding: utf-8 -*-

from scheduler_client import SchedulerClient
from proto.xspider_pb2 import *
import Queue
import os
import time
from datetime import date
import random
import sys

import logging
logger = logging.getLogger(__name__)


class Handler(object):
    """Handler is used to stored crawldoc."""
    queue = Queue.Queue()

    def __init__(self, scheduler_addr, cache_dir):
        self.scheduler_addr = scheduler_addr
        self.client = None
        self.cache_dir = cache_dir
        self.task_container = {}
        self.task_last_updated = {}
        self.running = True
        self.index = 0

    def _time_str(self):
        return time.strftime("%Y%m%d%H%M%S", time.localtime(time.time()))

    def _handle(self):
        something = False
        if self._process_queue():
            something = True
        if self._process_tasks():
            something = True
        return something

    def run(self):
        if not os.path.isdir(self.cache_dir):
            os.mkdir(self.cache_dir)
        while self.running:
            something = self._handle()
            if not something:
                time.sleep(1)

        self._process_queue()
        self._process_tasks(force=True)

    def _process_queue(self):
        if self.queue.empty():
            return False

        while not self.queue.empty():
            request = self.queue.get()
            if isinstance(request, CrawlDoc):
                if self._add_crawldoc(request):
                    # if docs is already enough for a taskid, break
                    return True
            else:
                logger.warning("upsupported request type of %s" %
                               type(request))
        return True

    def _add_crawldoc(self, crawldoc):
        taskid = crawldoc.taskid.taskid
        if taskid not in self.task_container:
            crawldocs = CrawlDocs()
            crawldocs.taskid.taskid = taskid
            doc = crawldocs.docs.add()
            doc.CopyFrom(crawldoc)
            self.task_container[taskid] = crawldocs
            self.task_last_updated[taskid] = time.time()
            return False
        else:
            doc = self.task_container[taskid].docs.add()
            doc.CopyFrom(crawldoc)
            self.task_last_updated[taskid] = time.time()
            return len(self.task_container[taskid].docs) >= 1024

    def _process_tasks(self, force=False):
        # process crawldoc in self.task_container
        dumped_taskids = []
        for taskid, crawldocs in self.task_container.iteritems():
            if force:
                self._dump(taskid, crawldocs)
                dumped_taskids.append(taskid)
            elif len(crawldocs.docs) >= 1024:
                self._dump(taskid, crawldocs)
                dumped_taskids.append(taskid)
            elif time.time() - self.task_last_updated[taskid] >= 300:
                self._dump(taskid, crawldocs)
                dumped_taskids.append(taskid)

        for taskid in dumped_taskids:
            del self.task_container[taskid]
        return len(dumped_taskids) > 0

    def _dump(self, taskid, crawldocs):
        date_path = os.path.join(self.cache_dir, str(date.today()))
        if not os.path.isdir(date_path):
            os.mkdir(date_path)
        path = os.path.join(date_path, taskid)
        if not os.path.isdir(path):
            os.mkdir(path)

        file_name_prefix = "%s-%d" % (self._time_str(), self.index)
        file_name = "%s.docs" % file_name_prefix
        file_path = os.path.join(path, file_name)
        while os.path.isfile(file_path):
            self.index += 1
            file_name_prefix = "%s-%d" % (self._time_str(), self.index)
            file_name = "%s.docs" % file_name_prefix
            file_path = os.path.join(path, file_name)

        tmp_path = os.path.join(path, "%s.tmp" % file_name_prefix)
        logger.debug("dump file %s with taskid %s" % (file_name, taskid))
        with open(tmp_path, "wb") as fp:
            fp.write(crawldocs.SerializeToString())

        os.rename(tmp_path, file_path)

        self.index += 1
        if self.index >= 9000:
            self.index = 0

    def sig_action(self, signum, frame):
        logger.warning("catch signal %d" % signum)
        self.running = False
