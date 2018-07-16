# -*- coding: utf-8 -*-

import time
import Queue
import math

from feeder import Feeder
from proto.xspider_pb2 import *

from bloomfilter import BloomFilter
from mongod_client import DBClient

from task_container import TaskContainer
from feeder import Feeder
from handler import Handler

import logging
logger = logging.getLogger(__name__)


class Scheduler(object):
    """Controller all the fetchers, dispatch the crawlingtask to fetchers."""

    rpc_queue = Queue.Queue()

    def __init__(self, dbaddr, handler, **kwargs):
        self.task_containers = {}
        self.feeders = {}
        self.dao = DBClient(dbaddr)
        self.handler = Handler(handler)

        crawl_bf_dump_path = kwargs.pop("crawl_bf_dump_path", "")
        crawl_bf_load_path = kwargs.pop("crawl_bf_load_path", "")
        self.crawl_filter = BloomFilter(shm_key=0x00912323,
                                        dump_path=crawl_bf_dump_path,
                                        reload_path=crawl_bf_load_path)

        link_bf_dump_path = kwargs.pop("link_bf_dump_path", "")
        link_bf_load_path = kwargs.pop("link_bf_load_path", "")
        self.link_filter = BloomFilter(shm_key=0x00912324,
                                       dump_path=link_bf_dump_path,
                                       reload_path=link_bf_load_path)

        self.healthy_feeders = 0
        self.running = True

    def _load_tasks(self):
        """Load tasks from db when start"""
        summarys = self.dao.load_tasksummary()
        for summary in summarys:
            if "taskid" not in summary:
                logger.error("taskid is not found for %s" % str(summary))
                continue
            taskid = summary["taskid"]
            self.task_containers[taskid] = TaskContainer(
                taskid, summary=summary)
            logger.debug("load task %s." % taskid)
        logger.info("load %s task summarys from db." %
                    len(self.task_containers))

    def _load_fetchers(self):
        """Load fetchers from db when start"""
        fetchers = self.dao.load_fetchers()
        for fetcher in fetchers:
            self.feeders[fetcher.name] = Feeder(fetcher.name, fetcher.addr)
        logger.info("load %s fetchers from db." % len(self.feeders))

    def _schedule(self):
        something = False
        # 1. handle all rpc proto in queue
        if self._process_queue():
            something = True
        # 2. ping handler, if handler is not valid, pause work
        if not self._ping_handler():
            return False
        # 3. handle all feeders, check connection
        if self._ping_fetchers():
            something = True
        if self.healthy_feeders == 0:
            return False
        # 4. handle all tasks in task_containers
        if self._process_containers():
            something = True
        return something

    def run(self):
        self._load_tasks()
        self._load_fetchers()
        # keep trying to feed crawling task to fetchers
        while self.running:
            something = self._schedule()
            if not something:
                time.sleep(1)

        self._process_queue()
        self._process_containers()
        self._rollback_taskstatus()
        self._dump_bloomfilter()

    def _process_queue(self):
        if self.rpc_queue.empty():
            return False

        while not self.rpc_queue.empty():
            request = self.rpc_queue.get()
            if isinstance(request, BasicTask):
                self._add_task(request)
            elif isinstance(request, Fetcher):
                self._add_fecther(request)
            elif isinstance(request, CrawledTask):
                self._add_crawledtask(request)
            elif isinstance(request, StoredTask):
                self._add_storedtask(request)
            elif isinstance(request, TaskControl):
                self._control(request)
            else:
                logger.warning("unsupported request type %s" % type(request))
        return True

    def _add_task(self, basic_task):
        logger.debug("get basic task from rpc with taskid %s"
                     % basic_task.taskid.taskid)
        taskid = basic_task.taskid.taskid
        if taskid in self.task_containers:
            logger.info("taskid %s is already in scheduler." % taskid)
            self._update_tasksummary(taskid, basic_task)
        else:
            logger.debug("add a new task %s to scheduler." % taskid)
            self.task_containers[taskid] = TaskContainer(taskid, basic_task)

    def _add_fecther(self, fetcher):
        logger.debug("add a fether %s" % str(fetcher))
        name = fetcher.name
        if name in self.feeders:
            logger.warning("fetcher %s in already in." % name)
            if self.feeders[name].addr != fetcher.addr:
                logger.error("fetcher %s has old addr %s, an new is %s"
                             % (name, self.feeders[name].addr, fetcher.addr))
                self.feeders[name].reset(fetcher.addr)
                self.dao.update_fetcher(fetcher)
        else:
            logger.info("add a fetcher %s with addr %s" % (name, fetcher.addr))
            self.feeders[name] = Feeder(name, fetcher.addr)
            self.dao.add_fetcher(fetcher)

    def _add_crawledtask(self, crawled_task):
        taskid = crawled_task.taskid.taskid
        if taskid not in self.task_containers:
            logger.error("crawled task %s is not found in scheduler." % taskid)
            return

        task_container = self.task_containers[taskid]
        if len(crawled_task.crawling_urls) == 0:
            task_container.add_crawledtask(crawled_task)
        else:
            task = CrawledTask()
            task.CopyFrom(crawled_task)
            task.ClearField("crawling_urls")

            for crawl_url in crawled_task.crawling_urls:
                if not task_container.link_filter(crawled_task):
                    url = task.crawling_urls.add()
                    url.CopyFrom(crawl_url)
                elif self.link_filter.insert(crawl_url.url):
                    url = task.crawling_urls.add()
                    url.CopyFrom(crawl_url)
                else:
                    logger.debug("%s is duplicated link." % crawl_url.url)
            task_container.add_crawledtask(task)

        if crawled_task.fetcher in self.feeders:
            self.feeders[crawled_task.fetcher].communicated()

        # add crawled url to bloomfilter
        task_container = self.task_containers[taskid]
        if task_container.should_filter(crawled_task):
            self.crawl_filter.insert(crawled_task.crawled_url.url)

    def _add_storedtask(self, stored_task):
        taskid = stored_task.taskid.taskid
        if taskid not in self.task_containers:
            logger.error("stored task %s is not found in scheduler." % taskid)
            return
        self.task_containers[taskid].add_storedtask(stored_task)

    def _update_tasksummary(self, taskid, basic_task):
        if taskid not in self.task_containers:
            return
        task_container = self.task_containers[taskid]
        task_container.summary.update_by_basictask(basic_task)

    def _ping_handler(self):
        if not self.handler.ping():
            return False
        return self.handler.healthy

    def _ping_fetchers(self):
        """Return True if some fetcher not healthy"""
        self.healthy_feeders = 0
        something = False
        for name, feeder in self.feeders.iteritems():
            if not feeder.ping():
                something = True
            else:
                self.healthy_feeders += 1

        for name in self.feeders.keys():
            feeder = self.feeders[name]
            if feeder.should_remove():
                self.dao.del_fetcher(
                    Fetcher(name=feeder.name, addr=feeder.addr))
                del self.feeders[name]

        return something

    def _healthy_filter(self, fetchers):
        healthy_fetchers = []
        for fetcher in fetchers:
            if fetcher not in self.feeders:
                continue
            if self.feeders[fetcher].healthy:
                healthy_fetchers.append(fetcher)
        return healthy_fetchers

    def _process_containers(self):
        something = False
        finished_containers = []
        for taskid, task_container in self.task_containers.iteritems():
            if self._process_task(task_container):
                something = True
            if task_container.finished():
                finished_containers.append(taskid)
        for taskid in finished_containers:
            logger.debug("%s finished, del it." % taskid)
            del self.task_containers[taskid]

        return something

    def _process_task(self, task_container):
        """Return True if some tasks are processed"""
        # 1. update status by crawledtask in task_container's cache
        task_container.update_tasks(self.dao)
        # 2. process the timeout urls, max time is 600s
        task_container.process_timeout_urls()

        if self.healthy_feeders == 0:
            logger.debug("no healthy feeders exist")
            return False

        # TODO
        fetchers = task_container.valid_fetchers(self.feeders.keys())
        fetchers = self._healthy_filter(fetchers)
        if len(fetchers) == 0:
            logger.debug("no healthy feeders for %s" % task_container.taskid)
            return False

        # 3. fetch task document from task collection if need
        task_container.fetch_tasks(self.dao)
        # 4. get urls to crawling
        candidate_tasks = task_container.crawling_tasks()
        crawl_tasks = []
        # 5. filt crawled url
        for task in candidate_tasks:
            if not task_container.should_filter(task):
                crawl_tasks.append(task)
            elif self.crawl_filter.exist(task.crawl_urls[0].url):
                crawled_task = self._filted_task(task, task_container)
                self.rpc_queue.put(crawled_task)
            else:
                crawl_tasks.append(task)

        if len(crawl_tasks) == 0:
            return False

        # 6. feed crawl_urls to all feeders
        urls_fetcher = math.ceil(len(crawl_tasks) / (len(fetchers)+0.0))
        urls_fetcher = int(urls_fetcher)
        start_index = 0
        for name in fetchers:
            feeder = self.feeders[name]
            if not feeder.healthy:
                continue
            end_index = start_index + urls_fetcher
            if end_index >= len(crawl_tasks):
                end_index = len(crawl_tasks)

            for task in crawl_tasks[start_index:end_index]:
                logger.debug("%s feed url %s" % (name, task.crawl_urls[0].url))
            feeder.feed(crawl_tasks[start_index:end_index])

            start_index += urls_fetcher

        return urls_fetcher > 0

    def _filted_task(self, crawling_task, task_container):
        crawled_task = CrawledTask()
        crawled_task.taskid.CopyFrom(crawling_task.taskid)
        crawled_task.crawled_url.CopyFrom(crawling_task.crawl_urls[0])
        crawled_task.status = 200
        crawled_task.content_empty = True
        return crawled_task

    def query_task(self, request):
        taskid = request.taskid
        if taskid not in self.task_containers:
            return TaskFullSummary(taskid=request)

        task_container = self.task_containers[taskid]
        full_summary = TaskFullSummary(taskid=request)
        full_summary.summary.CopyFrom(task_container.summary.basic_task)
        full_summary.total_urls = task_container.summary.total_urls
        full_summary.finished_urls = task_container.summary.finished_urls

        for objectid, task in task_container.tasks.iteritems():
            runtime_stats = full_summary.tasks.add()
            runtime_stats.taskid.taskid = task.taskid
            runtime_stats.taskid.objectid = task.objectid
            runtime_stats.total_urls = task.total_urls
            runtime_stats.crawling_urls = task.crawling_urls
            runtime_stats.finished_urls = task.finished_urls
            runtime_stats.last_crawling = \
                time.strftime("%Y-%m-%d %H:%M:%S",
                              time.localtime(task.last_crawling))

            for index, inner_url in task.urls.iteritems():
                crawl_url = None
                if inner_url.status == "wait":
                    crawl_url = runtime_stats.waitings.add()
                elif inner_url.status == "crawling":
                    crawl_url = runtime_stats.crawlings.add()
                elif inner_url.status == "crawled":
                    crawl_url = runtime_stats.crawleds.add()
                if crawl_url is None:
                    continue
                crawl_url.CopyFrom(inner_url.crawl_url)
        return full_summary

    def _control(self, task_control):
        if task_control.cmd_type == CMD_DUMP_BF:
            self._dump_bloomfilter()
        elif task_control.cmd_type == CMD_FINISH:
            self._finish_task(task_control.taskid.taskid)
        else:
            logger.error("invalid task_control received." % str(task_control))

    def _dump_bloomfilter(self):
        logger.info("dump bloom filter.")
        self.crawl_filter.dump()
        self.link_filter.dump()

    def _finish_task(self, taskid):
        logger.info("manually finish task %s" % taskid)
        if taskid not in self.task_containers:
            logger.warning("task %s is not in scheduler when manually finish"
                           % taskid)
            return
        task_container = self.task_containers[taskid]
        task_container.manually_finish(self.dao)

    def sig_action(self, signum, frame):
        logger.warning("catch signal %d" % signum)
        self.running = False

    def _rollback_taskstatus(self):
        logger.info("rollback taskstatus when exiting.")
        for taskid, task_container in self.task_containers.iteritems():
            logger.info("try to rollback taskstatus for %s." % taskid)
            task_container.rollback_taskstatus(self.dao)
