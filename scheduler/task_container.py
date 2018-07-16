# -*- coding: utf-8 -*-

import time
import math
import re
from proto.xspider_pb2 import *

import logging
logger = logging.getLogger(__name__)


class TaskSummary(object):
    """A tasksummary is corresponding to a document is Summary Collection"""

    def __init__(self, taskid, basic_task=None, summary=None):
        if basic_task is not None:
            self.basic_task = basic_task
            self.total_urls = len(basic_task.crawl_urls)
            self.finished_urls = 0
        elif summary is not None:
            self.basic_task = self._basic_task_proto(summary)
            self.total_urls = summary.get("total_urls", 0)
            self.finished_urls = summary.get("finished_urls", 0)
        else:
            logger.error("task %s cannot be init" % taskid)
            self.basic_task = self._default_task_proto(taskid)
        self.manually_finished = False

    def _default_task_proto(self, taskid):
        task_summary = BaiscTask()
        task_summary.taskid.taskid = taskid
        task_summary.runtime.download_delay = 0.4
        task_summary.runtime.concurrent_reqs = 5
        return task_summary

    def _basic_task_proto(self, summary_dict):
        task_summary = BasicTask()
        task_summary.taskid.taskid = summary_dict.get("taskid")
        task_summary.taskid.objectid = str(summary_dict.get("_id"))
        task_summary.name = summary_dict.get("name", "")
        task_summary.user = summary_dict.get("user", "")

        link_rules = summary_dict.get("link_rules", [])
        for link_rule in link_rules:
            rule = task_summary.rules.add()
            rule.CopyFrom(self._rule_proto(link_rule))

        storage = summary_dict.get("storage", {})
        if "store_type" in storage:
            task_summary.storage.store_type = storage["store_type"]
        if "dest" in storage:
            task_summary.storage.dest = storage["dest"]
        if "attachment" in storage:
            task_summary.storage.attachment = storage["attachment"]

        runtime = summary_dict.get("runtime", {})
        task_summary.runtime.download_delay = runtime.get(
            "download_delay", 0.4)
        task_summary.runtime.concurrent_reqs = runtime.get(
            "concurrent_reqs", 5)
        allow_fetchers = runtime.get("allow_fetchers", [])
        deny_fetchers = runtime.get("deny_fetchers", [])
        for fetcher in allow_fetchers:
            task_summary.runtime.allow_fetchers.append(fetcher)
        for fetcher in deny_fetchers:
            task_summary.runtime.deny_fetchers.append(fetcher)

        return task_summary

    def _rule_proto(self, link_rule):
        rule = LinkRule()
        rule.in_level = link_rule.get("in_level", "")
        rule.out_level = link_rule.get("out_level", "")
        rules = link_rule.get("rules", [])
        for r in rules:
            rule.rules.append(r)
        url_types = link_rule.get("url_types", [])
        for url_type in url_types:
            rule.url_types.append(url_type)

        allows = link_rule.get("allows", [])
        for host in allows:
            rule.allows.append(host)
        denys = link_rule.get("denys", [])
        for host in denys:
            rule.denys.append(host)

        if "reshape" not in link_rule:
            return rule

        reshape = link_rule.get("reshape", {})
        if "reshape_type" in reshape:
            rule.reshape.reshape_type = reshape.get("reshape_type")
        if "pattern" in reshape:
            rule.reshape.pattern = reshape.get("pattern", "")
        if "content" in reshape:
            rule.reshape.content = reshape.get("content", "")

        return rule

    def update_by_basictask(self, basic_task):
        self.total_urls += len(basic_task.crawl_urls)
        if basic_task.runtime.HasField("download_delay"):
            self.basic_task.runtime.download_delay = \
                basic_task.runtime.download_delay
        if basic_task.runtime.HasField("concurrent_reqs"):
            self.basic_task.runtime.concurrent_reqs = \
                basic_task.runtime.concurrent_reqs

    def finished(self):
        if self.manually_finished:
            return True
        return self.finished_urls >= self.total_urls

    def manually_finish(self):
        self.manually_finished = True
        self.finished_urls = 2 * self.total_urls


class InnerUrl(object):
    """An innerurl is corresponding to a url item in document"""

    def __init__(self, crawl_url):
        self.crawl_url = crawl_url
        self.status = "wait"
        self.fetcher = None
        self.code = -1
        self.last_crawling_time = 0

    def is_timeout(self):
        if self.status == "wait" or self.status == "crawled":
            return False
        return time.time() - self.last_crawling_time >= 300

    def crawlable(self):
        return self.status == "wait"

    def set_fetcher(self, fetcher):
        self.status = "crawling"
        self.fetcher = fetcher
        self.last_crawling_time = time.time()

    def set_waiting_status(self):
        self.status = "wait"

    def set_crawling_status(self):
        self.status = "crawling"
        self.last_crawling_time = time.time()

    def set_crawled_status(self, status):
        self.code = status
        self.status = "crawled"


class InnerTask(object):
    """An innertask is corresponding to a concrete task document in db"""
    objectid = None
    urls = {}  # index -> InnerUrl

    def __init__(self, objectid, task_dict):
        # TODO: add params
        self.objectid = objectid
        self.taskid = task_dict.get("taskid")
        self.finished_urls = 0
        self.feature = self._feature_proto(task_dict.get("feature", {}))
        self.urls = self._create_url(task_dict.get("urls", []))

        self.waiting_urls = len(self.urls)
        self.total_urls = len(self.urls)
        self.crawling_urls = 0
        self.last_crawling = 0
        self.manually_finished = False

    def _feature_proto(self, feature_dict):
        feature = Feature()
        feature.dup_ignore = feature_dict.get("dup_ignore", False)
        feature.testing = feature_dict.get("testing", False)
        feature.feature_type = feature_dict.get("feature_type", FEATURE_ONCE)
        feature.interval = feature_dict.get("interval", -1)
        return feature

    def _crawlurl_proto(self, url_dict):
        crawlurl = CrawlUrl()
        crawlurl.index = url_dict["index"]
        crawlurl.url = url_dict.get("url", "")
        for url_type in url_dict.get("url_types", []):
            crawlurl.url_types.append(url_type)
        crawlurl.level = url_dict.get("level", "")
        crawlurl.payload = url_dict.get("payload", "")
        crawlurl.parent_url = url_dict.get("parent_url", "")
        return crawlurl

    def _create_url(self, urls_list):
        urls = {}
        for url in urls_list:
            # if "status" in url:
            #    continue
            if "index" not in url:
                continue
            urls[url["index"]] = InnerUrl(self._crawlurl_proto(url))
        return urls

    def _crawling_task(self, inner_url):
        crawling_task = CrawlingTask()
        crawling_task.taskid.taskid = self.taskid
        crawling_task.taskid.objectid = self.objectid
        crawl_url = crawling_task.crawl_urls.add()
        crawl_url.CopyFrom(inner_url.crawl_url)
        crawling_task.feature.CopyFrom(self.feature)
        return crawling_task

    def is_period(self):
        return self.feature.feature_type == FEATURE_PERIOD and \
            self.feature.interval > 0

    def period_crawltasks(self):
        if not self.is_period():
            return []

        tasks = []
        interval = time.time() - self.last_crawling
        if interval >= self.feature.interval:
            self.last_crawling = time.time()
            self.crawling_urls = len(self.urls)
            for index, url in self.urls.iteritems():
                tasks.append(self._crawling_task(url))
        return tasks

    def fetch_crawltasks(self, maxcnt=-1):
        feed_list = []
        if maxcnt <= 0:
            maxcnt = len(self.urls)
        for index, inner_url in self.urls.iteritems():
            if maxcnt <= 0:
                break
            if inner_url.crawlable():
                feed_list.append(self._crawling_task(inner_url))
                inner_url.set_crawling_status()
                maxcnt -= 1
                self.last_crawling = time.time()
        self.crawling_urls += len(feed_list)
        self.waiting_urls -= len(feed_list)
        return feed_list

    def update_crawledtask(self, crawled_task):
        index = crawled_task.crawled_url.index
        if index not in self.urls:
            logger.error("url %s with index %d not in task %s"
                         % (crawled_task.crawled_url.url, index, self.objectid))
            return

        self.finished_urls += 1
        self.crawling_urls -= 1  # period task's crawling urls has no meaning
        self.urls[index].set_crawled_status(crawled_task.status)

    def finished(self):
        if self.manually_finished:
            return True

        if self.feature.feature_type == FEATURE_PERIOD and self.feature.interval > 0:
            return False
        return self.finished_urls >= self.total_urls

    def manually_finish(self):
        self.manually_finished = True
        self.finished_urls = 2*self.total_urls

    def process_timeout_urls(self):
        for index, inner_url in self.urls.iteritems():
            if inner_url.is_timeout():
                logger.debug("%s of %s is timeout"
                             % (inner_url.crawl_url.url, self.taskid))
                inner_url.set_waiting_status()
                self.crawling_urls -= 1
                self.waiting_urls += 1

    def usefull_for_summary(self):
        if not self.is_period():
            return True
        return self.finished_urls <= self.total_urls


class TaskContainer(object):
    """A taskcontainer is corresponding to a TaskSummary and it's task in db"""

    def __init__(self, taskid, basic_task=None, summary=None):
        self.tasks = {}  # objectid in task collections -> InnerTask
        self.summary = TaskSummary(taskid, basic_task, summary)

        self.crawledtask_cache = []
        self.storedtask_cache = []
        # -10, to void that, a period taskcontainer fetch no task
        # then it will mark finished in dummy scheduler
        self.last_feed_time = time.time() - 10
        self.testing = False

    @property
    def taskid(self):
        return self.summary.basic_task.taskid.taskid

    def _valid(self, fetcher, runtime):
        for deny_fetcher in runtime.deny_fetchers:
            if deny_fetcher.startswith("re:"):
                if re.match(deny_fetcher[3:], fetcher):
                    return False
            else:
                if deny_fetcher == fetcher:
                    return False

        for allow_fetcher in runtime.allow_fetchers:
            if allow_fetcher.startswith("re:"):
                if re.match(allow_fetcher[3:], fetcher):
                    return True
            else:
                if allow_fetcher == fetcher:
                    return True
        return len(runtime.allow_fetchers) == 0

    def valid_fetchers(self, fetchers):
        valids = []
        runtime = self.summary.basic_task.runtime
        for fetcher in fetchers:
            if self._valid(fetcher, runtime):
                valids.append(fetcher)

        return valids

    def add_crawledtask(self, crawled_task):
        self.crawledtask_cache.append(crawled_task)

    def add_storedtask(self, stored_task):
        pass
        # self.storedtask_cache.append(stored_task)

    def _update_status(self, crawled_task, crawlstats):
        status = crawled_task.status
        content_empty = crawled_task.content_empty
        if status == 400:
            crawlstats.code400 += 1
        elif status == 403:
            crawlstats.code403 += 1
        elif status == 404:
            crawlstats.code404 += 1
        elif status == 410:
            crawlstats.code410 += 1
        elif status == 500:
            crawlstats.code500 += 1
        elif status == 504:
            crawlstats.code504 += 1
        elif status < 300:
            if content_empty:
                crawlstats.content_empty += 1
            else:
                crawlstats.success += 1
        else:
            crawlstats.codexxx += 1

        crawlstats.total_url += 1
        crawlstats.extracted_url += len(crawled_task.crawling_urls)

    def update_tasks(self, dao):
        """Update task in crawledtask_cache and storedtask_cache"""
        if len(self.crawledtask_cache) == 0:
            return

        crawlstats = CrawlStats()
        crawled_task = CrawledTask()

        crawlstats.taskid.taskid = self.summary.basic_task.taskid.taskid
        crawled_task.taskid.taskid = self.summary.basic_task.taskid.taskid

        for task in self.crawledtask_cache:
            # 1. aggregate crawlstatus
            self._update_status(task, crawlstats)
            # 2. aggregate crawled_task to create new task collection document
            for crawl_url in task.crawling_urls:
                url = crawled_task.crawling_urls.add()
                url.CopyFrom(crawl_url)
            # 3. update self.tasks, decide if task is finished
            objectid = task.taskid.objectid
            finished = False
            if objectid not in self.tasks:
                logger.error("object id %s with taskid %s by url %s is not found."
                             % (objectid, crawlstats.taskid.taskid, task.crawled_url.url))
            else:
                self.tasks[objectid].update_crawledtask(task)
                finished = self.tasks[objectid].finished()
            # 4. update this url in db
            dao.update_task_crawled(task, finished)

            # 5. update task summary
            if not self.tasks[objectid].usefull_for_summary():
                crawlstats.total_url -= 1
            if self.testing:
                crawlstats.extracted_url = 0

            # 6. del finished task
            if finished:
                del self.tasks[objectid]

        # 7. update task summary by total stats
        if not self.testing:
            self.summary.total_urls += crawlstats.extracted_url
        else:
            logger.debug("task %s is testing task, donot update summary total."
                         % crawlstats.taskid.taskid)
        self.summary.finished_urls += crawlstats.total_url

        # TODO: important, for period task, only check summary itself is not enough
        # check self.tasks
        summary_finished = self.summary.finished()
        if len(self.tasks) > 0:
            summary_finished = False
        if dao.update_tasksummary_crawled(crawlstats, summary_finished) <= 0:
            logger.error("update task summary %s failed." %
                         crawlstats.taskid.taskid)

        # 8. create new task collection document by extracted urls
        status = "wait"
        if self.testing:
            status = "crawled"
            logger.debug("task %s is testing task, create task in table with crawld."
                         % crawlstats.taskid.taskid)
        dao.create_task_crawled(crawled_task, status)
        # 9. clear
        self.crawledtask_cache = []

    def process_timeout_urls(self):
        for objectid, inner_task in self.tasks.iteritems():
            if inner_task.crawling_urls == 0:
                continue
            inner_task.process_timeout_urls()

    def fetch_tasks(self, dao):
        # if self.tasks enough for runtime, do not fetch
        # when no task canbe fetch, and no piriod url, it is finished
        crawling_cnt = 0
        waiting_cnt = 0
        for objectid, inner_task in self.tasks.iteritems():
            crawling_cnt += inner_task.crawling_urls
            waiting_cnt += inner_task.waiting_urls

        download_delay = self.summary.basic_task.runtime.download_delay
        concurrent_reqs = self.summary.basic_task.runtime.concurrent_reqs
        # logger.debug("%s has crawling %d, waiting %d, concurrent %d" \
        #        % (self.taskid, crawling_cnt, waiting_cnt, concurrent_reqs))
        if crawling_cnt >= concurrent_reqs:
            return

        interval = time.time() - self.last_feed_time
        # logger.debug("%s last feed %f, interval %f, download_delay %d" \
        #        % (self.taskid, self.last_feed_time, interval, download_delay))
        if interval < download_delay:
            return
        urls = math.ceil((interval / download_delay) * concurrent_reqs)
        #logger.debug("%s try max %d urls" % (self.taskid, urls))
        if (crawling_cnt + waiting_cnt) >= urls:
            return

        urls = 5*concurrent_reqs if urls > 5*concurrent_reqs else urls

        taskid = self.summary.basic_task.taskid.taskid
        fetch_urls = urls - (crawling_cnt + waiting_cnt)
        logger.debug("task %s will fetch urls %d" % (taskid, fetch_urls))
        while True:
            tasks = dao.get_tasks(
                taskid, cnt=2, status="wait", target_status="crawling")
            logger.debug("task %s get task cnt %d" % (taskid, len(tasks)))
            if len(tasks) == 0:
                break
            for task in tasks:
                if "_id" not in task:
                    continue
                objectid = str(task["_id"])
                self.tasks[objectid] = InnerTask(objectid, task)
                fetch_urls -= self.tasks[objectid].waiting_urls
                if self.tasks[objectid].feature.testing:
                    self.testing = True
                logger.debug("fetch %d urls from task %s with id %s"
                             % (self.tasks[objectid].waiting_urls, taskid, objectid))
            if fetch_urls <= 0:
                break

        fetched_urls = (urls - (crawling_cnt + waiting_cnt)) - fetch_urls
        logger.debug("fetch %d urls for task %s" % (fetched_urls, taskid))

    def _feed_cnt(self):
        crawling_cnt = 0
        for objectid, inner_task in self.tasks.iteritems():
            crawling_cnt += inner_task.crawling_urls

        download_delay = self.summary.basic_task.runtime.download_delay
        concurrent_reqs = self.summary.basic_task.runtime.concurrent_reqs
        if crawling_cnt >= concurrent_reqs:
            return 0

        interval = time.time() - self.last_feed_time
        urls = math.ceil((interval / download_delay) *
                         concurrent_reqs) - crawling_cnt
        if urls <= 0:
            return 0

        return urls

    def crawling_tasks(self):
        # return the inner url to crawl, update last_feed_time
        feed_cnt = self._feed_cnt()
        # always check period
        crawl_tasks = []
        for objectid, inner_task in self.tasks.iteritems():
            if inner_task.is_period():
                crawl_tasks.extend(inner_task.period_crawltasks())

        feed_cnt -= len(crawl_tasks)
        for objectid, inner_task in self.tasks.iteritems():
            if feed_cnt <= 0:
                break
            if inner_task.is_period():
                continue
            feed_list = inner_task.fetch_crawltasks(maxcnt=feed_cnt)
            crawl_tasks.extend(feed_list)
            feed_cnt -= len(feed_list)

        for crawl_task in crawl_tasks:
            for rule in self.summary.basic_task.rules:
                link_rule = crawl_task.rules.add()
                link_rule.CopyFrom(rule)
            crawl_task.storage.CopyFrom(self.summary.basic_task.storage)
            self.last_feed_time = time.time()
        return crawl_tasks

    def should_filter(self, crawled_task):
        objectid = crawled_task.taskid.objectid
        if objectid not in self.tasks:
            return True

        task = self.tasks[objectid]
        if task.feature.dup_ignore or task.feature.testing or task.is_period():
            return False
        return True

    def link_filter(self, crawled_task):
        objectid = crawled_task.taskid.objectid
        if objectid not in self.tasks:
            return True
        task = self.tasks[objectid]
        if task.feature.testing:
            return False
        return True

    def finished(self):
        if len(self.tasks) > 0:
            return False
        if not self.summary.finished():
            return False
        return True

    def manually_finish(self, dao):
        for objectid, task in self.tasks.iteritems():
            task.manually_finish()
            dao.roll_task_status(objectid, status="crawled")

        self.summary.manually_finish()
        crawled_stat = CrawlStats()
        crawled_stat.taskid.CopyFrom(self.summary.basic_task.taskid)
        dao.update_tasksummary_crawled(crawled_stat, finished=True)

    def rollback_taskstatus(self, dao):
        taskid = self.summary.basic_task.taskid.taskid
        for objectid, inner_task in self.tasks.iteritems():
            if inner_task.finished():
                continue
            logger.info("task %s with id %s is not finished." %
                        (taskid, objectid))
            dao.roll_task_status(objectid)
