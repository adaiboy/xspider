# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import urlparse
import time
import os
from scrapy.exceptions import DropItem

import grpc
from proto.xspider_pb2 import *
from scheduler_client import SchedulerClient
from handler_client import HandlerClient

import logging
logger = logging.getLogger(__name__)


class WebsiteStatus(object):
    def __init__(self, name):
        self.name = name
        self.total = 0
        self.success = 0
        self.status_400 = 0
        self.status_403 = 0
        self.status_404 = 0
        self.status_410 = 0
        self.status_500 = 0
        self.status_504 = 0
        self.status_xxx = 0
        self.content_empty = 0
        self.start_time = time.time()
        self.total_interval = 0

    def _failed_status(self, status):
        return status >= 300

    def _abnormal(self):
        return self.success / (self.total + 0.0) < 0.9

    def _need_summary(self):
        return time.time() - self.start_time >= 60

    def _format(self, cnt):
        return "%d/%f" % (cnt, cnt/(self.total+0.0))

    def _summary(self):
        success = "success: %s" % self._format(self.success)
        status_40x = "status 40x: 400(%s), 403(%s), 404(%s), 410(%s)" \
                     % (self._format(self.status_400),
                        self._format(self.status_403),
                        self._format(self.status_404),
                        self._format(self.status_410))
        status_50x = "status 50x: 500(%s), 504(%s)" \
                     % (self._format(self.status_500),
                        self._format(self.status_504))
        status_xxx = "status other: content empty(%s), other(%s)" \
                     % (self._format(self.content_empty),
                        self._format(self.status_xxx))
        if self.total_interval == 0:
            speed = "Speed: 0 page/sec during %f sec" \
                % (time.time() - self.start_time)
        else:
            spend = time.time() - self.start_time
            speed = "Speed: total %d pages during %f sec, %f page/sec." \
                % (self.total_interval, spend,
                   self.total_interval/spend)
        return "%s\n%s\n%s\n%s\n%s" \
            % (success, status_40x, status_50x, status_xxx, speed)

    def add_status(self, status, content_empty):
        self.total += 1
        self.total_interval += 1

        if status == 400:
            self.status_400 += 1
        elif status == 403:
            self.status_403 += 1
        elif status == 404:
            self.status_404 += 1
        elif status == 410:
            self.status_410 += 1
        elif status == 500:
            self.status_500 += 1
        elif status == 504:
            self.status_504 += 1
        elif self._failed_status(status):
            self.status_xxx += 1
        elif content_empty:
            self.content_empty += 1
        else:
            self.success += 1

        if self._abnormal():
            d = {'website': self.name, 'summary': self._summary()}
            logger.warning("Attention of %s with \n%s"
                           % (self.name, self._summary()))

        if self._need_summary():
            d = {'website': self.name, 'summary': self._summary()}
            logger.info("Summary of %s with \n %s"
                        % (self.name, self._summary()))
            self.total_interval = 0
            self.start_time = time.time()


class StatusPipeline(object):
    def __init__(self):
        self.website_dict = {}
        self.total = 0
        self.doc_cnts_interval = 0
        self.start_time = time.time()

    def _need_summary(self):
        return time.time() - self.start_time >= 60

    def process_item(self, item, spider):
        status = int(item.get("status", '200'))
        url = item.get('url')
        rs = urlparse.urlparse(url)
        host = rs.netloc

        if host not in self.website_dict:
            self.website_dict[host] = WebsiteStatus(host)

        self.website_dict[host].add_status(status, len(item["content"]) < 10)
        self.total += 1
        self.doc_cnts_interval += 1

        if self._need_summary():
            time_iterval = int(time.time() - self.start_time) * 1000
            speed = (self.doc_cnts_interval + 0.0) * 1000.0 / time_iterval
            logger.info("Total Speed %f url/s in %d urls with total %d urls"
                        % (speed, self.doc_cnts_interval, self.total))
            self.doc_cnts_interval = 0
            self.start_time = time.time()

        return item

    def close_spider(self, spider):
        time_iterval = int(time.time() - self.start_time) * 1000
        if time_iterval == 0:
            return
        speed = (self.doc_cnts_interval + 0.0) * 1000.0 / time_iterval
        logger.info("Total Speed %f url/s in %d urls with total %d urls"
                    % (speed, self.doc_cnts_interval, self.total))


class LinkPipeline(object):
    def __init__(self, name, rpc_addr, scheduler_addr, handler_addr, istesting=False):
        self.name = name
        self.rpc_addr = rpc_addr
        self.scheduler_addr = scheduler_addr
        self.handler_addr = handler_addr

        self.scheduler = SchedulerClient(scheduler_addr)
        self.handler = HandlerClient(handler_addr)

        self.istesting = istesting
        self._connect()

    def _connect(self):
        if self.istesting:
            return
        fetcher = Fetcher(name=self.name, addr=self.rpc_addr)
        self.scheduler.add_fetcher(fetcher)

    @classmethod
    def from_crawler(cls, crawler):
        return cls(name=crawler.settings.get('FETCHER_NAME'),
                   rpc_addr=crawler.settings.get('RPC_ADDR'),
                   scheduler_addr=crawler.settings.get('SCHEDULER_ADDR'),
                   handler_addr=crawler.settings.get('HANDLER_ADDR'),
                   istesting=crawler.settings.get('MODE_TEST', False))

    def _get_crawledtask(self, item):
        crawling_task = item["crawling_task"]
        crawl_url = crawling_task.crawl_urls[0]

        task = CrawledTask()
        task.taskid.CopyFrom(crawling_task.taskid)
        task.fetcher = crawling_task.fetcher
        task.crawled_url.CopyFrom(crawl_url)
        task.status = item["status"]
        task.content_empty = (len(item["content"]) < 10)
        extracted_urls = item.get("extracted_urls", [])
        if extracted_urls is not None:
            for url in extracted_urls:
                crawling_url = task.crawling_urls.add()
                crawling_url.CopyFrom(url)
        logger.debug("get crawled task %s" % str(task))
        return task

    def _get_crawldoc(self, item):
        crawling_task = item["crawling_task"]
        crawl_url = crawling_task.crawl_urls[0]

        if len(crawl_url.url_types) == 1 and crawl_url.url_types[0] == URL_LIST:
            return None

        crawldoc = CrawlDoc()
        crawldoc.url = crawl_url.url
        crawldoc.status = item["status"]
        crawldoc.content_type = item.get("content_encoding", "")
        try:
            crawldoc.content = item["content"]
        except Exception, e:
            logger.warning("content decode error for %s" % crawl_url.url)
            crawldoc.content = "charset error with %s" % str(e)
            crawldoc.status = 406
            item["status"] = 406
        crawldoc.payload = crawl_url.payload
        crawldoc.taskid.CopyFrom(crawling_task.taskid)
        crawldoc.storage.CopyFrom(crawling_task.storage)
        return crawldoc

    def process_item(self, item, spider):
        crawldoc = self._get_crawldoc(item)
        crawled_task = self._get_crawledtask(item)
        logger.debug("get crawledtask %s" % str(crawled_task))

        handler_healthy = True
        if crawldoc is not None and not self.istesting:
            handler_healthy = self.handler.add_crawldoc(crawldoc)
        if not self.istesting and handler_healthy:
            self.scheduler.add_crawledtask(crawled_task)
        return item


class OildigPipeline(object):
    def process_item(self, item, spider):
        return item
