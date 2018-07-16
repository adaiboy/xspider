# -*- coding: utf-8 -*-

import sys
import logging
import six
import scrapy
from scrapy.core.engine import ExecutionEngine
from scrapy.crawler import Crawler
from scrapy.crawler import CrawlerProcess

logger = logging.getLogger(__name__)


class CustomEngine(ExecutionEngine):
    def __init__(self, crawler, spider_closed_callback):
        super(CustomEngine, self).__init__(crawler, spider_closed_callback)

    def _next_request(self, spider):
        slot = self.slot
        if not slot:
            return

        if self.paused:
            return

        while not self._needs_backout(spider):
            if not self._next_request_from_scheduler(spider):
                break

        if slot.start_requests and not self._needs_backout(spider):
            try:
                request = next(slot.start_requests)
            except StopIteration:
                slot.start_requests = None
            except Exception:
                slot.start_requests = None
                logger.error('Error while obtaining start requests',
                             exc_info=True, extra={'spider': spider})
            else:
                """When start_requests yield None, it will schedule later"""
                if request is None:
                    slot.nextcall.schedule(delay=1)
                else:
                    self.crawl(request, spider)

        if self.spider_is_idle(spider) and slot.close_if_idle:
            self._spider_idle(spider)


class CustomCrawler(Crawler):
    def __init__(self, spidercls, settings=None):
        super(CustomCrawler, self).__init__(spidercls, settings)

    def _create_engine(self):
        """Return CustomEngine instead of ExecutionEngine"""
        return CustomEngine(self, lambda _: self.stop())


class CustomCrawlerProcess(CrawlerProcess):
    def __init__(self, settings=None):
        super(CustomCrawlerProcess, self).__init__(settings)

    def _create_crawler(self, spidercls):
        if isinstance(spidercls, six.string_types):
            spidercls = self.spider_loader.load(spidercls)
        return CustomCrawler(spidercls, self.settings)
