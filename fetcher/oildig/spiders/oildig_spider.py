# -*- coding: utf-8 -*-

import scrapy
import os
import time
import Queue
import re

from scrapy.linkextractors import LinkExtractor
from oildig.items import OildigItem
from proto.xspider_pb2 import *
import copy

import logging
logger = logging.getLogger(__name__)


class OilDigSpider(scrapy.Spider):
    name = "oildig"
    queue = Queue.Queue()

    def __init__(self, *args, **kwargs):
        super(OilDigSpider, self).__init__(*args, **kwargs)
        self.stopping = False

    def stop(self):
        self.stopping = True

    def start_requests(self):
        while not self.stopping:
            if self.queue.empty():
                yield None
            else:
                task = self.queue.get()
                if len(task.crawl_urls) == 0:
                    yield None
                else:
                    taskid = task.taskid.taskid
                    objectid = task.taskid.objectid
                    url = task.crawl_urls[0].url
                    item = OildigItem(taskid=taskid, url=url,
                                      objectid=objectid, crawling_task=task)
                    yield scrapy.Request(url=url, meta={'item': item},
                                         dont_filter=True, callback=self.parse)

    def parse(self, response):
        item = response.meta['item']
        item["actual_url"] = response.url
        item["status"] = response.status
        if "Content-Encoding" in response.headers:
            item["content_encoding"] = response.headers.get("Content-Encoding")
        content = item.get('content', "")
        item["content"] = response.body
        #item["content"] = "this is fake content for simple log."
        item['time_spend'] = response.meta.get("download_latency", -1)

        logger.debug("start extract links for %s" % response.url)
        extracted_urls = self._extract_links(response, item)
        item["extracted_urls"] = extracted_urls
        return item

    def _extract_links(self, response, item):
        crawling_task = item["crawling_task"]
        crawl_url = crawling_task.crawl_urls[0]

        extracted_urls = []
        for link_rule in crawling_task.rules:
            logger.debug("start to check rule %s" % link_rule.in_level)
            if not self._should_rule(link_rule, crawl_url):
                continue
            logger.debug("apply rule %s to url %s"
                         % (link_rule.in_level, response.url))
            urls = self._extract_crawlurls(link_rule, crawling_task, response)
            extracted_urls.extend(urls)

        return extracted_urls

    def _should_rule(self, link_rule, crawl_url):
        if link_rule.in_level == "*" or crawl_url.level == "":
            return True
        return link_rule.in_level == crawl_url.level

    def _extract_crawlurls(self, link_rule, crawling_task, response):
        """Return CrawlUrls"""
        logger.debug("cuurent rule %s" % str(link_rule))

        re_patterns = []
        xpaths = []
        denys = []
        allow_domains = []
        deny_domains = []
        for rule in link_rule.rules:
            if rule.startswith("re:"):
                logger.debug("apply re pattern %s" % rule[3:])
                re_patterns.append(rule[3:])
            elif rule.startswith("xpath:"):
                logger.debug("apply xpath %s" % rule[6:])
                xpaths.append(rule[6:])
            else:
                logger.error("unsupported rule %s for url %s from task %s"
                             % (rule, response.url, crawling_task.taskid.taskid))

        for allow in link_rule.allows:
            if allow.startswith("re:"):
                logger.debug("apply allows %s" % allow[3:])
                re_patterns.append(allow[3:])
            else:
                logger.debug("apply allow domain %s" % allow)
                allow_domains.append(allow)

        for deny in link_rule.denys:
            if deny.startswith("re:"):
                logger.debug("apply denys %s" % deny[3:])
                denys.append(deny[3:])
            else:
                logger.debug("apply deny domain %s" % deny)
                deny_domains.append(deny)

        link_extractor = LinkExtractor(allow=re_patterns, deny=denys,
                                       allow_domains=allow_domains, deny_domains=deny_domains,
                                       restrict_xpaths=xpaths)
        links = link_extractor.extract_links(response)

        extracted_urls = []
        for link in links:
            crawl_url = CrawlUrl()
            reshaped, reshaped_url = self._reshape(link_rule.reshape, link.url)
            crawl_url.url = reshaped_url
            for url_type in link_rule.url_types:
                crawl_url.url_types.append(url_type)
            crawl_url.level = link_rule.out_level
            if reshaped:
                crawl_url.parent_url = link.url
            else:
                crawl_url.parent_url = response.url
            extracted_urls.append(crawl_url)
            logger.debug("extract link %s" % str(crawl_url))

        return extracted_urls

    def _reshape(self, reshaper, url):
        if not reshaper.HasField('reshape_type'):
            return False, url

        if reshaper.reshape_type == RESHAPE_ADD:
            return self._reshape_add(reshaper, url)
        elif reshaper.reshape_type == RESHAPE_DEL:
            return self._reshape_del(reshaper, url)
        else:
            return False, url

    def _reshape_add(self, reshaper, url):
        if len(reshaper.content) == 0:
            return False, url

        if len(reshaper.pattern) != 0:
            if not re.search(reshaper.pattern, url):
                return False, url

        return True, "%s%s" % (url, reshaper.content)

    def _reshape_del(self, reshaper, url):
        if len(reshaper.pattern) == 0:
            return False, url

        match = re.search(reshaper.pattern, url)
        if not match:
            return False, url

        searched_str = match.group(0)
        target = searched_str
        for group in match.groups():
            target = target.replace(group, '', 1)
        return True, url.replace(searched_str, target)
