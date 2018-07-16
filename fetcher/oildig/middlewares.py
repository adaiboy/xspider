# -*- coding: utf-8 -*-

# Define here the models for your spider middleware
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/spider-middleware.html

import time
import random
import logging
import urlparse
from scrapy import signals
import scrapy


class RandomUserAgent(object):
    """Randomly rotate user agents based on a list of predefined ones"""

    def __init__(self, agents):
        logging.info("init agents %s" % agents[0])
        self.agents = agents

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings.getlist('USER_AGENT'))

    def process_request(self, request, spider):
        request.headers.setdefault('User-Agent', random.choice(self.agents))


class FixBindAddress(object):
    """Fixed a ip bases on predifined"""

    def __init__(self, address):
        logging.info("init address %s" % address)
        self.address = address

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings.get('BIND_ADDRESS'))

    def process_request(self, request, spider):
        if self.address is None or len(self.address) == 0:
            return
        request.meta.setdefault("bindaddress", (self.address, 0))


class StartTime(object):
    """set start time in meta"""

    def process_request(self, request, spider):
        request.meta.setdefault("start_time", int(round(time.time() * 1000)))


class HandleAllCode(object):
    """handle all code"""

    def process_request(self, request, spider):
        request.meta.setdefault("handle_httpstatus_all", True)


class CustomeSlotKey(object):
    def process_request(self, request, spider):
        rs = urlparse.urlparse(request.url)
        host = rs.netloc
        if host is None or len(host) == 0:
            return
        request.meta['download_slot'] = host


class ExceptionProcessor(object):
    """process exception"""

    def process_exception(self, request, exception, spider):
        return scrapy.http.HtmlResponse(url=request.url, status=405,
                                        body=unicode(str(exception), "utf-8"),
                                        encoding="utf-8", request=request)


class OildigSpiderMiddleware(object):
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(self, response, spider):
        # Called for each response that goes through the spider
        # middleware and into the spider.

        # Should return None or raise an exception.
        return None

    def process_spider_output(self, response, result, spider):
        # Called with the results returned from the Spider, after
        # it has processed the response.

        # Must return an iterable of Request, dict or Item objects.
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Response, dict
        # or Item objects.
        pass

    def process_start_requests(self, start_requests, spider):
        # Called with the start requests of the spider, and works
        # similarly to the process_spider_output() method, except
        # that it doesn’t have a response associated.

        # Must return only requests (not items).
        for r in start_requests:
            yield r

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)
