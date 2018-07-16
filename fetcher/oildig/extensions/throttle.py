# -*- coding: utf-8 -*-

from scrapy.extensions.throttle import AutoThrottle
import logging
import urlparse


class CustomAutoThrottle(AutoThrottle):
    def __init__(self, crawler):
        self.delay_domains = crawler.settings.getdict('DELAY_DOMAINS')
        logging.info("loading custom throttle %s" % str(self.delay_domains))
        super(CustomAutoThrottle, self).__init__(crawler)

    def _adjust_delay(self, slot, latency, response):
        rs = urlparse.urlparse(response.url)
        host = rs.netloc
        if host not in self.delay_domains:
            #super(CustomAutoThrottle, self)._adjust_delay(slot, latency, response)
            slot.delay = 0.0
        else:
            #logging.info("url %s in delay %f" % (response.url, self.delay_domains[host]))
            slot.delay = self.delay_domains[host]
