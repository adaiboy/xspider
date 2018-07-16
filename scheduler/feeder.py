# -*- coding: utf-8 -*-

import time
import math
import logging
import grpc

from proto.xspider_pb2 import *
from proto.xspider_pb2_grpc import FetchStub

logger = logging.getLogger(__name__)


class Feeder(object):
    """A Feeder is corresponding to a fetcher."""

    name = None
    addr = None

    def __init__(self, name, addr):
        self.name = name
        self.addr = addr
        self.client = None
        self.healthy = True
        self.last_communicated = 0
        self.last_unhealthy = 0
        self.failed_cnt = 0

    def reset(self, addr):
        self.addr = addr
        self.client = None

    def communicated(self):
        self.last_communicated = time.time()
        self.healthy = True
        self.failed_cnt = 0

    def should_remove(self):
        if self.healthy:
            return False
        return self.failed_cnt > 12

    def _mark_healthy(self, healthy):
        self.healthy = healthy
        if not healthy:
            self.last_unhealthy = time.time()
            self.failed_cnt += 1

    def _connect(self):
        if self.client is None:
            self.client = FetchStub(grpc.insecure_channel(self.addr))

    def error_callback(self):
        self.healthy = False
        self.client = None
        self.failed_cnt += 1
        self.last_unhealthy = time.time()

    def should_delay(self):
        if self.failed_cnt == 0:
            return False

        failed_cnt = self.failed_cnt
        if failed_cnt > 12:
            failed_cnt = 12

        delay = math.pow(failed_cnt, 2)
        if time.time() - self.last_unhealthy <= delay:
            return True
        return False

    def ping(self):
        # no need to ping as it is too short for communicate
        if time.time() - self.last_communicated < 5:
            return True

        if self.should_delay():
            return

        try:
            self._connect()
            request = PingRequest(name=self.name)
            response = self.client.ping(request)
        except Exception, e:
            logger.error("ping exception %s" % str(e))
            self.error_callback()
            return False
        else:
            if not response.healthy:
                self._mark_healthy(False)
                logger.warning("%s with addr %s is not healthy as %s"
                               % (self.name, self.addr, response.msg))
            else:
                self._mark_healthy(True)
                self.communicated()
            return self.healthy

    def feed(self, crawling_tasks):
        try:
            self._connect()
            for crawling_task in crawling_tasks:
                crawling_task.fetcher = self.name
                self.client.add_crawlingtask(crawling_task)
                #logger.debug("mock send crawlingtask %s" % str(crawling_task))
        except Exception, e:
            logger.error("ping exception %s" % str(e))
            self.error_callback()
