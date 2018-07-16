# -*- coding: utf-8 -*-

import time
import math
import grpc
from proto.xspider_pb2 import *
from proto.xspider_pb2_grpc import HandleStub

import logging
logger = logging.getLogger(__name__)


class Handler(object):
    """A Handler is corresponding to a Handler."""

    addr = None

    def __init__(self, addr):
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

    def _mark_healthy(self, healthy):
        self.healthy = healthy
        if not healthy:
            self.last_unhealthy = time.time()
            self.failed_cnt += 1

    def _connect(self):
        if self.client is None:
            self.client = HandleStub(grpc.insecure_channel(self.addr))

    def error_callback(self):
        self.healthy = False
        self.client = None
        self.failed_cnt += 1
        self.last_unhealthy = time.time()

    def should_delay(self):
        if self.failed_cnt == 0:
            return False
        if self.failed_cnt > 18:
            self.failed_cnt = 18

        delay = math.pow(self.failed_cnt, 2)
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
            request = PingRequest(name="handler")
            response = self.client.ping(request)
        except Exception, e:
            logger.error("ping exception %s" % str(e))
            self.error_callback()
            return False
        else:
            if not response.healthy:
                self._mark_healthy(False)
                logger.warning("handler with addr %s is not healthy as %s"
                               % (self.addr, response.msg))
            else:
                self._mark_healthy(True)
                self.communicated()
            return self.healthy
