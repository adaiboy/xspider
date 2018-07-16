# -*- coding: utf-8 -*-

import grpc
from proto.xspider_pb2_grpc import ScheduleStub

import functools
import logging
logger = logging.getLogger(__name__)


def retry(attempt):
    def decorateor(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            att = 0
            while att < attempt:
                res = func(*args, **kwargs)
                if not res:
                    att += 1
                    logger.debug("retry %d for %s" % (att, func.__name__))
                else:
                    return res
            return False

        return wrapper
    return decorateor


class SchedulerClient(object):
    addr = None

    def __init__(self, addr):
        self.addr = addr
        self.client = None
        self.succ = 0
        self.fail = 0

    def error_callback(self):
        self.succ = 0
        self.fail += 1
        self.client = None

    def succ_callback(self):
        self.succ += 1
        if self.fail > 0:
            self.fail -= 1

    def connect(self):
        if self.client is None:
            self.client = ScheduleStub(grpc.insecure_channel(self.addr))

    @retry(3)
    def add_fetcher(self, fetcher):
        try:
            self.connect()
            self.client.add_fetcher(fetcher)
        except Exception:
            self.error_callback()
            return False
        else:
            self.succ_callback()
            return True

    @retry(3)
    def add_crawledtask(self, crawled_task):
        try:
            self.connect()
            self.client.add_crawledtask(crawled_task)
        except Exception:
            self.error_callback()
            return False
        else:
            self.succ_callback()
            return True
