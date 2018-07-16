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
    def add_task(self, basic_task):
        logger.debug("add task %s" % str(basic_task))
        try:
            self.connect()
            response = self.client.add_task(basic_task)
        except Exception:
            self.error_callback()
            return False
        else:
            self.succ_callback()
        logger.debug("add task with response %s" % str(response))
        return response.code == 0

    def query_task(self, request):
        logger.debug("query task %s" % str(request))
        try:
            self.connect()
            response = self.client.query_task(request)
        except Exception:
            self.error_callback()
            return TaskFullSummary(taskid=request.taskid)
        else:
            self.succ_callback()
        logger.debug("add task with response %s" % str(response))
        return response

    def dump_bloomfilter(self, request):
        logger.debug("dump bloomfilter")
        try:
            self.connect()
            response = self.client.dump_bloomfilter(request)
        except Exception, e:
            self.error_callback()
            return TaskResponse(code=1, msg=str(e))
        else:
            self.succ_callback()
        return TaskResponse(code=0)
