# -*- coding: utf-8 -*-

import grpc
from proto.xspider_pb2 import *
from proto.xspider_pb2_grpc import TaskInterfaceStub
from pbjson import *

import argparse
import os
import sys
import json

import logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class PortalClient(object):
    addr = ""

    def __init__(self, addr):
        self.addr = addr
        self._client = None

    def _connect(self):
        if self._client is None:
            self._client = TaskInterfaceStub(grpc.insecure_channel(self.addr))

    def add_task(self, basic_task):
        self._connect()
        response = self._client.add_task(basic_task)
        logger.debug("response:\n %s" % str(response))

    def query_task(self, taskid):
        self._connect()
        response = self._client.query_task(TaskId(taskid=taskid))
        logger.debug("response:\n %s" % str(response))

    def query_runtime(self, taskid):
        self._connect()
        response = self._client.query_runtime(TaskId(taskid=taskid))
        logger.debug("response:\n %s" % str(response))

    def dump_bloomfilter(self):
        self._connect()
        response = self._client.dump_boomfilter(Empty())
        logger.debug("response:\n %s" % str(response))


def _install_args():
    parser = argparse.ArgumentParser(fromfile_prefix_chars='@')
    parser.add_argument("--addr", default="127.0.0.1:10000")
    parser.add_argument("--taskfile", default="")
    parser.add_argument("--func", default="query")
    parser.add_argument("--taskid", default="")
    return parser


def _append_library():
    base_dir = os.path.abspath(".")
    library_path = os.path.join(base_dir, "proto")
    sys.path.append(library_path)


def _check_args(args):
    if args.func == "query":
        if len(args.taskid) == 0:
            logger.error("taskid is empty")
            logger.error(
                "usage: python client.py --func=query --taskid=nonemptyid")
            sys.exit(1)
        return
    if args.func == "add":
        if len(args.taskfile) == 0:
            logger.error(
                "usage: python client.py --func=add --taskfile=nonemptypath")
            exit(1)
        if not os.path.isfile(args.taskfile):
            logger.error("%s is not file" % args.taskfile)
            logger.error(
                "usage: python client.py --func=add --taskfile=existedfile")
            exit(1)
        return
    if args.func == "runtime":
        if len(args.taskid) == 0:
            logger.error("taskid is empty")
            logger.error(
                "usage: python client.py --func=runtime --taskid=nonemptyid")
            sys.exit(1)
        return
    if args.func == "dump":
        return

    logger.error("usage: func can only be query/add/runtime/dump")
    exit(1)


def query(client, taskid):
    client.query_task(taskid)


def add(client, filepath):
    jdict = None
    with open(filepath, "r") as fp:
        jdict = json.loads(fp.read())

    if jdict is None:
        logger.error("load none from %s" % filepath)
        exit(1)

    logger.debug("load file with json %s" % str(jdict))
    basic_task = dict2pb(BasicTask, jdict)
    logger.debug("get basic task: \n%s" % str(basic_task))
    client.add_task(basic_task)


def runtime(client, taskid):
    client.query_runtime(taskid)


def dump(client):
    client.dump_boomfilter()


if __name__ == "__main__":
    parser = _install_args()
    args = parser.parse_args()
    _check_args(args)

    # append proto to be library
    _append_library()

    # portal client
    client = PortalClient(args.addr)

    # run command
    if args.func == "query":
        query(client, args.taskid)
    elif args.func == "add":
        add(client, args.taskfile)
    elif args.func == "runtime":
        runtime(client, args.taskid)
    elif args.func == "dump":
        dump(client)
