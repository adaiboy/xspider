# -*- coding: utf-8 -*-


import sys
import os
import grpc
import time
from concurrent import futures

from proto.xspider_pb2_grpc import add_TaskInterfaceServicer_to_server
from task_server import TaskServer

import argparse

import logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def _install_args():
    parser = argparse.ArgumentParser(fromfile_prefix_chars='@')
    parser.add_argument("--addr", default="10.212.15.26:10000")
    parser.add_argument("--dbaddr", default="10.212.15.26:27017")
    parser.add_argument("--scheduler", default="10.212.15.26:20000")
    parser.add_argument("--loglevel", default="DEBUG")
    return parser


def _append_library():
    base_dir = os.path.abspath(".")
    library_path = os.path.join(base_dir, "proto")
    sys.path.append(library_path)


def _init_logging(level):
    logdict = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR
    }
    if level not in logdict:
        level = "INFO"
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        "[%(levelname)s %(asctime)s %(name)s:%(lineno)d] %(message)s")
    handler.setFormatter(formatter)
    logging.getLogger().addHandler(handler)
    logging.getLogger().setLevel(logdict[level])


def main():
    parser = _install_args()
    args = parser.parse_args()

    _append_library()
    _init_logging(args.loglevel)

    # 2. start rpc server, get fetch name
    task_server = TaskServer(args.dbaddr, args.scheduler)
    gprc_server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
    add_TaskInterfaceServicer_to_server(task_server, gprc_server)
    gprc_server.add_insecure_port(args.addr)
    gprc_server.start()

    # 3.
    logger.info("start server at %s" % args.addr)
    while True:
        time.sleep(2)


if __name__ == "__main__":
    main()
