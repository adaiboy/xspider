# -*- coding: utf-8 -*-


import sys
import os
import grpc
import time
from concurrent import futures

from schedule_server import ScheduleServer
from proto.xspider_pb2_grpc import add_ScheduleServicer_to_server
from dummy_scheduler import Scheduler

import argparse
import signal

import logging
logger = logging.getLogger(__name__)


def _install_args():
    parser = argparse.ArgumentParser(fromfile_prefix_chars='@')
    parser.add_argument("--addr", default="127.0.0.1:20000")
    parser.add_argument("--dbaddr", default="127.0.0.1:27017")
    parser.add_argument("--handler", default="127.0.0.1:40000")
    parser.add_argument("--crawl_bf_path", default="./crawl.bf")
    parser.add_argument("--link_bf_path", default="./link.bf")
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

    # 1. init Scheduler
    scheduler = Scheduler(args.dbaddr,
                          handler=args.handler,
                          crawl_bf_dump_path=args.crawl_bf_path,
                          crawl_bf_load_path=args.crawl_bf_path,
                          link_bf_dump_path=args.link_bf_path,
                          link_bf_load_path=args.link_bf_path)

    # 2. start rpc server, get fetch name
    schedule_server = ScheduleServer(scheduler)
    gprc_server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
    add_ScheduleServicer_to_server(schedule_server, gprc_server)
    gprc_server.add_insecure_port(args.addr)
    gprc_server.start()

    # 3. install signal
    signal.signal(signal.SIGTERM, scheduler.sig_action)
    signal.signal(signal.SIGINT, scheduler.sig_action)

    # 4.
    logger.info("start server at %s" % args.addr)
    scheduler.run()


if __name__ == "__main__":
    main()
