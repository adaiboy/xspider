# -*- coding: utf-8 -*-


import sys
import os
import grpc
import time
import signal
from concurrent import futures

from handle_server import HandleServer
from dummy_handler import Handler
from proto.xspider_pb2_grpc import add_HandleServicer_to_server

import argparse
import logging
logger = logging.getLogger(__name__)


def _install_args():
    parser = argparse.ArgumentParser(fromfile_prefix_chars='@')
    parser.add_argument("--addr", default="127.0.0.1:40000")
    parser.add_argument("--scheduler", default="127.0.0.1:20000")
    parser.add_argument("--cache_dir", default="./cache")
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
    handler = Handler(args.scheduler, args.cache_dir)
    gprc_server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    add_HandleServicer_to_server(HandleServer(handler.queue), gprc_server)
    gprc_server.add_insecure_port(args.addr)
    gprc_server.start()

    # 3. install signal
    signal.signal(signal.SIGTERM, handler.sig_action)
    signal.signal(signal.SIGINT, handler.sig_action)

    # 4.
    logger.debug("server start at %s" % args.addr)
    handler.run()


if __name__ == "__main__":
    main()
