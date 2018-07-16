# -*- coding: utf-8 -*-


import sys
import os
import time
import argparse
import Queue
from concurrent import futures
import grpc

from fetch_server import FetchServer
from proto.xspider_pb2_grpc import add_FetchServicer_to_server


def _install_args():
    parser = argparse.ArgumentParser(fromfile_prefix_chars='@')
    parser.add_argument("--name", default="fetcher_0")
    parser.add_argument("--addr", default="127.0.0.1:30000")
    parser.add_argument("--crawler", default="oildig")
    parser.add_argument("--scheduler", default="127.0.0.1:20000")
    parser.add_argument("--handler", default="127.0.0.1:40000")
    parser.add_argument("--bind", default="101.227.132.46")
    return parser


def _append_library():
    """The subdir with project name in cfg will be add to sys.path"""
    base_dir = os.path.abspath(".")
    library_path = os.path.join(base_dir, "proto")
    sys.path.append(library_path)


def main():
    parser = _install_args()
    args = parser.parse_args()

    _append_library()

    # 2. start rpc server, get fetch name
    queue = Queue.Queue()
    fetch_server = FetchServer(args.name, queue)
    gprc_server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    add_FetchServicer_to_server(fetch_server, gprc_server)
    gprc_server.add_insecure_port(args.addr)
    gprc_server.start()
    print "start server at %s" % args.addr

    # reactor.run()
    while True:
        time.sleep(2)


if __name__ == "__main__":
    main()
