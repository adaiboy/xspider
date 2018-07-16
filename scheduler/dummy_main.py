# -*- coding: utf-8 -*-


import sys
import os
import time
from concurrent import futures
import argparse
import grpc

from dummy_server import ScheduleServer
from dummy_scheduler import Scheduler
from proto.xspider_pb2_grpc import add_ScheduleServicer_to_server


def _install_args():
    parser = argparse.ArgumentParser(fromfile_prefix_chars='@')
    parser.add_argument("--addr", default="127.0.0.1:20000")
    return parser


def _append_library():
    base_dir = os.path.abspath(".")
    library_path = os.path.join(base_dir, "proto")
    sys.path.append(library_path)


def main():
    parser = _install_args()
    args = parser.parse_args()

    _append_library()

    # 1. scheduler
    scheduler = Scheduler()

    # 2. start rpc server, get fetch name
    schedule_server = ScheduleServer(scheduler.rpc_queue)
    gprc_server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    add_ScheduleServicer_to_server(schedule_server, gprc_server)
    gprc_server.add_insecure_port(args.addr)
    gprc_server.start()
    print "start server at %s" % args.addr

    # 3.
    scheduler.run()


if __name__ == "__main__":
    main()
