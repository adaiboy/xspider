# -*- coding: utf-8 -*-


import sys
import os
import scrapy
from scrapy.settings import Settings
from importlib import import_module
from six.moves.configparser import SafeConfigParser

import grpc
from concurrent import futures

from custom_scrapy import CustomCrawlerProcess
from fetch_server import FetchServer
from proto.xspider_pb2_grpc import add_FetchServicer_to_server

import argparse

import logging
logger = logging.getLogger(__name__)


def _install_args():
    parser = argparse.ArgumentParser(fromfile_prefix_chars='@')
    parser.add_argument("--name", default="fetcher_0")
    parser.add_argument("--addr", default="127.0.0.1:30000")
    parser.add_argument("--crawler", default="oildig")
    parser.add_argument("--scheduler", default="127.0.0.1:20000")
    parser.add_argument("--handler", default="127.0.0.1:40000")
    parser.add_argument("--bind", default="101.227.132.46")
    return parser


def _append_library(crawler_name):
    """The subdir with project name in cfg will be add to sys.path"""
    base_dir = os.path.abspath(".")
    library_path = os.path.join(base_dir, crawler_name)
    sys.path.append(library_path)

    library_path = os.path.join(base_dir, "proto")
    sys.path.append(library_path)


def get_crawler(args):
    """Start crawl process on spider with name in project defined by cfg"""
    # 1. default settings and custom project settings
    settings = Settings()

    crawler_name = args.crawler
    module_settings = "%s.settings" % crawler_name
    settings.setmodule(module_settings, "project")

    # 2. override some settings
    settings.set("FETCHER_NAME", args.name)
    settings.set("RPC_ADDR", args.addr)
    settings.set("SCHEDULER_ADDR", args.scheduler)
    settings.set("HANDLER_ADDR", args.handler)
    settings.set("BIND_ADDRESS", args.bind)

    # 3. Command and its CrawlProcess surely be crawl
    crawler_process = CustomCrawlerProcess(settings)

    if not (crawler_name in crawler_process.spider_loader._spiders):
        raise RuntimeError("spider %s is not in settings." % crawler_name)

    return crawler_process


def main():
    parser = _install_args()
    args = parser.parse_args()

    crawler_name = args.crawler
    _append_library(crawler_name)
    crawler_process = get_crawler(args)

    # 1. get spider and its queue
    spider = crawler_process.spider_loader._spiders[crawler_name]
    queue = spider.queue

    # 2. start rpc server, get fetch name
    fetch_server = FetchServer(args.name, queue)
    gprc_server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
    add_FetchServicer_to_server(fetch_server, gprc_server)
    gprc_server.add_insecure_port(args.addr)
    gprc_server.start()
    logger.info("start server at %s" % args.addr)

    # 3. crawler run cmd.run(args=[name], opts={})
    crawler_process.crawl(crawler_name)
    crawler_process.start()


if __name__ == "__main__":
    main()
