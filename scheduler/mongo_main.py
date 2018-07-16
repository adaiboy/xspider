# -*- coding: utf-8 -*-


import sys
import os
import time
from proto.xspider_pb2 import *
from mongod_client import DBClient
import argparse


def _append_library():
    base_dir = os.path.abspath(".")
    library_path = os.path.join(base_dir, "proto")
    sys.path.append(library_path)


def create_tasksummary(dbclient):
    basic_task = BasicTask()
    basic_task.name = "Unittest-1"
    basic_task.user = "xboy"
    crawl_url = basic_task.crawl_urls.add()
    crawl_url.url = "http://www.baidu.com/"
    crawl_url.url_types.append(URL_LIST)

    basic_task.feature.dup_ignore = True
    basic_task.feature.testing = True
    basic_task.feature.feature_type = FEATURE_ONCE
    rule = basic_task.rules.add()
    rule.rules.append('re:a href="(.*)"')
    rule.url_types.append(URL_CONTENT)

    print dbclient.create_tasksummary(basic_task)


def update_tasksummary_basic(dbclient):
    basic_task = BasicTask()

    basic_task.taskid.taskid = "Task-20170807103930-0001"
    basic_task.taskid.objectid = "5987d2e2346759bf93785d31"

    crawl_url = basic_task.crawl_urls.add()
    crawl_url.url = "http://www.baidu.com/"
    crawl_url.url_types.append(URL_LIST)

    basic_task.runtime.download_delay = 0.1
    basic_task.runtime.concurrent_reqs = 20

    print dbclient.update_tasksummary_basic(basic_task)


def update_tasksummary_crawled(dbclient):
    crawled_stats = CrawlStats()
    crawled_stats.taskid.taskid = "Task-20170807103930-0001"
    crawled_stats.total_url = 10
    crawled_stats.success = 5
    crawled_stats.code404 = 4
    crawled_stats.content_empty = 1
    print dbclient.update_tasksummary_crawled(crawled_stats)


def create_task_basic(dbclient):
    basic_task = BasicTask()

    basic_task.taskid.taskid = "Task-20170807103930-0001"

    crawl_url = basic_task.crawl_urls.add()
    crawl_url.url = "http://www.baidu.com/"
    crawl_url.url_types.append(URL_LIST)

    basic_task.feature.dup_ignore = True
    basic_task.feature.testing = True
    basic_task.feature.feature_type = FEATURE_ONCE

    print dbclient.create_task_basic(basic_task)


def create_task_crawled(dbclient):
    crawled_task = CrawledTask()
    crawled_task.taskid.taskid = "Task-20170807103930-0001"
    crawled_task.crawled_url.url = "http://www.baidu.com/"
    crawled_task.status = 200
    crawled_task.content_empty = False
    for i in xrange(4):
        crawl_url = crawled_task.crawling_urls.add()
        crawl_url.url = "http://www.baidu.com/%d.html" % i
        crawl_url.parent_url = crawled_task.crawled_url.url

    print dbclient.create_task_crawled(crawled_task)


def update_task_crawled(dbclient):
    objectid = "5987d95e3467595d8b05c645"
    crawled_task = CrawledTask()
    crawled_task.taskid.taskid = "Task-20170807103930-0001"
    crawled_task.taskid.objectid = objectid

    crawled_task.crawled_url.url = "http://www.baidu.com/"
    crawled_task.crawled_url.index = 0

    crawled_task.status = 200
    crawled_task.content_empty = False
    for i in xrange(4):
        crawl_url = crawled_task.crawling_urls.add()
        crawl_url.url = "http://www.baidu.com/%d.html" % i
        crawl_url.parent_url = crawled_task.crawled_url.url

    print dbclient.update_task_crawled(crawled_task)


def get_tasks(dbclient):
    #taskid = "Task-20170807103930-0001"
    taskid = "send_url-20170918144657-0469"
    #tasks = dbclient.get_tasks(taskid, cnt=5, status="crawling", target_status="wait")
    tasks = dbclient.get_tasks(
        taskid, cnt=5, status="wait", target_status="crawling")
    for task in tasks:
        print task


def main():
    _append_library()
    #dbclient = DBClient("mongodb://10.212.15.26:27017/")
    dbclient = DBClient("mongodb://10.155.134.220:27017/")
    # create_tasksummary(dbclient)
    # update_tasksummary_basic(dbclient)
    # update_tasksummary_crawled(dbclient)
    # create_task_basic(dbclient)
    # create_task_crawled(dbclient)
    # update_task_crawled(dbclient)
    get_tasks(dbclient)


if __name__ == "__main__":
    main()
