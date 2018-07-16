# -*- coding: utf-8 -*-

import time
import copy
import bson
from pymongo import MongoClient
from proto.xspider_pb2 import *
import functools

import logging
logger = logging.getLogger(__name__)


def retry(attempt, restype="bool"):
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
            if restype == "bool":
                return False
            elif restype == "list":
                return []
            else:
                return None

        return wrapper
    return decorateor


class DBClient(object):
    _taskid_index = 1
    _summary_table = "summary"
    _task_table = "task"
    _fetcher_table = "fetcher"
    _json_summary = {
        "taskid": "taskid",
        "user": "",
        "total_urls": 0,
        "finished_urls": 0,
        "started_time": "1970-07-01 19:00:00",
        "last_updated": "1970-07-01 19:00:00",
        "finished": False,
        "statistics": {
            "success": 0,
            "400": 0,
            "403": 0,
            "404": 0,
            "410": 0,
            "500": 0,
            "504": 0,
            "empty": 0,
            "other": 0,
        },
        "runtime": {
            "download_delay": 0.4,
            "concurrent_reqs":  5,
            "allow_fetchers": [],
            "deny_fetchers": []
        },
        "storage": {
            "store_type": "",
            "dest": "",
            "attachment": "",
            "files": []
        },
        "link_rules": []
    }

    _json_task = {
        "taskid": "xxxx",
        "total_urls": 0,
        "finished_urls": 0,
        "last_updated": "1970-07-01 00:00:00",
        "status": "wait",
        "urls": []
    }

    def __init__(self, addr,
                 db='spider',
                 usr='spider',
                 pwd='spider1024'):
        self.db = db
        self.usr = usr
        self.pwd = pwd
        self.addr = addr
        self._client = None

    def _time_str(self):
        return time.strftime("%Y%m%d%H%M%S", time.localtime(time.time()))

    def _date_time(self):
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))

    def _error_callback(self):
        self._client = None

    def _succ_callback(self):
        pass

    def _connect(self):
        if self._client is None:
            self._client = MongoClient(host=self.addr)
            self._client[self.db].authenticate(self.usr, self.pwd)

    def _status_key(self, status):
        if status == 400:
            return "400"
        elif status == 403:
            return "403"
        elif status == 410:
            return "410"
        elif status == 500:
            return "500"
        elif status == 504:
            return "504"
        elif status < 300:
            return "success"
        else:
            return "other"

    @retry(3, "none")
    def _insert(self, collection, data):
        """Insert will return a objectid or None"""
        try:
            self._connect()
            collection_dao = self._client[self.db][collection]
            if isinstance(data, list):
                return collection_dao.insert_many(data).inserted_id
            else:
                return collection_dao.insert_one(data).inserted_id
        except Exception, e:
            logger.warning("insert exception %s" % str(e))
            self._error_callback()
        return None

    @retry(3, "bool")
    def _remove(self, collection, data, many=False):
        try:
            self._connect()
            collection_dao = self._client[self.db][collection]
            if many:
                collection_dao.delete_many(data)
            else:
                collection_dao.delete_one(data)
            return True
        except Exception, e:
            logger.warning("remove exception %s" % str(e))
            self._error_callback()
            return False

    @retry(3, "bool")
    def _update(self, collection, find_data, target_data, many=True):
        logger.debug("update %s where %s set %s"
                     % (collection, str(find_data), str(target_data)))
        try:
            self._connect()
            collection_dao = self._client[self.db][collection]
            if many:
                res = collection_dao.update_many(find_data, target_data)
            else:
                res = collection_dao.update(find_data, target_data)
        except Exception, e:
            logger.warning("update exception %s" % str(e))
            self._error_callback()
            return False
        return res['nModified'] > 0

    # @retry(2, "list")
    def _search(self, collection, data, cnt=1):
        logger.debug("search collection %s with data %s" %
                     (collection, str(data)))
        try:
            self._connect()
            collection_dao = self._client[self.db][collection]
            if cnt == 1:
                return [collection_dao.find_one(data)]
            elif cnt <= 0:
                return [task for task in collection_dao.find(data)]
            else:
                return [task for task in collection_dao.find(data).limit(cnt)]
        except Exception, e:
            logger.warning("search exception %s" % str(e))
            self._error_callback()
            return []

    def _process_taskid(self, basic_task):
        if len(basic_task.taskid.taskid) > 0:
            return basic_task.taskid.taskid
        name = "Task"
        if basic_task.HasField("name") and len(basic_task.name) > 0:
            name = basic_task.name
        taskid = "%s-%s-%04d" % (name, self._time_str(), self._taskid_index)
        self._taskid_index += 1
        if self._taskid_index > 9999:
            self._taskid_index = 1
        basic_task.taskid.taskid = taskid
        return taskid

    def _process_default(self, basic_task):
        if not basic_task.runtime.HasField('download_delay'):
            basic_task.runtime.download_delay = 0.4
        if not basic_task.runtime.HasField('concurrent_reqs'):
            basic_task.runtime.concurrent_reqs = 5

        if not basic_task.storage.HasField('store_type'):
            basic_task.storage.store_type = STORAGE_HDFS
        if not basic_task.storage.HasField('dest'):
            basic_task.storage.dest = "/user/test/%s" % basic_task.taskid.taskid
        if not basic_task.storage.HasField('attachment'):
            basic_task.storage.attachment = 'test:test,supergroup'

    def _rule_dict(self, link_rule):
        d = {
            "in_level": link_rule.in_level,
            "rules": [],
            "url_types": [],
            "out_level": link_rule.out_level,
            "allows": [],
            "denys": []
        }

        d["rules"].extend([rule for rule in link_rule.rules])
        d["url_types"].extend([url_type for url_type in link_rule.url_types])
        d["allows"].extend([domain for domain in link_rule.allows])
        d["denys"].extend([domain for domain in link_rule.denys])

        # reshaper
        if link_rule.HasField('reshape'):
            if not link_rule.reshape.HasField('reshape_type'):
                return d

            d['reshape'] = {}
            d['reshape']['reshape_type'] = link_rule.reshape.reshape_type
            d['reshape']['pattern'] = link_rule.reshape.pattern
            d['reshape']['content'] = link_rule.reshape.content

        return d

    def _crawlurl_dict(self, crawl_url):
        d = {
            "index": 0,
            "url": crawl_url.url,
            "url_types": [url_type for url_type in crawl_url.url_types],
            "level": crawl_url.level,
            "payload": crawl_url.payload,
            "parent_url": crawl_url.parent_url
        }
        return d

    def _feature_dict(self, feature):
        d = {}
        if feature.HasField('dup_ignore'):
            d['dup_ignore'] = feature.dup_ignore
        if feature.HasField('testing'):
            d['testing'] = feature.testing
        if feature.HasField('feature_type'):
            d['feature_type'] = feature.feature_type
        if feature.HasField('interval'):
            d['interval'] = feature.interval
        return d

    def query_tasksummary(self, taskid=None, objectid=None):
        if taskid is None and objectid is None:
            return None

        req_data = {}
        if objectid is not None:
            req_data["_id"] = bson.objectid.ObjectId(objectid)
        else:
            req_data["taskid"] = taskid

        responses = self._search(self._summary_table, req_data)
        return responses[0] if len(responses) > 0 else None

    def create_tasksummary(self, basic_task):
        json_data = copy.deepcopy(self._json_summary)
        json_data["taskid"] = self._process_taskid(basic_task)

        self._process_default(basic_task)

        json_data["user"] = basic_task.user
        json_data["task_name"] = basic_task.name

        json_data["total_urls"] = len(basic_task.crawl_urls)
        #json_data["finished_urls"] = 0

        date_time = self._date_time()
        json_data["started_time"] = date_time
        json_data["last_updated"] = date_time
        #json_data["finished"] = False

        json_data["runtime"]["download_delay"] = basic_task.runtime.download_delay
        json_data["runtime"]["concurrent_reqs"] = basic_task.runtime.concurrent_reqs
        json_data["runtime"]["allow_fetchers"].extend(
            [fetcher for fetcher in basic_task.runtime.allow_fetchers])
        json_data["runtime"]["deny_fetchers"].extend(
            [fetcher for fetcher in basic_task.runtime.deny_fetchers])

        json_data["storage"]["store_type"] = basic_task.storage.store_type
        json_data["storage"]["dest"] = basic_task.storage.dest
        json_data["storage"]["attachment"] = basic_task.storage.attachment
        #json_data["storage"]["files"] = []

        # link rule
        link_rules = json_data["link_rules"]
        for rule in basic_task.rules:
            link_rules.append(self._rule_dict(rule))

        objectid = self._insert(self._summary_table, json_data)
        if objectid is None:
            return None
        basic_task.taskid.objectid = str(objectid)
        return objectid

    def update_tasksummary_basic(self, basic_task):
        if not basic_task.taskid.HasField("taskid") or \
                len(basic_task.taskid.taskid) == 0:
            return self.create_tasksummary(basic_task)

        taskid = basic_task.taskid.taskid
        objectid = None
        if basic_task.taskid.HasField("objectid") and \
                len(basic_task.taskid.objectid) > 0:
            objectid = basic_task.taskid.objectid

        task = self.query_tasksummary(taskid, objectid)
        if task is None:
            return self.create_tasksummary(basic_task)

        inc_urls = len(basic_task.crawl_urls)
        where_data = {"taskid": taskid}
        modify_data = {
            "$inc": {"total_urls": inc_urls},
            "$set": {"last_updated": self._date_time()}
        }

        # runtime
        if basic_task.runtime.HasField("download_delay"):
            modify_data["$set"]["runtime.download_delay"] = \
                basic_task.runtime.download_delay
        if basic_task.runtime.HasField("concurrent_reqs"):
            modify_data["$set"]["runtime.concurrent_reqs"] = \
                basic_task.runtime.concurrent_reqs
        return self._update(self._summary_table, where_data, modify_data, False)

    def update_tasksummary_crawled(self, crawled_stats, finished=False):
        taskid = crawled_stats.taskid.taskid
        task = self.query_tasksummary(taskid)
        if task is None:
            logger.warning("no task found by taskid %s" % taskid)
            return False

        where_data = {"taskid": taskid}
        modify_data = {
            "$inc": {
                "total_urls": crawled_stats.extracted_url,
                "finished_urls": crawled_stats.total_url
            },
            "$set": {"last_updated": self._date_time()}
        }
        if crawled_stats.success > 0:
            modify_data["$inc"]["statistics.success"] = crawled_stats.success
        if crawled_stats.code400 > 0:
            modify_data["$inc"]["statistics.400"] = crawled_stats.code400
        if crawled_stats.code403 > 0:
            modify_data["$inc"]["statistics.403"] = crawled_stats.code403
        if crawled_stats.code404 > 0:
            modify_data["$inc"]["statistics.404"] = crawled_stats.code404
        if crawled_stats.code410 > 0:
            modify_data["$inc"]["statistics.410"] = crawled_stats.code410
        if crawled_stats.code500 > 0:
            modify_data["$inc"]["statistics.500"] = crawled_stats.code500
        if crawled_stats.code504 > 0:
            modify_data["$inc"]["statistics.504"] = crawled_stats.code504
        if crawled_stats.codexxx > 0:
            modify_data["$inc"]["statistics.other"] = crawled_stats.codexxx
        if crawled_stats.content_empty > 0:
            modify_data["$inc"]["statistics.empty"] = crawled_stats.content_empty

        if finished:
            modify_data["$set"]["finished"] = True

        return self._update(self._summary_table, where_data, modify_data, many=False)

    def create_task_basic(self, basic_task):
        json_task = copy.deepcopy(self._json_task)

        json_task["taskid"] = basic_task.taskid.taskid
        json_task["last_updated"] = self._date_time()
        json_task["total_urls"] = len(basic_task.crawl_urls)
        json_task["status"] = "wait"  # not started
        feature_json = self._feature_dict(basic_task.feature)
        json_task["feature"] = feature_json
        for crawl_url in basic_task.crawl_urls:
            url_json = self._crawlurl_dict(crawl_url)
            # url_json.update(feature_json)
            url_json["index"] = len(json_task["urls"])
            json_task["urls"].append(url_json)

        return self._insert(self._task_table, json_task)

    def create_task_crawled(self, crawled_task, status="wait"):
        taskid = crawled_task.taskid.taskid
        if len(taskid) == 0 or len(crawled_task.crawling_urls) == 0:
            return

        json_task = copy.deepcopy(self._json_task)
        json_task["taskid"] = taskid
        json_task["last_updated"] = self._date_time()
        json_task["total_urls"] = len(crawled_task.crawling_urls)
        json_task["status"] = status  # default not started

        for crawl_url in crawled_task.crawling_urls:
            url_json = self._crawlurl_dict(crawl_url)
            url_json["index"] = len(json_task["urls"])
            json_task["urls"].append(url_json)

        return self._insert(self._task_table, json_task)

    def update_task_crawled(self, crawled_task, finished=False):
        """Every url crawled will update taskcollection"""
        taskid = crawled_task.taskid.taskid
        objectid = crawled_task.taskid.objectid

        where_data = {"_id": bson.objectid.ObjectId(objectid)}
        modify_data = {
            "$inc": {"finished_urls": 1},
            "$set": {
                "last_updated": self._date_time(),
            }
        }
        if finished:
            modify_data["$set"]["status"] = "crawled"

        idx_str = "urls.%d" % crawled_task.crawled_url.index
        modify_data["$set"]["%s.status" % idx_str] = crawled_task.status
        modify_data["$set"]["%s.content_empty" %
                            idx_str] = crawled_task.content_empty
        modify_data["$set"]["%s.extracted_urls" % idx_str] = \
            len(crawled_task.crawling_urls)
        return self._update(self._task_table, where_data, modify_data, many=False)

    def roll_task_status(self, objectid, status="wait"):
        where_data = {"_id": bson.objectid.ObjectId(objectid)}
        modify_data = {
            "$set": {"status": status}
        }

        return self._update(self._task_table, where_data, modify_data, many=False)

    def get_tasks(self, taskid, cnt=1, status="wait", target_status=None):
        """Get task from taskcollection with status, mainly 'wait'"""
        where_data = {"taskid": taskid, "status": status}
        res = self._search(self._task_table, where_data, cnt=cnt)
        logger.debug("get task %s with cnt %d" % (taskid, len(res)))
        if target_status is None:
            return res
        # update the task's status with target_status, such as crawling
        for task in res:
            query_data = {"_id": task["_id"]}
            modify_data = {
                "$set": {
                    "status": target_status,
                    "last_updated": self._date_time()
                }
            }
            if 0 == self._update(self._task_table, query_data, modify_data, False):
                logger.error("update status task %s of taskid %s failed."
                             % (str(task["_id"]), task["taskid"]))
        return res

    def load_tasksummary(self):
        # TODO: return proto but not dict or list
        where_data = {"finished": False}
        res = self._search(self._summary_table, where_data, cnt=-1)
        return res

    def load_fetchers(self):
        results = []
        fetchers = self._search(self._fetcher_table, {}, cnt=-1)
        for fetcher in fetchers:
            name = fetcher.get("name", "")
            if len(name) == 0:
                continue
            results.append(Fetcher(name=name, addr=fetcher.get("addr", "")))

        return results

    def add_fetcher(self, fetcher):
        if len(fetcher.name) == 0:
            return
        data = {
            "name": fetcher.name,
            "addr": fetcher.addr
        }
        if not self._insert(self._fetcher_table, data):
            return False
        return True

    def update_fetcher(self, fetcher):
        if len(fetcher.name) == 0:
            return
        where = {"name": fetcher.name}
        modify_data = {
            "$set": {"addr": fetcher.addr}
        }
        return self._update(self._fetcher_table, where, modify_data, many=False)

    def del_fetcher(self, fetcher):
        if len(fetcher.name) == 0:
            return
        data = {"name": fetcher.name}
        return self._remove(self._fetcher_table, data, many=True)
