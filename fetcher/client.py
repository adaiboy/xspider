# -*- coding: utf-8 -*-

import grpc
from proto import xspider_pb2
from proto.xspider_pb2_grpc import FetchStub


def run():
    conn = grpc.insecure_channel("127.0.0.1:30000")
    client = FetchStub(conn)

    task = xspider_pb2.CrawlingTask()
    task.taskid.taskid = str("Test-20170803095100-0001")

    crawl_url = task.crawl_urls.add()
    crawl_url.url = "http://language.chinadaily.com.cn/news_bilingual_2.html"
    crawl_url.url_types.append(xspider_pb2.URL_LIST)
    crawl_url.level = "level-1"
    crawl_url.index = 0

    linkrule = task.rules.add()
    linkrule.in_level = "level-1"
    # linkrule.rules.append('xpath://*[@id="div_currpage"]')
    # linkrule.rules.append('re://href="(.*)"')
    linkrule.url_types.append(xspider_pb2.URL_CONTENT)
    linkrule.out_level = "level-1"
    # linkrule.allows.append("language.chinadaily.com.cn")
    linkrule.denys.append("re:language")

    print "add_crawlingtask with task %s" % str(task)
    response = client.add_crawlingtask(task)
    print "response %s" % str(response)


if __name__ == "__main__":
    run()
