# xspider
基于通用规则的站点覆盖式爬虫，旨在解决不同站点抓取的通用性，核心场景是站点覆盖和增量抓取。

## 环境和依赖
* python2.7 
* scrapy  pip install scrapy
* grpc pip install grpc
* pymongo pip install pymongo
> 部署一个monogodb

> 在scheduler目录下按照bloomfilter.py的接口实现一个c/c++的bloomfilter
  或者直接把它改成python版也行，在我实际使用时，我是改了一个开源的c/c++的，忘了来源了，所以没贴上来

## 各目录说明
* proto 各个模块要用的message和rpc接口都统一在里面xspider.proto了
* portal 面向用户和上层webui/tools的门户
* scheduler 这个是纯裸写的python代码，负责统计抓取信息，冷备数据，控制各个任务的抓取进度和相应的URL调度
* fetcher 改造scrapy，把scrapy从工具订制成一个service，复用其抓取，实现按照规则抽链逻辑
* handler 存储抓取的结果，注意代码里只有存储本地的逻辑实现，没有消息队列和HDFS的实现
* tools 一些工具和例子
> 各个服务目录下均有实际应用的启动脚本

## 架构设计
