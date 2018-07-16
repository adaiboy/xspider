# -*- coding: utf-8 -*-

# Scrapy settings for oildig project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     http://doc.scrapy.org/en/latest/topics/settings.html
#     http://scrapy.readthedocs.org/en/latest/topics/downloader-middleware.html
#     http://scrapy.readthedocs.org/en/latest/topics/spider-middleware.html

# User Custom Setting
MAXDOC_IN_PROTO = 1024

FETCHER_NAME = "fetcher"
RPC_ADDR = "127.0.0.1:30000"
SCHEDULER_ADDR = '127.0.0.1:20000'
HANDLER_ADDR = '127.0.0.1:40000'
BIND_ADDRESS = ''

HTML_PATH = '/data/sample'


LOG_FILE = 'spider.log'
# LOG_LEVEL='INFO'
LOG_LEVEL = 'DEBUG'
MODE_TEST = False
# MODE_TEST=True

SPIDER_MODULES = ['oildig.spiders']
NEWSPIDER_MODULE = 'oildig.spiders'


# Crawl responsibly by identifying yourself (and your website) on the user-agent
#USER_AGENT = 'oildig (+http://www.yourdomain.com)'
USER_AGENT = [
    "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.71 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/534.57.2 (KHTML, like Gecko) Version/5.1.7 Safari/534.57.2",
    "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:34.0) Gecko/20100101 Firefox/34.0",
    "Mozilla/5.0 (Windows NT 5.1; U; en; rv:1.8.1) Gecko/20061208 Firefox/2.0.0 Opera 9.50"
]
DNSCACHE_ENABLE = True
COOKIES_ENABLED = False
# Obey robots.txt rules
ROBOTSTXT_OBEY = False

# Configure maximum concurrent requests performed by Scrapy (default: 16)
#CONCURRENT_REQUESTS = 32

# Configure a delay for requests for the same website (default: 0)
# See http://scrapy.readthedocs.org/en/latest/topics/settings.html#download-delay
# See also autothrottle settings and docs
DOWNLOAD_DELAY = 0.2  # 0.5s 500ms waiting to download the pages from the same website
# The download delay setting will honor only one of:
CONCURRENT_REQUESTS_PER_DOMAIN = 8
#CONCURRENT_REQUESTS_PER_IP = 8

# extensions
#AUTOTHROTTLE_ENABLED = True
#AUTOTHROTTLE_DEBUG = True

# EXTENSIONS = {
# }

# Disable cookies (enabled by default)
#COOKIES_ENABLED = False

# Disable Telnet Console (enabled by default)
#TELNETCONSOLE_ENABLED = False

# Override the default request headers:
DEFAULT_REQUEST_HEADERS = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Encoding': 'gzip, deflate',
    'Accept-Language': 'zh-CN,zh;q=0.8,en;q=0.6',
}

# Enable or disable spider middlewares
# See http://scrapy.readthedocs.org/en/latest/topics/spider-middleware.html
# SPIDER_MIDDLEWARES = {
#    'oildig.middlewares.OildigSpiderMiddleware': 543,
# }

# Enable or disable downloader middlewares
# See http://scrapy.readthedocs.org/en/latest/topics/downloader-middleware.html
DOWNLOADER_MIDDLEWARES = {
    # 'oildig.middlewares.MyCustomDownloaderMiddleware': 543,
    'oildig.middlewares.RandomUserAgent': 1,
    'oildig.middlewares.FixBindAddress': 100,
    # 'oildig.middlewares.StartTime'	: 101,
    'oildig.middlewares.HandleAllCode'	: 102,
    # 'oildig.middlewares.CustomeSlotKey' : 103,
    'oildig.middlewares.ExceptionProcessor': 104
}
#COMPRESSION_ENABLED = False
# Enable or disable extensions
# See http://scrapy.readthedocs.org/en/latest/topics/extensions.html
# EXTENSIONS = {
#    'scrapy.extensions.telnet.TelnetConsole': None,
# }

# Configure item pipelines
# See http://scrapy.readthedocs.org/en/latest/topics/item-pipeline.html
ITEM_PIPELINES = {
    # 'oildig.pipelines.OildigPipeline': 300,
    'oildig.pipelines.StatusPipeline': 1,
    'oildig.pipelines.LinkPipeline': 100,
}

# Enable and configure the AutoThrottle extension (disabled by default)
# See http://doc.scrapy.org/en/latest/topics/autothrottle.html
#AUTOTHROTTLE_ENABLED = True
# The initial download delay
#AUTOTHROTTLE_START_DELAY = 5
# The maximum download delay to be set in case of high latencies
#AUTOTHROTTLE_MAX_DELAY = 60
# The average number of requests Scrapy should be sending in parallel to
# each remote server
#AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0
# Enable showing throttling stats for every response received:
#AUTOTHROTTLE_DEBUG = False

# Enable and configure HTTP caching (disabled by default)
# See http://scrapy.readthedocs.org/en/latest/topics/downloader-middleware.html#httpcache-middleware-settings
#HTTPCACHE_ENABLED = True
#HTTPCACHE_EXPIRATION_SECS = 0
#HTTPCACHE_DIR = 'httpcache'
#HTTPCACHE_IGNORE_HTTP_CODES = []
#HTTPCACHE_STORAGE = 'scrapy.extensions.httpcache.FilesystemCacheStorage'
