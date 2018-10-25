# -*- coding: utf-8 -*-
import scrapy
import sys


class TencentpostionSpider(scrapy.Spider):
    name = 'tencentPostion'
    allowed_domains = ['tencent.com']
    start_urls = ['https://hr.tencent.com/position.php?keywords=&lid=0&tid=87']

    def parse(self, response):
        filename = 'body.txt'

        with open(filename, 'wb') as f:
            f.write(response.body)
