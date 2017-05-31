# encoding: utf-8
import scrapy
import re, datetime
from scrapy.selector import Selector
from news.items import NewsItem
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule


class NewsSpider(CrawlSpider):
    name = "news"
    allowed_domains = ["caixin.com"]
    start_urls = ['http://economy.caixin.com',
                  "http://finance.caixin.com",
                  "http://international.caixin.com",
                  "http://china.caixin.com",
                  ]

    rules = (
        Rule(LinkExtractor(allow=r"/2017-05-3\d+/*"),
             callback="parse_news", follow=True),
    )

    def parse_news(self, response):
        item = NewsItem()
        item['news_thread'] = response.url.strip().split('/')[-1].split(".")[0]
        self.get_title(response, item)
        self.get_source(response, item)
        self.get_url(response, item)
        self.get_topic(response, item)
        self.get_img_href(response, item)
        self.get_text(response, item)
        # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!remenber to Retrun Item after parse
        return item

    # 新闻标题
    def get_title(self, response, item):
        # 选择HTML文档中 <head> 标签内的 <title> 元素
        title = response.xpath("/html/head/title/text()").extract()
        if title:
            item['news_title'] = title[0].split("_")[0]

    def get_source(self, response, item):
        source = response.xpath("//div[@class='artInfo']/text()").extract()

        if source:
            dateinfo = re.findall(ur"\s*(\d+)年(\d+)月(\d+)日 (\d+):(\d+)\s*", source[0])

            date_list = [int(x) for x in dateinfo[0]]
            date_list.append(0)
            date_integer = tuple(date_list)
            item['news_time'] = datetime.datetime(*date_integer)

    # 新闻内容
    def get_text(self, response, item):
        news_body = response.xpath("//div[@class='text']/p/text()").extract()

        if news_body:
            item['news_body'] = "\n".join(news_body)

    # 新闻url
    def get_url(self, response, item):
        item['news_url'] = response.url

    def get_topic(self, response, item):
        item["news_topic"] = response.url.split(".")[0].split("//")[1]

    def get_img_href(self, response, item):
        source = response.xpath("//dl[@class='media_pic']/dt/img/@src").extract()
        if source:
            item["news_img_href"] = source[0]
