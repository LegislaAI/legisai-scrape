from datetime import date, datetime, timedelta
from botocore.exceptions import ClientError
from scrapy.signals import spider_closed
from ...items import articleItem
from scrapy.http import Request
from bs4 import BeautifulSoup
import requests
import logging
import locale
import scrapy
import boto3
import json
import os
import re

locale.setlocale(locale.LC_TIME, 'pt_BR.UTF-8')

now = datetime.now()
timestamp = datetime.timestamp(now)

today = date.today().strftime("%d/%m/%Y")
today = datetime.strptime(today, "%d/%m/%Y")

search_limit = date.today() - timedelta(days=1)
search_limit = datetime.strptime(search_limit.strftime("%d/%m/%Y"), "%d/%m/%Y")

with open("/home/scrapeops/intersites-scrape/Spiders/CSS_Selectors/DCM.json") as f:
    search_terms = json.load(f)

site_id = "2994cedd-152e-4282-aa99-9fb7e07265fd"

class DCMSpider(scrapy.Spider):
    name = "DCM"
    allowed_domains = ["diariodocentrodomundo.com.br"]
    start_urls = ["https://www.diariodocentrodomundo.com.br/ultimas-noticias/"]
    data = []

    def parse(self, response):
        for article in response.css(search_terms['article']):
            link = article.css(search_terms['link']).get()
            if link is not None:
                yield Request(link, callback=self.parse_article, priority=1)
   
    def parse_article(self, response):
        updated = response.css(search_terms['updated']).get()
        updated = updated.split("T")[0]
        updated = datetime.strptime(updated, "%Y-%m-%d")
        title = response.css(search_terms['title']).get()
        content = response.css(search_terms['content']).getall()
        content = BeautifulSoup(" ".join(content), "html.parser").text
        content = content.replace("\n", " ")
        if search_limit <= updated <= today:
            item = articleItem(
                updated=updated,
                title=title,
                content=content,
                link=response.url,
            )
            yield item
            self.data.append(item)
        else: 
            self.crawler.engine.stop()
            self.upload_data(self)
            
    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(DCMSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.upload_data, signal=spider_closed)
        return spider

    def upload_data(self, spider):
        file_path = f"/home/scrapeops/intersites-scrape/Spiders/Results/{self.name}_{timestamp}.json"
        if not os.path.isfile(file_path):
            with open(file_path, "w") as f:
                json.dump([], f)

        with open(file_path, "r") as f:
            file_data = json.load(f)
            
        data_dicts = [item.to_dict() for item in self.data]

        file_data.extend(data_dicts)

        with open(file_path, "w") as f:
            json.dump(file_data, f, ensure_ascii=False)
            
        upload = requests.post(f"{os.environ['API_URL']}{site_id}", json={"news": file_data})
        print("upload: ", upload)
