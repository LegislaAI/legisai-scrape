from datetime import date, datetime, timedelta
from scrapy.signals import spider_closed
from ...items import articleItem
from scrapy.http import Request
from bs4 import BeautifulSoup
import requests
import locale
import scrapy
import json
import os

locale.setlocale(locale.LC_TIME, 'pt_BR.UTF-8')

now = datetime.now()
timestamp = datetime.timestamp(now)

today = datetime.strptime(date.today().strftime("%d/%m/%Y"), "%d/%m/%Y")
search_limit = datetime.strptime((date.today() - timedelta(days=1)).strftime("%d/%m/%Y"), "%d/%m/%Y")

with open("Spiders/CSS_Selectors/CamaraNoticias.json") as f:
    search_terms = json.load(f)

main_url = "https://www.camara.leg.br/noticias/ultimas?pagina="

site_id = "ab9a6448-3471-491f-9009-d7ec57daba54"

class CamaraNoticiasSpider(scrapy.Spider):
    name = "CamaraNoticias"
    allowed_domains = ["camara.leg.br"]
    start_urls = ["https://www.camara.leg.br/noticias/ultimas?pagina=1"]
    INCREMENT = 1
    data = []
    article_count = 0  # Track collected articles
    found_old_articles = False  # Track if we encounter older articles

    MAX_ARTICLES = 100  # Limit of articles per website

    def parse(self, response):
        if self.article_count >= self.MAX_ARTICLES or self.found_old_articles:
            self.crawler.engine.close_spider(self, "Reached article limit or found older articles without hitting 10.")
            return  

        articles_in_timeframe = 0  # Track valid articles found in this page

        for article in response.css(search_terms['article']):
            if self.article_count >= self.MAX_ARTICLES:
                break  

            link = article.css(search_terms['link']).get()
            if link:
                articles_in_timeframe += 1
                yield Request(link, callback=self.parse_article, priority=1)

        # If no new articles were found in this request, stop scraping
        if articles_in_timeframe == 0:
            self.found_old_articles = True
            self.crawler.engine.close_spider(self, "Found older articles without reaching 10. Stopping.")
            return  

        self.INCREMENT += 1
        next_page = f"{main_url}{self.INCREMENT}"
        yield response.follow(next_page, callback=self.parse)

    def parse_article(self, response):
        if self.article_count >= self.MAX_ARTICLES:
            return  

        updated = response.css(search_terms['updated']).get()
        updated = updated.strip()
        updated = updated.split(" ")[0]
        updated = updated.replace("/", "-")
        updated = datetime.strptime(updated, "%d-%m-%Y")
        print("updated: ",  updated)
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
            self.article_count += 1  

        else:
            # If we find an old article and haven't hit 10, stop scraping
            self.found_old_articles = True
            self.crawler.engine.close_spider(self, "Found older articles without reaching 10. Stopping.")

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(CamaraNoticiasSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.upload_data, signal=spider_closed)
        return spider

    def upload_data(self, spider):
        file_path = f"Spiders/Results/{self.name}_{timestamp}.json"
        if not os.path.isfile(file_path):
            with open(file_path, "w") as f:
                json.dump([], f)

        with open(file_path, "r") as f:
            file_data = json.load(f)

        data_dicts = [item.to_dict() for item in self.data]

        file_data.extend(data_dicts)

        with open(file_path, "w") as f:
            json.dump(file_data, f, ensure_ascii=False)

        # upload = requests.post(f"{os.environ['API_URL']}{site_id}", json={"news": file_data})
        # print("upload: ", upload)
