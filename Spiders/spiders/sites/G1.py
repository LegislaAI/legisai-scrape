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

today = date.today().strftime("%d/%m/%Y")
today = datetime.strptime(today, "%d/%m/%Y")

search_limit = date.today() - timedelta(days=1)
search_limit = datetime.strptime(search_limit.strftime("%d/%m/%Y"), "%d/%m/%Y")

with open("/home/scrapeops/intersites-scrape/Spiders/CSS_Selectors/G1.json") as f:
    search_terms = json.load(f)

site_id = "a5ce174d-dff8-4c64-8e3f-c1c80926a1b4"

class G1Spider(scrapy.Spider):
    name = "G1"
    allowed_domains = ["g1.globo.com"]
    start_urls = ["https://g1.globo.com/economia/"]
    INCREMENT = 1
    data = []
    article_count = 0  # Added counter
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
        next_page = f"https://g1.globo.com/economia/index/feed/pagina-{self.INCREMENT}.ghtml"
        yield response.follow(next_page, callback=self.parse)

    def parse_article(self, response):
        if self.article_count >= self.MAX_ARTICLES:
            return  # Stop parsing articles if limit is reached

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
            self.article_count += 1  # Increment article count

        else:
            # If we find an old article and haven't hit 10, stop scraping
            self.found_old_articles = True
            self.crawler.engine.close_spider(self, "Found older articles without reaching 10. Stopping.")

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(G1Spider, cls).from_crawler(crawler, *args, **kwargs)
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