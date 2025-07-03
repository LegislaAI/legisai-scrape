from scrapy.signals import spider_closed
from ...items import cabinetItem
from datetime import datetime
import requests
import scrapy
import json
import os

now = datetime.now()
timestamp = datetime.timestamp(now)

with open("/home/scrapeops/legisai-scrape/Spiders/CSS_Selectors/CamaraGabinete.json") as f:
    search_terms = json.load(f)

year = os.environ['YEAR']

class CamaraGabineteSpider(scrapy.Spider):
    name = "CamaraGabinete"
    allowed_domains = ["camara.leg.br"]
    data = []
    
    def start_requests(self):
        # Make the API request here
        request = requests.get(f"{os.environ['API_URL']}/politician/ids")
        response_data = request.json()
        ids = response_data['ids']  # Extract the array from the 'ids' key

        # Generate URLs and create requests
        for politician_id in ids:
            url = f"https://www.camara.leg.br/deputados/{politician_id}/pessoal-gabinete?ano={year}"
            yield scrapy.Request(url=url, callback=self.parse, meta={'politician_id': politician_id})


    def parse(self, response):
        politician_id = response.meta['politician_id']   # Get all table rows
        for row in response.css(search_terms['row']):
            # Extract text content, not the full element
            name = row.css(search_terms['name']).get()
            group = row.css(search_terms['group']).get()
            role = row.css(search_terms['role']).get()
            period = row.css(search_terms['period']).get()
            monthly = row.css(search_terms['monthly']).get()

            item = cabinetItem(
                politicianId=politician_id,
                year=year,
                name=name,
                group=group,
                role=role,
                period=period,
                monthly=monthly,
            )
            yield item
            self.data.append(item)

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(CamaraGabineteSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.upload_data, signal=spider_closed)
        return spider

    def upload_data(self, spider):
        file_path = f"/home/scrapeops/legisai-scrape/Spiders/Results/{self.name}_{timestamp}.json"
        if not os.path.isfile(file_path):
            with open(file_path, "w") as f:
                json.dump([], f)

        with open(file_path, "r") as f:
            file_data = json.load(f)

        data_dicts = [item.to_dict() for item in self.data]
        file_data.extend(data_dicts)

        with open(file_path, "w") as f:
            json.dump(file_data, f, ensure_ascii=False)