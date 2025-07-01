from scrapy.signals import spider_closed
from ...items import roleItem
from datetime import datetime
from bs4 import BeautifulSoup
import requests
import scrapy
import json
import os

now = datetime.now()
timestamp = datetime.timestamp(now)

year = os.environ['YEAR']

class CamaraRoleSpider(scrapy.Spider):
    name = "CamaraRole"
    allowed_domains = ["camara.leg.br"]
    data = []

    def start_requests(self):
        # Make the API request here
        request = requests.get(f"{os.environ['API_URL']}/politician/ids")
        response_data = request.json()
        ids = response_data['ids']  # Extract the array from the 'ids' key

        # Generate URLs and create requests
        for politician_id in ids:
            url = f"https://www.camara.leg.br/deputados/{politician_id}?ano={year}"
            yield scrapy.Request(url=url, callback=self.parse, meta={'politician_id': politician_id})

    def parse(self, response):
        politician_id = response.meta['politician_id']   # Get all table rows
        roleData = {
            'politicianId': None,
            'year': None,
            'name': None,
            'description': None,
            'date': None,
        }

        role = response.css('section.cargos-deputado ul.cargos-deputado-container').getall()
        for r in role:
            soup = BeautifulSoup(r, "html.parser")
            name = soup.find("div", class_='cargos-deputado__cargo').text
            description = soup.find('div', class_='titulo-cargos-deputado-todos').text
            description = description.strip()
            date = soup.find('span', class_='cargos-deputado__periodo').text
            date = ' '.join(date.split())  # Remove extra whitespace

            roleData['politicianId'] = politician_id
            roleData['year'] = year
            roleData['name'] = name
            roleData['description'] = description
            roleData['date'] = date

            # Create a single comprehensive item
            item = roleItem(**roleData)
            yield item
            self.data.append(item)

        # Close spider AFTER processing all rows
        self.crawler.engine.close_spider(self, "Todas as informações foram coletadas.")
    
    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(CamaraRoleSpider, cls).from_crawler(crawler, *args, **kwargs)
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

        file_name = requests.post(f"{os.environ['API_URL']}/politician-position", json={"records": file_data})
        print("upload: ", file_name)