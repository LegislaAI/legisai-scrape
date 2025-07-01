import scrapy
import json
from ...items import roleItem
from scrapy.signals import spider_closed
from datetime import date, datetime, timedelta
import os
from bs4 import BeautifulSoup

now = datetime.now()
timestamp = datetime.timestamp(now)

id = os.environ['POLITICIAN_ID']

year = os.environ['YEAR']

main_url = f"https://www.camara.leg.br/deputados/{id}?ano={year}"

class CamaraRoleSpider(scrapy.Spider):
    name = "CamaraRole"
    allowed_domains = ["camara.leg.br"]
    start_urls = [f"https://www.camara.leg.br/deputados/{id}?ano={year}"]
    data = []

    def parse(self, response):
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

            roleData['politicianId'] = id
            roleData['year'] = year
            roleData['name'] = name
            roleData['description'] = description
            roleData['date'] = date

            # Create a single comprehensive item
            item = roleItem(**roleData)
            yield item
            self.data.append(item)
        
        # Close spider AFTER processing all rows
        self.crawler.engine.close_spider(self, "Todos do gabinete foram coletados.")
    
    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(CamaraRoleSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.upload_data, signal=spider_closed)
        return spider

    def upload_data(self, spider):
        file_path = f"Spiders/Results/{self.name}_{id}_{timestamp}.json"
        if not os.path.isfile(file_path):
            with open(file_path, "w") as f:
                json.dump([], f)

        with open(file_path, "r") as f:
            file_data = json.load(f)

        data_dicts = [item.to_dict() for item in self.data]
        file_data.extend(data_dicts)

        with open(file_path, "w") as f:
            json.dump(file_data, f, ensure_ascii=False)