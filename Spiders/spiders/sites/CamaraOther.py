from scrapy.signals import spider_closed
from ...items import otherItem
from datetime import datetime
from bs4 import BeautifulSoup
import requests
import scrapy
import json
import os
from dotenv import load_dotenv

load_dotenv()

now = datetime.now()
timestamp = datetime.timestamp(now)

# Default to current year if not set
year = os.environ.get('YEAR', str(datetime.now().year))

class CamaraOtherSpider(scrapy.Spider):
    name = "CamaraOther"
    allowed_domains = ["camara.leg.br"]
    data = []

    def start_requests(self):
        # Make the API request here
        api_url = os.environ.get('API_URL')
        if not api_url:
            self.logger.error("API_URL not set")
            return

        self.logger.info(f"Fetching politician IDs from {api_url}/politician/ids")
        try:
            request = requests.get(f"{api_url}/politician/ids")
            if request.status_code != 200:
                self.logger.error(f"Failed to fetch IDs: {request.status_code}")
                return

            response_data = request.json()
            ids = response_data.get('ids', [])  # Extract the array from the 'ids' key
            self.logger.info(f"Successfully fetched {len(ids)} politician IDs")
    
            # Generate URLs and create requests
            for politician_id in ids:
                url = f"https://www.camara.leg.br/deputados/{politician_id}?ano={year}"
                yield scrapy.Request(url=url, callback=self.parse, meta={'politician_id': politician_id})
        except Exception as e:
            self.logger.error(f"Error fetching IDs: {str(e)}")

    def parse(self, response):
        politician_id = response.meta['politician_id']   # Get all table rows
        url = f"https://www.camara.leg.br/deputados/{politician_id}/pessoal-gabinete?ano={year}"
        # self.logger.info(f"Parsing other info for Politician ID: {politician_id}")
        rows = response.css("section.recursos-deputado ul li").getall()
        parsed_rows = []

        for row_html in rows:
            soup = BeautifulSoup(row_html, "html.parser")

            # Extract title and info separately for better control
            title_elem = soup.find('h3', class_='beneficio__titulo')
            info_elem = soup.find(['a', 'span'], class_='beneficio__info')

            if title_elem and info_elem:
                title = title_elem.get_text().strip().replace('?', '')
                info = info_elem.get_text().strip()

                # Clean up whitespace
                title = ' '.join(title.split())
                info = ' '.join(info.split())

                # Combine title and info
                full_text = f"{title} {info}"
                parsed_rows.append(full_text)

        # Initialize all benefit fields with None
        all_benefits = {
            'politicianId': None,
            'year': None,
            'contractedPeople': None,
            'grossSalary': None,
            'functionalPropertyUsage': None,
            'housingAssistant': None,
            'trips': None,
            'diplomaticPassport': None,
            'url': None
        }

        # Map the parsed rows to specific benefit fields
        for text in parsed_rows:
            if text.startswith('Pessoal de gabinete') or text.startswith('Pessoal de Gabinete'):
                all_benefits['contractedPeople'] = text
            elif text.startswith('Salário mensal bruto'):
                all_benefits['grossSalary'] = text
            elif text.startswith('Imóvel funcional'):
                all_benefits['functionalPropertyUsage'] = text
            elif text.startswith('Auxílio-moradia'):
                all_benefits['housingAssistant'] = text
            elif text.startswith('Viagens em missão oficial'):
                all_benefits['trips'] = text
            elif text.startswith('Passaporte diplomático'):
                all_benefits['diplomaticPassport'] = text

        all_benefits['politicianId'] = politician_id
        all_benefits['year'] = year
        all_benefits['url'] = url

        # Create a single comprehensive item
        item = otherItem(**all_benefits)
        yield item
        self.data.append(item)

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(CamaraOtherSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.upload_data, signal=spider_closed)
        return spider

    def upload_data(self, spider):
        results_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "Results")
        if not os.path.exists(results_dir):
            os.makedirs(results_dir)
            
        file_path = os.path.join(results_dir, f"{self.name}_{timestamp}.json")
        
        if not os.path.isfile(file_path):
            with open(file_path, "w") as f:
                json.dump([], f)

        with open(file_path, "r") as f:
            file_data = json.load(f)

        data_dicts = [item.to_dict() for item in self.data]
        file_data.extend(data_dicts)

        with open(file_path, "w", encoding='utf-8') as f:
            json.dump(file_data, f, ensure_ascii=False)

        api_url = os.environ.get('API_URL')
        if not api_url:
            spider.logger.error("API_URL not set")
            return

        spider.logger.info(f"Uploading {len(file_data)} records to API at {api_url}/politician-finance/other")
        try:
            response = requests.post(f"{api_url}/politician-finance/other", json=file_data)
            if response.status_code >= 200 and response.status_code < 300:
                spider.logger.info(f"Upload successful: {response.status_code} - {response.text}")
            else:
                spider.logger.error(f"Upload failed: {response.status_code} - {response.text}")
        except Exception as e:
            spider.logger.error(f"Error during upload: {str(e)}")