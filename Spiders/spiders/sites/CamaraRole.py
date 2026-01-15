from scrapy.signals import spider_closed
from ...items import roleItem
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

class CamaraRoleSpider(scrapy.Spider):
    name = "CamaraRole"
    allowed_domains = ["camara.leg.br"]
    
    # Batch configuration
    BATCH_SIZE = 100
    batch_data = []
    all_data = []  # For local JSON backup

    def start_requests(self):
        # Make the API request here
        api_url = os.environ.get('API_URL')
        if not api_url:
            self.logger.error("API_URL not set")
            return
            
        try:
            request = requests.get(f"{api_url}/politician/ids")
            if request.status_code != 200:
                self.logger.error(f"Failed to fetch IDs: {request.status_code}")
                return

            response_data = request.json()
            ids = response_data.get('ids', [])  # Extract the array from the 'ids' key
            self.logger.info(f"Fetched {len(ids)} politician IDs to scrape")
    
            # Generate URLs and create requests
            for politician_id in ids:
                url = f"https://www.camara.leg.br/deputados/{politician_id}?ano={year}"
                yield scrapy.Request(url=url, callback=self.parse, meta={'politician_id': politician_id})
        except Exception as e:
            self.logger.error(f"Error fetching IDs: {str(e)}")

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
            
            # Add to batch and all_data for local backup
            self.batch_data.append(item)
            self.all_data.append(item)
            
            # Upload batch if size reached
            if len(self.batch_data) >= self.BATCH_SIZE:
                self._upload_batch()

    def _upload_batch(self):
        """Upload current batch to API"""
        if not self.batch_data:
            return
            
        api_url = os.environ.get('API_URL')
        if not api_url:
            self.logger.error("API_URL not set, cannot upload batch")
            return

        try:
            data_dicts = [item.to_dict() for item in self.batch_data]
            self.logger.info(f"Uploading batch of {len(data_dicts)} records...")
            
            response = requests.post(f"{api_url}/politician-position", json={"records": data_dicts})
            
            if response.status_code >= 200 and response.status_code < 300:
                self.logger.info(f"Batch upload success: {len(data_dicts)} records")
            else:
                self.logger.error(f"Batch upload failed: {response.status_code} - {response.text}")
        except Exception as e:
            self.logger.error(f"Error during batch upload: {str(e)}")
        finally:
            self.batch_data = []  # Clear batch regardless of success/failure
    
    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(CamaraRoleSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.upload_data, signal=spider_closed)
        return spider

    def upload_data(self, spider):
        # Upload any remaining data in the batch
        self._upload_batch()
        
        # Save local JSON backup
        results_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "Results")
        if not os.path.exists(results_dir):
            os.makedirs(results_dir)
            
        file_path = os.path.join(results_dir, f"{self.name}_{timestamp}.json")
        
        data_dicts = [item.to_dict() for item in self.all_data]
        with open(file_path, "w", encoding='utf-8') as f:
            json.dump(data_dicts, f, ensure_ascii=False)
        
        self.logger.info(f"Saved {len(data_dicts)} records to {file_path}")
