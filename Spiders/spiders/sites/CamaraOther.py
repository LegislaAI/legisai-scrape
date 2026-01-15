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

        self.logger.info(f"Fetching politician IDs from {api_url}/politician/ids")
        try:
            request = requests.get(f"{api_url}/politician/ids")
            if request.status_code != 200:
                self.logger.error(f"Failed to fetch IDs: {request.status_code}")
                return

            response_data = request.json()
            ids = response_data.get('ids', [])  # Extract the array from the 'ids' key
            self.logger.info(f"Successfully fetched {len(ids)} politician IDs to scrape")
    
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
            
            response = requests.post(f"{api_url}/politician-finance/other", json=data_dicts)
            
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
        spider = super(CamaraOtherSpider, cls).from_crawler(crawler, *args, **kwargs)
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
