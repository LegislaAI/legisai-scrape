from scrapy.signals import spider_closed
from ...items import generalItem
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

class CamaraFinancialSpider(scrapy.Spider):
    name = "CamaraFinancial"
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
            rows = response.css("div.gasto__col tbody tr").getall()

            # Parse each row and extract text
            parsed_rows = []
            for row_html in rows:
                soup = BeautifulSoup(row_html, "html.parser")
                cells = soup.find_all('td')
                if len(cells) >= 3:
                    text = f"{cells[0].get_text().strip()} {cells[1].get_text().strip()} {cells[2].get_text().strip()}"
                    parsed_rows.append({
                        'label': cells[0].get_text().strip(),
                        'value': cells[1].get_text().strip(),
                        'percentage': cells[2].get_text().strip(),
                        'full_text': text
                    })

            # Find the split point (second occurrence of "Gasto")
            gasto_indices = [i for i, row in enumerate(parsed_rows) if row['label'] == 'Gasto']

            if len(gasto_indices) >= 2:
                split_index = gasto_indices[1]
                parliamentary_rows = parsed_rows[:split_index]
                cabinet_rows = parsed_rows[split_index:]
            else:
                # Fallback: if we can't find two "Gasto" entries, try to split by "Não utilizado"
                nao_utilizado_indices = [i for i, row in enumerate(parsed_rows) if row['label'] == 'Não utilizado']
                if len(nao_utilizado_indices) >= 2:
                    split_index = nao_utilizado_indices[1]
                    parliamentary_rows = parsed_rows[:split_index]
                    cabinet_rows = parsed_rows[split_index:]
                else:
                    parliamentary_rows = parsed_rows
                    cabinet_rows = []

            # Month mapping
            month_map = {
                'JAN': 'jan', 'FEV': 'feb', 'MAR': 'mar', 'ABR': 'apr',
                'MAI': 'may', 'JUN': 'jun', 'JUL': 'jul', 'AGO': 'aug',
                'SET': 'sep', 'OUT': 'oct', 'NOV': 'nov', 'DEZ': 'dec'
            }

            # Initialize all fields with None
            all_data = {
                'politicianId': None,
                'year': None,
                'usedParliamentaryQuota': None,
                'unusedParliamentaryQuota': None,
                'usedCabinetQuota': None,
                'unusedCabinetQuota': None
            }

            # Initialize all month fields
            for month in ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec']:
                all_data[f"{month}UsedParliamentaryQuota"] = None
                all_data[f"{month}CabinetQuota"] = None

            # Process parliamentary data
            for row in parliamentary_rows:
                if row['label'] == 'Gasto':
                    all_data['usedParliamentaryQuota'] = row['full_text']
                elif row['label'] == 'Não utilizado':
                    all_data['unusedParliamentaryQuota'] = row['full_text']
                elif row['label'] in month_map:
                    month_key = f"{month_map[row['label']]}UsedParliamentaryQuota"
                    all_data[month_key] = row['full_text']

            # Process cabinet data
            for row in cabinet_rows:
                if row['label'] == 'Gasto':
                    all_data['usedCabinetQuota'] = row['full_text']
                elif row['label'] == 'Não utilizado':
                    all_data['unusedCabinetQuota'] = row['full_text']
                elif row['label'] in month_map:
                    month_key = f"{month_map[row['label']]}CabinetQuota"
                    all_data[month_key] = row['full_text']

            # Create a single item with all data
            all_data['politicianId'] = politician_id
            all_data['year'] = year
            item = generalItem(**all_data)
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
            
            response = requests.post(f"{api_url}/politician-finance/finance", json=data_dicts)
            
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
        spider = super(CamaraFinancialSpider, cls).from_crawler(crawler, *args, **kwargs)
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
