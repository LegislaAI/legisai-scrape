from scrapy.signals import spider_closed
from ...items import generalItem
from datetime import datetime
from bs4 import BeautifulSoup
import requests
import scrapy
import json
import os

now = datetime.now()
timestamp = datetime.timestamp(now)

year = os.environ['YEAR']

class CamaraFinancialSpider(scrapy.Spider):
    name = "CamaraFinancial"
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
            self.data.append(item)

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(CamaraFinancialSpider, cls).from_crawler(crawler, *args, **kwargs)
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

        file_name = requests.post(f"{os.environ['API_URL']}/politician-finance/finance", json=file_data)
        print("upload: ", file_name)