import requests
import json
import os

file_name = "Spiders/Results/CamaraNoticias_1751411220.900963.json"

with open(file_name) as f:
    file_data = json.load(f)

upload = requests.post(f"{os.environ['API_URL']}/news/scrape?type=PARLIAMENT", json={"records": file_data})
print(upload.text)