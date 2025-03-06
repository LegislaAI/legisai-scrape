import requests
import json

file_name = "Spiders/Results/Brasil247_1739203997.740861.json"

with open(file_name) as f:
    file_data = json.load(f)

upload = requests.post("https://api.vivadicas.com/news/scrape", json={"news": file_data})
print(upload.text)