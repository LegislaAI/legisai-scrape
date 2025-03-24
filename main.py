import requests
import json

file_name = "Spiders/Results/AutoEsporte_1741352288.269919.json"

with open(file_name) as f:
    file_data = json.load(f)

upload = requests.post("https://api.vivadicas.com/news/scrape/ab9a6448-3471-491f-9009-d7ec57daba54", json={"news": file_data})
print(upload.text)