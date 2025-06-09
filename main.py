import requests
import json
import os

file_name = "Spiders/Results/OndeAssistir_1749489255.856638.json"
site_id = "9b8af8dc-e9ef-41c6-b246-1cd734326081"


with open(file_name) as f:
    file_data = json.load(f)

upload = requests.post(f"{os.environ['API_URL']}{site_id}", json={"news": file_data})
print(upload.text)