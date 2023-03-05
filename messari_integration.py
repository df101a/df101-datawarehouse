import requests
import os
import json
from dotenv import load_dotenv
load_dotenv()

def call_api(url):
    token = os.environ.get("messari-api-key")
    res = requests.get(
            url=url, 
            headers = {"X-Api-Key": f"{token}"}
        )
    res.raise_for_status()
    return res.json()


with open("tokens.json", 'r') as f:
    tokens = json.load(f)

import time
for i in range(1,100):
    res = call_api(f"https://data.messari.io/api/v2/assets?page={i}&fields=id,slug,symbol&limit=500")
    print(len(res['data']))
    time.sleep(0.5)
with open("dump.json", 'w') as f:
    f.write(json.dumps(res))