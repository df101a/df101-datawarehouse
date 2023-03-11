import requests
from dotenv import load_dotenv
load_dotenv()
import os
import json

#token = os.environ.get('glassnods-api-key')
#res = requests.get(url="https://api.glassnode.com/v2/metrics/endpoints", headers={"X-Api-Key": token})
#res.raise_for_status()

#with open('tmp.json', 'w') as f:
#    f.write(json.dumps(res.json()))


with open('tmp.json', 'r') as f:
    data = json.load(f)
    for p in data:
        print(p['path'])