import requests
import os
import json
import time
from dotenv import load_dotenv
load_dotenv()


metrics = []
def call_api(url):
    token = os.environ.get("messari-api-key")
    res = requests.get(
            url=url, 
            headers = {"X-Api-Key": f"{token}"}
        )
    res.raise_for_status()
    return res.json()

def populate_tokens_json():
    with open("tokens.json", 'r') as f:
        tokens = json.load(f)

    for token_key in tokens.keys():
        symbol = tokens[token_key]["cg_symbol"]
        res = call_api(f"https://data.messari.io/api/v1/assets/{symbol}")
        tokens[token_key]["messari_symbol"] = res['data']['symbol']
        tokens[token_key]["messari_name"] = res['data']['name']
        tokens[token_key]["messari_slug"] = res['data']['slug']
        print(symbol)
        time.sleep(2)

    with open(f"tokens.json", 'w') as f:
        f.write(json.dumps(tokens))


def get_all_metrics():
    all_coins={}
    with open(f"tokens.json", 'r') as f:
        coins = json.load(f)
        for coin_key in coins.keys():
            cg_id = coins[coin_key]['cg_id']
            
            messari_symbol = coins[coin_key]['messari_symbol']
            
            cg_data = requests.get(url=f"https://api.coingecko.com/api/v3/coins/{cg_id}")
            cg_data.raise_for_status()
            
            cg_data = dict(cg_data.json())
            print(cg_data)
            assert cg_data is not None, f"Not able to fetch info from coingecko {cg_data}"
            total_supply = float(cg_data['market_data']['total_supply'])

            messar_data = call_api(url=f"https://data.messari.io/api/v1/assets/{messari_symbol}/metrics?fields=supply/circulating, market_data/price_usd")
            circulating = float(messar_data['data']['supply']['circulating'])
            price = float(messar_data['data']['price_usd'])

            all_coins[coin_key] = {
                'circulating_supply': circulating,
                'circulating_supply_percentage': circulating / total_supply,
                'tokens_staked': total_supply - circulating,
                'assets_staked_on_chain': (total_supply - circulating) * price
            }
            
            break
    return all_coins

r = get_all_metrics()
print(r)

    
    