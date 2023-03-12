import datetime
import logging
import requests
import json
import time
import pandas as pd
import numpy as np
import os
import azure.functions as func

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
    #all_coins={}
    all_coins_list = []
    with open(f"tokens.json", 'r') as f:
        coins = json.load(f)
        
        for coin_key in coins.keys():
            cg_id = coins[coin_key]['cg_id']
            logging.info(f"Fetching Messari data for token {coin_key}")
            messari_symbol = coins[coin_key]['messari_symbol']
            
            cg_data = requests.get(url=f"https://api.coingecko.com/api/v3/coins/{cg_id}")
            cg_data.raise_for_status()
            
            cg_data = dict(cg_data.json())

            assert cg_data is not None, f"Not able to fetch info from coingecko for token {coin_key}"

            messari_data = call_api(url=f"https://data.messari.io/api/v1/assets/{messari_symbol}/metrics?fields=supply/circulating,market_data/price_usd")
            
            if 'market_data' in cg_data.keys():
                total_supply = float(cg_data['market_data']['total_supply']) if cg_data['market_data']['total_supply'] else None
            else:
                logging.error(f"Unable to fetch total supply from coin gecko for token {coin_key}")

            if 'data' in messari_data.keys():
                circulating = float(messari_data['data']['supply']['circulating']) if messari_data['data']['supply']['circulating'] else None
                price = float(messari_data['data']['price_usd']) if messari_data['data']['price_usd'] else None
            else:
                logging.error(f"Got no Messari data for token {coin_key}")
                
            #all_coins[coin_key] = {
            #    'circulating_supply': circulating,
            #    'circulating_supply_percentage': circulating / total_supply,
            #    'tokens_staked': total_supply - circulating,
            #    'assets_staked_on_chain': (total_supply - circulating) * price
            #}

            all_coins_list.append({
                'token': coin_key,
                'circulating_supply': circulating,
                'circulating_supply_percentage': circulating / total_supply,
                'tokens_staked': total_supply - circulating,
                'assets_staked_on_chain': (total_supply - circulating) * price
            })
            
            
    return all_coins_list

def main(mytimer: func.TimerRequest, msg: func.Out[str]) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()
    
    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)

    metrics = []

    r = get_all_metrics()
            
    #Formatting all responses    
    df = pd.DataFrame(r)  # dataframing because this is easier
    df.set_index("token", inplace=True)
    update_time = df[["timestampz"]].to_dict()
    df.drop(columns=["timestampz"], inplace=True)
    df.replace(np.nan, None, inplace=True)
    df_dict = df.to_dict()
    
    #preparing upload
    messages = {}

    for topic in df_dict.keys():
        messages[topic] = [
            {
                "token": k,
                "value": v,
                "timestampz": update_time["timestampz"][k],
                "source": "Coin_Gecko",
                # "GUID_functions": context.invocation_id,
            }
            for k, v in df_dict[topic].items()
            if v is not None
        ]

    c = msg.set(json.dumps(messages))
    logging.info(c)
    return c