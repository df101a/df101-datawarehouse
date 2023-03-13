import datetime
import logging
import requests
import json
import time
import pandas as pd
import numpy as np
import os
import azure.functions as func
from collections.abc import MutableMapping
import pandas as pd

def flatten_dict(d: MutableMapping, sep: str= '.') -> MutableMapping:
    if len(d.keys()) == 0:
        return {}
    [flat_dict] = pd.json_normalize(d, sep=sep).to_dict(orient='records')
    return flat_dict


def get_all_data(token_id, cg_id, messari_symbol):
    data = {
        'cg': None,
        'messari':None
    }
    try:
        cg_data = requests.get(url=f"https://api.coingecko.com/api/v3/coins/{cg_id}")
        cg_data.raise_for_status()
        data['cg'] = dict(cg_data.json())
    except Exception as ex:
        logging.error(f'Encountered an error when fetching data from CoinGecko for token {token_id} -- {str(ex)}')

    try:
        token = os.environ.get("messari-api-key")
        messari_data = requests.get(
                url=f"https://data.messari.io/api/v1/assets/{messari_symbol}/metrics?fields=supply/circulating,market_data/price_usd", 
                headers = {"X-Api-Key": f"{token}"}
            )
        messari_data.raise_for_status()
        data['messari'] = dict(messari_data.json())
    except Exception as ex:
        logging.error(f'Encountered an error when fetching Messari data -- {str(ex)}')
    time.sleep(4)
    return flatten_dict(data)

def get_all_metrics():
    #all_coins={}
    all_coins_list = []
    with open(f"tokens.json", 'r') as f:
        coins = json.load(f)
        
        for coin_key in coins.keys():
            logging.info(f"Fetching Messari data for token {coin_key}")
            messari_symbol = coins[coin_key]['messari_symbol']
            cg_id = coins[coin_key]['cg_id']

            coin_data = get_all_data(token_id=coin_key, cg_id=cg_id, messari_symbol=messari_symbol)

            # Extract all the data we need
            total_supply = coin_data['cg.market_data.total_supply'] if 'cg.market_data.total_supply' in coin_data.keys() else None
            circulating = coin_data['messari.data.supply.circulating'] if 'messari.data.supply.circulating' in coin_data.keys() else None
            price = coin_data['messari.data.market_data.price_usd'] if 'messari.data.market_data.price_usd' in coin_data.keys() else None

            # Type casting
            total_supply = float(total_supply) if total_supply else None 
            circulating = float(circulating) if circulating else None
            price = float(price) if price else None

            token_data = {
                'token': coin_key,
                'circulating_supply': circulating,
                'circulating_supply_percentage': circulating / total_supply if circulating and total_supply else None,
                'tokens_staked': total_supply - circulating if circulating and total_supply else None,
                'assets_staked_on_chain': (total_supply - circulating) * price if total_supply and price and circulating else None
            }

            token_data["timestampz"] = (
                datetime.datetime.utcnow()
                .replace(tzinfo=datetime.timezone.utc)
                .isoformat()
            )

            all_coins_list.append(token_data)
            logging.info(token_data)
            
    return all_coins_list

def get_empty_coin_data(coin):
    coin_data = {}
    coin_data['token'] = coin
    coin_data["timestampz"] = (
        datetime.datetime.utcnow()
        .replace(tzinfo=datetime.timezone.utc)
        .isoformat()
    )
    coin_data['circulating_supply'] = None
    coin_data['circulating_supply_percentage'] = None
    coin_data['tokens_staked'] = None
    coin_data['assets_staked_on_chain'] = None
    
    return coin_data

def main(mytimer: func.TimerRequest, msg: func.Out[str]) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()
    
    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)

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
    missing_messages = {}

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

    for topic in df_dict.keys():
        missing_messages[topic] = [
            {
                "token": k,
                "value": v,
                "timestampz": update_time["timestampz"][k],
                "source": "Messari",
                # "GUID_functions": context.invocation_id,
            }
            for k, v in df_dict[topic].items()
            if v is None
        ]

    with open('log/missing/messari.json', 'w') as f:
         f.write(json.dumps(missing_messages))

    c = msg.set(json.dumps(messages))
    