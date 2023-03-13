import datetime
import logging
import requests
import json
import time
import pandas as pd
import numpy as np
import azure.functions as func
from collections.abc import MutableMapping
import pandas as pd
import time

def flatten_dict(d: MutableMapping, sep: str= '.') -> MutableMapping:
    if len(d.keys()) == 0:
        return {}
    [flat_dict] = pd.json_normalize(d, sep=sep).to_dict(orient='records')
    return flat_dict

def get_all_data(token_id, cg_id):
    try:
        res = requests.get(
                    url=f"https://api.coingecko.com/api/v3/coins/{cg_id}"
                ).json()
    except Exception as e:
        logging.error(f"Encountered an exception when fetching Coin Gecko data for token {token_id}")
        res = {}
    return flatten_dict(dict(res))

def get_empty_coin_data(coin):
    coin_data = {}
    coin_data['token'] = coin
    coin_data["timestampz"] = (
        datetime.datetime.utcnow()
        .replace(tzinfo=datetime.timezone.utc)
        .isoformat()
    )
    coin_data['current_price_usd'] = None
    coin_data['total_supply'] = None
    coin_data['fully_diluted_valuation'] = None
    coin_data['max_supply'] = None
    coin_data['fully_diluted_valuation_calculated'] = None
    coin_data['total_volume'] = None
    coin_data['all_time_high'] = None
    coin_data['repos_url'] = None
    coin_data['subreddit_url'] = None
    coin_data['twitter_screen_name'] = None
    
    return coin_data

def main(mytimer: func.TimerRequest, msg: func.Out[str]) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()
    
    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
        
    with open('tokens.json', 'r') as f:
                tokens = json.load(f)

    all_coin_data = []
    for token in tokens.keys():
        if tokens[token]["cg_id"]:
            logging.info(f"Fetching CoinGecko data for token {token}")
            coin_data = {}
            coin_data["token"] = token
            cg_id = tokens[token]["cg_id"]
            
            data = get_all_data(token_id=token, cg_id=cg_id)

            if data == {}:
                all_coin_data.append(get_empty_coin_data(token))
                continue

            coin_data['current_price_usd'] = data['market_data.current_price.usd'] if 'market_data.current_price.usd' in data.keys() else None
            coin_data['total_supply'] = data['market_data.total_supply'] if 'market_data.total_supply' in data.keys() else None
            coin_data['fully_diluted_valuation'] = data['market_data.fully_diluted_valuation.usd'] if 'market_data.fully_diluted_valuation.usd' in data.keys() else None
            coin_data['max_supply'] = data['market_data.max_supply'] if 'market_data.max_supply' in data.keys() else None
            coin_data['fully_diluted_valuation_calculated'] = int(coin_data['max_supply']) * coin_data['current_price_usd'] if coin_data['current_price_usd'] and  coin_data['max_supply'] else None
            coin_data['total_volume'] = data['market_data.total_volume'] if 'market_data.total_volume' in data.keys() else None
            coin_data['all_time_high'] = data['market_data.ath'] if 'market_data.ath' in data.keys() else None
            coin_data['repos_url'] = data['links.repos_url.github'] if 'links.repos_url.github' in data.keys() else None
            coin_data['subreddit_url'] = data['links.subreddit_url'] if 'links.subreddit_url' in data.keys() else None
            coin_data['twitter_screen_name'] = data['links.twitter_screen_name'] if 'links.twitter_screen_name' in data.keys() else None


            coin_data["timestampz"] = (
                datetime.datetime.utcnow()
                .replace(tzinfo=datetime.timezone.utc)
                .isoformat()
            )
            
            all_coin_data.append(coin_data)
            logging.info(coin_data)
            time.sleep(4)
            
    #Formatting all responses    
    df = pd.DataFrame(all_coin_data)  # dataframing because this is easier
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
                "source": "Coin_Gecko",
                # "GUID_functions": context.invocation_id,
            }
            for k, v in df_dict[topic].items()
            if v is None
        ]

    with open('log/missing/cg.json', 'w') as f:
         f.write(json.dumps(missing_messages))
         
    c = msg.set(json.dumps(messages))
    logging.info(c)
    return c