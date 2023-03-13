import datetime
import logging
import os
import requests
import json
import pandas as pd
import numpy as np
import azure.functions as func
from collections.abc import MutableMapping
import pandas as pd

def flatten_dict(d: MutableMapping, sep: str= '.') -> MutableMapping:
    if len(d.keys()) == 0:
        return {}
    [flat_dict] = pd.json_normalize(d, sep=sep).to_dict(orient='records')
    return flat_dict

def get_all_data(token_id, cmc_id):
    logging.info(cmc_id)
    headers = {'X-CMC_PRO_API_KEY': os.environ.get("coin-market-cap-token")}
    try:
        res = requests.get(
                    url="https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest"+"?id="+','.join(map(str, cmc_id)),
                    headers=headers
            ).json()
    except Exception as e:
        logging.error(f"Encountered an exception when fetching Coin Market Cap data for token {token_id} -- {str(e)}")
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
    coin_data['market_capitalization_fully_diluted'] = None
    coin_data['trading_volume_24h'] = None
    coin_data['max_supply'] = None
    coin_data['total_supply'] = None
    
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
        if tokens[token]['cmc_id']:
            data = get_all_data(token_id=token, cmc_id=[tokens[token]['cmc_id']])
            if data == {}:
                all_coin_data.append(get_empty_coin_data(token))
                continue
            
            coin_data={}
            coin_data['token'] = token
            coin_data['current_price_usd']= data[f"data.{tokens[token]['cmc_id']}.quote.USD.price"] if  f"data.{tokens[token]['cmc_id']}.quote.USD.price" in data.keys() else None
            coin_data['market_capitalization_fully_diluted']= data[f"data.{tokens[token]['cmc_id']}.quote.USD.fully_diluted_market_cap"] if  f"data.{tokens[token]['cmc_id']}.quote.USD.fully_diluted_market_cap" in data.keys() else None
            coin_data['trading_volume_24h']= data[f"data.{tokens[token]['cmc_id']}.quote.USD.volume_24h"] if  f"data.{tokens[token]['cmc_id']}.quote.USD.volume_24h" in data.keys() else None
            coin_data['max_supply']= data[f"data.{tokens[token]['cmc_id']}.max_supply"] if  f"data.{tokens[token]['cmc_id']}.max_supply" in data.keys() else None
            coin_data['total_supply']= data[f"data.{tokens[token]['cmc_id']}.total_supply"] if  f"data.{tokens[token]['cmc_id']}.total_supply" in data.keys() else None
            coin_data["timestampz"] = (
                datetime.datetime.utcnow()
                .replace(tzinfo=datetime.timezone.utc)
                .isoformat()
            )
            all_coin_data.append(coin_data)
            logging.info(coin_data)
        else:
            logging.error(f"Found no coin market cap ID for token {token}")
    
    # update_time = res.json()['status']['timestamp']
        
    #Formatting all responses    
    df = pd.DataFrame(all_coin_data)  # dataframing because this is easier
    df.set_index("token", inplace=True)
    update_time = df[["timestampz"]].to_dict()
    df.drop(columns=["timestampz"], inplace=True)
    df.replace(np.nan, None, inplace=True)
    df.replace('N/A', None, inplace=True)
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
                "source": "CoinMarketCap",
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
                "source": "CoinMarketCap",
                # "GUID_functions": context.invocation_id,
            }
            for k, v in df_dict[topic].items()
            if v is None
        ]

    with open('log/missing/cmc.json', 'w') as f:
         f.write(json.dumps(missing_messages))

    logging.info(json.dumps(messages))
    msg.set(json.dumps(messages))