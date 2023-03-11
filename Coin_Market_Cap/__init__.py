import datetime
import logging
import os
import requests
import json
import pandas as pd
import numpy as np


import azure.functions as func


def main(mytimer: func.TimerRequest, msg: func.Out[str]) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)

    with open('tokens.json', 'r') as f:
            tokens = json.load(f)
        
    coin_cmc_id = []
    for token in tokens.keys():
        if tokens[token]['cmc_id']:
            coin_cmc_id.append(tokens[token]['cmc_id'])
    
    headers = {'X-CMC_PRO_API_KEY': os.environ.get("coin-market-cap-token")}
    res = requests.get(url="https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest"+"?id="+','.join(map(str, coin_cmc_id)), headers=headers)
    update_time = res.json()['status']['timestamp']
    res = res.json()['data']
    
    
    
    token_cmc_id_mapping = pd.DataFrame().from_dict(tokens, orient='index')
    
    token_data=[]
    for coin_id in res.keys():
        coin_data={}
        coin_data['token'] = token_cmc_id_mapping.index[token_cmc_id_mapping['cmc_id']==int(coin_id)].to_list()[0]
        coin_data['current_price_usd']= res[coin_id]['quote']['USD']['price']
        coin_data['market_capitalization_fully_diluted']= res[coin_id]['quote']['USD']['fully_diluted_market_cap']
        coin_data['trading_volume_24h']= res[coin_id]['quote']['USD']['volume_24h']
        coin_data['max_supply']= res[coin_id]['max_supply']
        coin_data['total_supply']= res[coin_id]['total_supply']
        token_data.append(coin_data)
        
    #Formatting all responses    
    df = pd.DataFrame(token_data)  # dataframing because this is easier
    df.set_index("token", inplace=True)
    df.replace(np.nan, None, inplace=True)
    df.replace('N/A', None, inplace=True)
    df_dict = df.to_dict()
    
    #preparing upload
    messages = {}
    
    for topic in df_dict.keys():
        messages[topic] = [
            {
                "token": k,
                "value": v,
                "timestampz": update_time,
                "source": "CoinMarketCap",
                # "GUID_functions": context.invocation_id,
            }
            for k, v in df_dict[topic].items()
            if v is not None
        ]

    msg.set(json.dumps(messages))