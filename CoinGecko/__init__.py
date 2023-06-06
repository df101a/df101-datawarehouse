import datetime
import logging
import requests
import json
import time
import pandas as pd
import numpy as np
import azure.functions as func
import pandas as pd
import os

from jsonschema import validate
from src.schemas import coingecko_schema
from src.key_mappings import coingecko_map
from src.utils import write_message_to_json, flatten_dict, get, format_response, publish_to_kafka, get_empty_coin_data, populate_coin_data

#with open('schema.json', 'r') as file:
#    schema = json.load(file)


def prepare_data(cg_id):    
    res = get(url=f"https://api.coingecko.com/api/v3/coins/{cg_id}")
    
    res = flatten_dict(dict(res))
    
    with open('function_logs/datapoints/cg.txt', 'w') as f:
        for key in res.keys():
            f.write(f"{key}\n")

    return res  

def main(mytimer: func.TimerRequest, msg: func.Out[str]) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()
    
    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
        
    with open('tokens.json', 'r') as f:
        coins = json.load(f)

    integration_id = "cg_id"
    integration_name = "CoinGecko"

    all_coin_data = []
    for coin in coins.keys():

        if coins[coin][integration_id]:
            data = prepare_data(coins[coin][integration_id])
            coin_data = get_empty_coin_data(coin=coin, schema=coingecko_schema)
            coin_data = populate_coin_data(coin_data=coin_data, api_data=data, key_mapping=coingecko_map)
            all_coin_data.append(coin_data.copy())
            logging.info(f"Fetching token data for {coin} from {integration_name}...({len(all_coin_data)}/{len(coins.keys())})")
        
        time.sleep(10)

    messages, missing_messages = format_response(all_coin_data, integration_name)
    
    #for k,v in messages.items():
    #    for m in v:
    #        try:
    #            validate(m, schema)
    #        except:
    #            ("Wrongly formatted schema found:" +m)

    write_message_to_json(missing_messages, f'function_logs/missing/{integration_name}.json')
    write_message_to_json(messages, f'function_logs/successful/{integration_name}.json')

    publish_to_kafka(messages)
         
    c = msg.set(json.dumps(messages))
    logging.info(c)


    return c