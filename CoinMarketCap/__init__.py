import datetime
import logging
import os
import requests
import json
import time
import pandas as pd
import numpy as np
import azure.functions as func
from collections.abc import MutableMapping
import pandas as pd
from src.kafka_producer import Df101KafkaProducer
from src.utils import write_message_to_json, flatten_dict, get, format_response, publish_to_kafka, get_empty_coin_data, populate_coin_data

from src.schemas import coinmarketcap_schema
from src.key_mappings import coinmarketcap_map
from jsonschema import validate
import json

#with open('schema.json', 'r') as file:
#    schema = json.load(file)


def prepare_data(id):
    logging.info(id)
    headers = {'X-CMC_PRO_API_KEY': os.environ.get("coin-market-cap-token")}

    res = get(url="https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest"+"?id="+','.join(map(str, id)), headers=headers)
    
    res = res['data'][str(id[0])]
    res = flatten_dict(dict(res))
    
    with open('function_logs/datapoints/cmc.txt', 'w') as f:
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
        
    integration_id = 'cmc_id'
    integration_name = 'CoinMarketCap'

    all_coin_data = []
    for coin in coins.keys():
        if coins[coin]['cmc_id']:
            data = prepare_data(id=[coins[coin][integration_id]])
            coin_data = get_empty_coin_data(coin=coin, schema=coinmarketcap_schema)
            coin_data = populate_coin_data(coin_data=coin_data, api_data=data, key_mapping=coinmarketcap_map)
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

    publish_to_kafka(messages)
    


    #with open('function_logs/missing/cmc.json', 'w') as f:
    #     f.write(json.dumps(missing_messages))

    # write_message_to_json(missing_messages, f'function_logs/missing/{integration_name}.json')
    # write_message_to_json(messages, f'function_logs/successful/{integration_name}.json')

    msg.set(json.dumps(messages))