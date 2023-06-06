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
import os
from src.kafka_producer import Df101KafkaProducer
from jsonschema import validate
import json
from src.utils import write_message_to_json, flatten_dict, get, format_response, publish_to_kafka, get_empty_coin_data, populate_coin_data

from src.schemas import defilama_schema
from src.key_mappings import defilama_map

#with open('schema.json', 'r') as file:
#    schema = json.load(file)


def prepare_data():
    res = get(url=f"https://api.llama.fi/chains")
    
    a = {}
    for item in res:
        a[item['tokenSymbol']] = item
 
    return a

def main(mytimer: func.TimerRequest, msg: func.Out[str]) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
        
    with open('tokens.json', 'r') as f:
        coins = json.load(f)

    integration_id = 'defilama_symbol'
    integration_name = 'DefiLama'

    data = prepare_data()
    
    all_coin_data = []

    for coin in coins.keys():
        if coins[coin]["cg_id"]:
            logging.info(f"Fetching DefiLama data for token {coin}")
            
            coin_data = get_empty_coin_data(coin=coin, schema=defilama_schema)

            defilama_id = coins[coin][integration_id]

            coin_data = populate_coin_data(
                coin_data=coin_data, 
                api_data=data[coin] if coin in data.keys() else {},
                key_mapping=defilama_map
            )
            all_coin_data.append(coin_data.copy())
            logging.info(f"Fetching token data for {coin} from {integration_name}...({len(all_coin_data)}/{len(coins.keys())})")
        
            time.sleep(10)
            
    messages, missing_messages = format_response(all_coin_data, integration_name)
    publish_to_kafka(messages)
    #for k,v in messages.items():
    #    for m in v:
    #        try:
    #            validate(m, schema)
    #        except:
    #            ("Wrongly formatted schema found:" +m)
         
    write_message_to_json(missing_messages, f'function_logs/missing/{integration_name}.json')
    write_message_to_json(messages, f'function_logs/successful/{integration_name}.json')
    c = msg.set(json.dumps(messages))
    


    return c