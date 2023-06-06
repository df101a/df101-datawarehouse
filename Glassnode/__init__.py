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
import os
from src.kafka_producer import Df101KafkaProducer
from jsonschema import validate
from src.utils import write_message_to_json, flatten_dict, get, format_response, publish_to_kafka, get_empty_coin_data, populate_coin_data
from src.schemas import glassnode_schema
from src.key_mappings import glassnode_map

with open('schema.json', 'r') as file:
    schema = json.load(file)

def get_all_data(token_id):
    data = {}

    glassnode_token = os.environ.get('glassnode-api-key')
    params = {'a': token_id, 'api_key': glassnode_token}

    endpoints = [
        'https://api.glassnode.com/v1/metrics/market/price_drawdown_relative',
        "https://api.glassnode.com/v1/metrics/eth2/active_validators_count",
        "https://api.glassnode.com/v1/metrics/supply/inflation_rate",
        "https://api.glassnode.com/v1/metrics/addresses/count",
        "https://api.glassnode.com/v1/metrics/fees/gas_price_mean"
    ]

    keys = ['drop_from_ath', 'active_validators_count', 'inflation_rate', 'wallet_count', 'gas_price_mean']
    idx = 0
    for endpoint in endpoints:
        res = get(url=endpoint, params=params)
        if res != {}:
            data[keys[idx]] = res[-1]['v'] 
        idx += 1

    return data

def main(mytimer: func.TimerRequest, msg: func.Out[str]) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()
    
    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
        
    with open('tokens.json', 'r') as f:
        coins = json.load(f)

    integration_name = 'Glassnode'
    all_coin_data = []
    for coin in coins.keys():
        coin_data = get_empty_coin_data(coin=coin, schema=glassnode_schema)
        logging.info(f"Fetching token data for {coin} from {integration_name}...({len(all_coin_data)}/{len(coins.keys())})")
        data = get_all_data(coin)

        coin_data = populate_coin_data(coin_data=coin_data, api_data=data, key_mapping=glassnode_map)

        logging.info(coin_data)
        all_coin_data.append(coin_data.copy())
        logging.info(f"Fetching token data for {coin} from {integration_name}...({len(all_coin_data)}/{len(coins.keys())})")
        
        time.sleep(10)

    messages, missing_messages = format_response(coin_data=all_coin_data, integration_name = integration_name)
            
    #for k,v in messages.items():
    #    for m in v:
    #        try:
    #            validate(m, schema)
    #        except:
    #            ("Wrongly formatted schema found:" +m)

    publish_to_kafka(messages)
    write_message_to_json(missing_messages, f'function_logs/missing/{integration_name}.json')
    write_message_to_json(messages, f'function_logs/successful/{integration_name}.json')
    c = msg.set(json.dumps(messages))
    logging.info(c)


    return c