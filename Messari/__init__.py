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
from src.kafka_producer import Df101KafkaProducer
from jsonschema import validate
import json
from src.utils import write_message_to_json, flatten_dict, get, format_response, publish_to_kafka, get_empty_coin_data, populate_coin_data
from src.schemas import messari_schema
from src.key_mappings import messari_map
#with open('schema.json', 'r') as file:
#    schema = json.load(file)

def prepare_data(cg_id, messari_symbol):
    data = {
        'cg': None,
        'messari':None
    }

    cg_data = get(url=f"https://api.coingecko.com/api/v3/coins/{cg_id}")
    data['cg'] = dict(cg_data)

    messari_token = os.environ.get('messari_api_key')
    headers = {"X-Api-Key": f"{messari_token}"}
    messari_data = get(
        url=f"https://data.messari.io/api/v1/assets/{messari_symbol}/metrics?fields=supply/circulating,market_data/price_usd",
        headers=headers
    )
    
    data['messari'] = dict(messari_data)
    
    res = flatten_dict(data)

    return res




def main(mytimer: func.TimerRequest, msg: func.Out[str]) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()
    
    DUMP_DATAPOINTS = True
    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)

    all_coin_data = []
    integration_name = 'Messari'

    with open(f"tokens.json", 'r') as f:
        coins = json.load(f)
        
    for coin in coins.keys():
        logging.info(f"Fetching Messari data for token {coin}")
        logging.info(f"Fetching token data for {coin} from {integration_name}...({len(all_coin_data)}/{len(coins.keys())})")
        messari_symbol = coins[coin]['messari_symbol']
        cg_id = coins[coin]['cg_id']

        coin_data = get_empty_coin_data(coin=coin, schema=messari_schema)
        data = prepare_data(cg_id=cg_id, messari_symbol=messari_symbol)
        coin_data = populate_coin_data(coin_data=coin_data, api_data=data, key_mapping=messari_map)


        # Extract all the data we need
        total_supply = coin_data['total_supply']
        circulating = coin_data['circulating_supply']
        price = coin_data['price_usd']

        # Type casting
        total_supply = float(total_supply) if total_supply else None 
        circulating = float(circulating) if circulating else None
        price = float(price) if price else None

        
        coin_data['circulating_supply_percentage'] = circulating / total_supply if circulating and total_supply else None
        coin_data['tokens_staked'] = total_supply - circulating if circulating and total_supply else None
        coin_data['assets_staked_on_chain'] = (total_supply - circulating) * price if total_supply and price and circulating else None


        all_coin_data.append(coin_data.copy())
        logging.info(coin_data)
        time.sleep(10)
    
    messages, missing_messages = format_response(all_coin_data, integration_name)       
    publish_to_kafka(messages)
    #for k,v in messages.items():
    #    for m in v:
    #        try:
    #            validate(m, schema)
    #        except:
    #            ("Wrongly formatted schema found:" +m)
    #publish_to_kafka(messages)

    # write_message_to_json(missing_messages, f'function_logs/missing/{integration_name}.json')
    # write_message_to_json(messages, f'function_logs/successful/{integration_name}.json')

    c = msg.set(json.dumps(messages))
    return c
    