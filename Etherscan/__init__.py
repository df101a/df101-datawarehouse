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
from src.utils import flatten_dict, get, populate_coin_data, get_empty_coin_data

with open('schema.json', 'r') as file:
    schema = json.load(file)

def prepare_data():
    res = get(
            url="https://api.etherscan.io/api",
            params={
                "module": "gastracker",
                "action": "gasoracle",
                "apikey": os.environ.get("etherscan-access-token"),
            }
        )
    
    res = flatten_dict(dict(res))
    
    return res         

def main(mytimer: func.TimerRequest, msg: func.Out[str]) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()
    
    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
        
    with open('tokens.json', 'r') as f:
        coins = json.load(f)

    all_coin_data = []
    integration_name = 'Etherscan'
    data = prepare_data()
    logging.info(data)
    return
    for coin in coins.keys():
        
        cg_id = coins[coin]["cg_id"]
        
        
        coin_data['gas_fee'] = data['result.ProposeGasPrice'] if 'result.ProposeGasPrice' in data.keys() else None
        
        coin_data["timestampz"] = (
            datetime.datetime.utcnow()
            .replace(tzinfo=datetime.timezone.utc)
            .isoformat()
        )

        all_coin_data.append(coin_data)
        logging.info(coin_data)
        
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
                "source": "Etherscan",
                # "GUID_functions": context.invocation_id,
            }
            for k, v in df_dict[topic].items()
            if v is not None
        ]
    
  
    for k,v in messages.items():
        for m in v:
            try:
                validate(m, schema)
            except:
                ("Wrongly formatted schema found:" +m)
    
    publish_to_kafka(messages)
    for topic in df_dict.keys():
        missing_messages[topic] = [
            {
                "token": k,
                "value": v,
                "timestampz": update_time["timestampz"][k],
                "source": "Etherscan",
                # "GUID_functions": context.invocation_id,
            }
            for k, v in df_dict[topic].items()
            if v is None
        ]
         
    c = msg.set(json.dumps(messages))
    logging.info(c)


    return c