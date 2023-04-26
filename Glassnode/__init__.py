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

with open('schema.json', 'r') as file:
    schema = json.load(file)

def flatten_dict(d: MutableMapping, sep: str= '.') -> MutableMapping:
    if len(d.keys()) == 0:
        return {}
    [flat_dict] = pd.json_normalize(d, sep=sep).to_dict(orient='records')
    return flat_dict

def get_all_data(token_id):
    drop_from_ath = None
    active_validators_cnt = None
    inflation_rate = None 
    wallet_count = None 
    gas_price_mean = None
    logging.info(os.environ.get('glassnode-api-key'))
    try:
        res = requests.get(
                url='https://api.glassnode.com/v1/metrics/market/price_drawdown_relative',
                params={'a': token_id, 'api_key': os.environ.get('glassnode-api-key')}
            ).json()
        
        if res != {}:
            drop_from_ath = res[-1]['v']
    except Exception as e:
        res = {}
        logging.error(f"Encountered an exception when fetching Glassnode data for token {token_id}: \n" + str(e) + "HTTP Result: \n" + str(res))


    try:   
        res = requests.get(
                url="https://api.glassnode.com/v1/metrics/eth2/active_validators_count",
                params={'a': token_id, 'api_key': os.environ.get('glassnode-api-key')}
            ).json()
        if res != {}:
            active_validators_cnt = res[-1]['v']
    except Exception as e:
        res = {}
        logging.error(f"Encountered an exception when fetching Glassnode data for token {token_id}: \n" + str(e) + "HTTP Result: \n" + str(res))
    
    try:
        res = requests.get(
                url="https://api.glassnode.com/v1/metrics/supply/inflation_rate",
                params={'a': token_id, 'api_key': os.environ.get('glassnode-api-key')}
            ).json()
        if res != {}:
            inflation_rate = res[-1]['v']
    except Exception as e:
        res = {}
        logging.error(f"Encountered an exception when fetching Glassnode data for token {token_id}: \n" + str(e) + "HTTP Result: \n" + str(res))
        

    try:   
        res = requests.get(
                url="https://api.glassnode.com/v1/metrics/addresses/count",
                params={'a': token_id, 'api_key': os.environ.get('glassnode-api-key')}
            ).json()
        if res != {}:
            wallet_count = res[-1]['v']
    except Exception as e:
        res = {}
        logging.error(f"Encountered an exception when fetching Glassnode data for token {token_id}: \n" + str(e) + "HTTP Result: \n" + str(res))
        

    try:   
        res = requests.get(
                url="https://api.glassnode.com/v1/metrics/fees/gas_price_mean",
                params={'a': token_id, 'api_key': os.environ.get('glassnode-api-key')}
            ).json()
        if res != {}:
            gas_price_mean = res[-1]['v']
    except Exception as e:
        res = {}
        logging.error(f"Encountered an exception when fetching Glassnode data for token {token_id}: \n" + str(e) + "HTTP Result: \n" + str(res))
        

    data = {
        str(token_id): {
        'drop_from_ath': drop_from_ath,
        'active_validators_count': active_validators_cnt,
        'inflation_rate': inflation_rate,
        'wallet_count': wallet_count,
        'gas_price_mean': gas_price_mean
        }
    }

    data = flatten_dict(dict(data))

    return data

def get_empty_coin_data(coin):
    pass

def publish_to_kafka(messages: dict):
     kfk_prod = Df101KafkaProducer(os.environ.get('kafka-connection-string'))
     for key in messages.keys():
        topic_name = key
        logging.info(topic_name)
        for coin in messages[key]:
            kfk_prod.send(topic=topic_name, message=coin)
             
             

def main(mytimer: func.TimerRequest, msg: func.Out[str]) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()
    DUMP_DATAPOINTS = True
    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
        
    with open('tokens.json', 'r') as f:
                tokens = json.load(f)

    all_coin_data = []
    for token in tokens.keys():
        time.sleep(10)
        
        coin_data = {}  
        coin_data["token"] = token
          
        data = get_all_data(token)
        coin_data['drop_from_ath'] = data[f"{token}.drop_from_ath"]
        coin_data['active_validators_count'] = data[f"{token}.active_validators_count"]
        coin_data['inflation_rate'] = data[f"{token}.inflation_rate"]
        coin_data['gas_price_mean'] = data[f"{token}.gas_price_mean"]

        if data == {}:
            all_coin_data.append(get_empty_coin_data(token))
            continue
        

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
                "source": "Glassnode",
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
                "source": "Glassnode",
                # "GUID_functions": context.invocation_id,
            }
            for k, v in df_dict[topic].items()
            if v is None
        ]
         
    c = msg.set(json.dumps(messages))
    logging.info(c)


    return c