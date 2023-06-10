from collections.abc import MutableMapping
import requests
import logging
import pandas as pd
from src.kafka_producer import Df101KafkaProducer
from datetime import datetime, timezone
import json
import numpy as np
import os

def flatten_dict(d: MutableMapping, sep: str= '.') -> MutableMapping:
    if len(d.keys()) == 0:
        return {}
    [flat_dict] = pd.json_normalize(d, sep=sep).to_dict(orient='records')
    return flat_dict

def get(url: str, headers: dict = {}, params: dict = {}):
    res = None
    try:
        res = requests.get(
                    url=url,
                    headers=headers,
                    params=params
                )
        res.raise_for_status()
        res = res.json()
    except Exception as e:
        logging.error(f"Encountered an exception when fetching data for url {url}")
        logging.error(e)
        res = {}
    return res

def publish_to_kafka(messages: dict):
     kfk_prod = Df101KafkaProducer(os.environ.get('kafka_connection_string'))
     for key in messages.keys():
        topic_name = key
        logging.info(topic_name)
        for coin in messages[key]:
            kfk_prod.send(topic=topic_name, message=coin)

def get_empty_coin_data(coin: str, schema: dict):
    for key in schema.keys():
        schema[key] = None
    schema['token'] = coin 
    schema['timestampz'] = (
        datetime.utcnow()
        .replace(tzinfo=timezone.utc)
        .timestamp()
    )
    return schema

def populate_coin_data(coin_data: dict, api_data: dict, key_mapping: dict):
    for key in key_mapping.keys():
        if key not in ['token', 'timestampz']:
            coin_data[key] = api_data[key_mapping[key]] if key_mapping[key] in api_data.keys() else None

    return coin_data

def write_message_to_json(data: list, path: str):
    with open(path, 'w') as f:
        f.write(json.dumps(data))

def format_response(coin_data: list, integration_name: str):
    df = pd.DataFrame(coin_data)  # dataframing because this is easier
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
                "source": f"{integration_name}",
                # "GUID_functions": context.invocation_id,
            }
            for k, v in df_dict[topic].items()
            if v is not None
        ]

        missing_messages[topic] = [
            {
                "token": k,
                "value": v,
                "timestampz": update_time["timestampz"][k],
                "source": f"{integration_name}",
                # "GUID_functions": context.invocation_id,
            }
            for k, v in df_dict[topic].items()
            if v is None
        ]

    
    return messages, missing_messages

