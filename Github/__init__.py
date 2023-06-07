import datetime
import logging
import requests
import json
import pandas as pd
import numpy as np
import os
import azure.functions as func
from collections.abc import MutableMapping
import pandas as pd
import time
from src.kafka_producer import Df101KafkaProducer
from jsonschema import validate
import json
from src.utils import write_message_to_json, flatten_dict, get, format_response, publish_to_kafka, get_empty_coin_data, populate_coin_data

from src.schemas import github_schema
from src.key_mappings import github_map
#with open('schema.json', 'r') as file:
#    schema = json.load(file)


def prepare_data(repo_name: str):
    access_token = os.environ.get("github_access_token") # WILL EXPIRE IN MARCH 2024
    headers = {"Authorization": f"Bearer {access_token}"}
    
    data_points = [
        'basic',
        'contributors',
        'commits',
        'openissues',
        'closedissues',
        'mergedpullrequests'
    ]

    endpoints = [
        f"https://api.github.com/repos/{repo_name}", 
        f"https://api.github.com/repos/{repo_name}/contributors",
        f"https://api.github.com/repos/{repo_name}/stats/participation",
        f"https://api.github.com/search/issues?q=repo:{repo_name}+type:issue+state:open&page=0&per_page=1",
        f"https://api.github.com/search/issues?q=repo:{repo_name}+type:issue+state:closed&page=0&per_page=1",
        f"https://api.github.com/search/issues?q=repo:{repo_name}+type:pr+is:merged&page=0&per_page=1"
    ]

    idx=0
    data = {}
    for endpoint in endpoints:
        data[data_points[idx]] = get(url=endpoint, headers=headers)
        idx+=1

    return flatten_dict(data)


def main(mytimer: func.TimerRequest, msg: func.Out[str]) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()
    
    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
        
    with open('tokens.json', 'r') as f:
        coins = json.load(f)
            
    integration_name = 'Github'
    all_coin_data = []
    
    for coin in coins.keys():
        logging.info(f"Fetching github data for token {coin}")
        logging.info(f"Fetching token data for {coin} from {integration_name}...({len(all_coin_data)}/{len(coins.keys())})")
        repo_address = coins[coin]['github_repo'].replace('http://api.github.com/repos/','')

        coin_data = get_empty_coin_data(coin=coin, schema=github_schema)
        
        data = prepare_data(repo_address)

        coin_data = populate_coin_data(coin_data=coin_data, api_data=data, key_mapping=github_map)

        # Calculated columns
        coin_data['github_commits'] = sum(data['commits.all']) if 'commits.all' in data.keys() and data['commits.all'] else None
        coin_data['github_average_commits_per_week'] = sum(data['commits.all']) / len(data['commits.all']) if 'commits.all' in data.keys() and data['commits.all'] else None
        coin_data['github_contributors'] = len(data['contributors']) if 'contributors' in data.keys() and data['contributors'] else None

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
    # write_message_to_json(missing_messages, f'function_logs/missing/{integration_name}.json')
    # write_message_to_json(messages, f'function_logs/successful/{integration_name}.json')

    msg.set(json.dumps(messages))