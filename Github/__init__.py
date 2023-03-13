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

def flatten_dict(d: MutableMapping, sep: str= '.') -> MutableMapping:
    if len(d.keys()) == 0:
        return {}
    [flat_dict] = pd.json_normalize(d, sep=sep).to_dict(orient='records')
    return flat_dict

def call_github_endpoint(url, topic=None, token_id=None):
    access_token = os.environ.get("github-access-token") # WILL EXPIRE IN MARCH 2024
    headers = {"Authorization": f"Bearer {access_token}"}
    data = None

    try:
        data = requests.get(url, headers=headers).json()
    except Exception as ex:
        logging.error(f"Encountered an error when fetching github's {topic} for token {token_id}")
        
    return data

def get_all_data(token_id: str, repo_name: str):
    data = {
        'basic': None,
        'contributor': None,
        'commits': None,
        'openissues': None,
        'closedissues': None,
        'mergedpullrequests': None
    }

    data['basic'] = call_github_endpoint(
        url=f"https://api.github.com/repos/{repo_name}",
        topic='basic data',
        token_id=token_id
    )
    data['contributor'] = call_github_endpoint(
        url=f"https://api.github.com/repos/{repo_name}/contributors",
        topic='contributor data',
        token_id=token_id
    )
    data['commits'] = call_github_endpoint(
        url=f"https://api.github.com/repos/{repo_name}/stats/participation",
        topic='commit data',
        token_id=token_id
    )
    data['openissues'] = call_github_endpoint(
        url=f"https://api.github.com/search/issues?q=repo:{repo_name}+type:issue+state:open&page=0&per_page=1",
        topic='open issue data',
        token_id=token_id
    )
    data['closedissues'] = call_github_endpoint(
        url=f"https://api.github.com/search/issues?q=repo:{repo_name}+type:issue+state:closed&page=0&per_page=1",
        topic='closed issues data',
        token_id=token_id
    )
    data['mergedpullrequests'] = call_github_endpoint(
        url=f"https://api.github.com/search/issues?q=repo:{repo_name}+type:pr+is:merged&page=0&per_page=1",
        topic='merged pull request data',
        token_id=token_id
    )

    return flatten_dict(data)

def get_empty_coin_data(coin):
    coin_data = {}
    coin_data['token'] = coin
    coin_data["timestampz"] = (
        datetime.datetime.utcnow()
        .replace(tzinfo=datetime.timezone.utc)
        .isoformat()
    )
    coin_data['github_commits'] = None
    coin_data['github_average_commits_per_week'] = None
    coin_data['github_contributors'] = None
    coin_data['github_forks'] = None
    coin_data['github_watchers'] = None
    coin_data['github_stars'] =  None
    coin_data['github_merged_pull_requests'] = None
    coin_data['github_open_issues'] = None
    coin_data['github_closed_issues'] = None
    coin_data['github_commit_speed_per_contributor'] = None 
    coin_data['github_capitalization_contributors'] = None 
    return coin_data

def main(mytimer: func.TimerRequest, msg: func.Out[str]) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()
    
    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
        
    with open('tokens.json', 'r') as f:
        tokens = json.load(f)
            
    token_data = []
    
    for coin in tokens.keys():
        coin_data = {}
        logging.info(f"Fetching github data for token {coin}")
        coin_data['token'] = coin
        repo_address = tokens[coin]['github_repo'].replace('http://api.github.com/repos/','')

        if repo_address == '':
            logging.error(f"Missing repository for token {coin}")
            token_data.append(get_empty_coin_data(coin))
            continue
        
        data = get_all_data(coin, repo_address)

        if data is None:
            token_data.append(get_empty_coin_data(coin))
            continue

        ## Github Data
        coin_data["timestampz"] = (
            datetime.datetime.utcnow()
            .replace(tzinfo=datetime.timezone.utc)
            .isoformat()
        )
        coin_data['github_commits'] = sum(data['commits.all']) if 'commits.all' in data.keys() and data['commits.all'] else None
        coin_data['github_average_commits_per_week'] = sum(data['commits.all']) / len(data['commits.all']) if 'commits.all' in data.keys() and data['commits.all'] else None
        coin_data['github_contributors'] = len(data['contributors']) if 'contributors' in data.keys() and data['contributors'] else None
        coin_data['github_forks'] = data['basic.forks_count'] if 'basic.forks_count' in data.keys() else None
        coin_data['github_watchers'] = data['basic.subscribers_count'] if 'basic.subscribers_count' in data.keys() else None
        coin_data['github_stars'] = data['basic.stargazers_count'] if 'basic.stargazers_count' in data.keys() else None
        coin_data['github_merged_pull_requests'] = data['mergedpullrequests.total_count'] if 'mergedpullrequests.total_count' in data.keys() else None
        coin_data['github_open_issues'] = data['openissues.total_count'] if 'openissues.total_count' in data.keys() else None
        coin_data['github_closed_issues'] = data['closedissues.total_count'] if 'closedissues.total_count' in data.keys() else None
        coin_data['github_commit_speed_per_contributor'] = None #TODO
        coin_data['github_capitalization_contributors'] = None #TODO
        token_data.append(coin_data)
        logging.info(coin_data)
        time.sleep(2)
        
    #Formatting all responses    
    df = pd.DataFrame(token_data)  # dataframing because this is easier
    df.set_index("token", inplace=True)
    update_time = df[["timestampz"]].to_dict()
    df.drop(columns=["timestampz"], inplace=True)
    df.replace(np.nan, None, inplace=True)
    df.replace('N/A', None, inplace=True)
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
                "source": "GitHub",
                # "GUID_functions": context.invocation_id,
            }
            for k, v in df_dict[topic].items()
            if v is not None
        ]
        
    for topic in df_dict.keys():
        missing_messages[topic] = [
            {
                "token": k,
                "value": v,
                "timestampz": update_time["timestampz"][k],
                "source": "Github",
                # "GUID_functions": context.invocation_id,
            }
            for k, v in df_dict[topic].items()
            if v is None
        ]

    with open('log/missing/github.json', 'w') as f:
         f.write(json.dumps(missing_messages))
        
    msg.set(json.dumps(messages))