import datetime
import logging
import requests
import json
import time
import pandas as pd
import numpy as np
import azure.functions as func


def main(mytimer: func.TimerRequest, msg: func.Out[str]) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()
    
    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
        
    with open('tokens.json', 'r') as f:
                tokens = json.load(f)
            
    coin_id = []
    for token in tokens.keys():
        if tokens[token]['cg_id']:
            coin_id.append(tokens[token]['cg_id'])

    token_data = []
    #Sourcing Data
    for token in tokens.keys():
        if tokens[token]["cg_id"]:
            coin_data = {}
            coin_data["token"] = token
            cg_id = tokens[token]["cg_id"]
            res = requests.get(
                url=f"https://api.coingecko.com/api/v3/coins/{cg_id}"
            ).json()
            coin_data["timestampz"] = (
                datetime.datetime.utcnow()
                .replace(tzinfo=datetime.timezone.utc)
                .isoformat()
            )
            # coin_data['cg_id'] = res['id']
            coin_data["current_price_usd"] = res["market_data"]["current_price"]["usd"]
            if res["market_data"]["total_supply"]:
                coin_data["total_supply"] = res["market_data"]["total_supply"]
                coin_data["fully_diluted_valuation"] = res["market_data"][
                    "fully_diluted_valuation"
                ]["usd"]
            if res["market_data"]["max_supply"]:
                coin_data["max_supply"] = res["market_data"]["max_supply"]
                coin_data["fully_diluted_valuation_calculated"] = (
                    int(res["market_data"]["max_supply"])
                    * res["market_data"]["current_price"]["usd"]
                )
            coin_data["total_volume"] = res["market_data"]["total_volume"]["usd"]
            coin_data["github_link"] = res["links"]["repos_url"]["github"]
            coin_data["github_link"] = res["links"]["subreddit_url"]
            coin_data["twitter_screen_name"] = res["links"]["twitter_screen_name"]
            if res["market_data"]["ath"]["usd"]:
                coin_data["all_time_high"] = res["market_data"]["ath"]["usd"]
                coin_data["all_time_high_date"] = res["market_data"]["ath_date"]["usd"]
            token_data.append(coin_data)
            time.sleep(4)
            
    #Formatting all responses    
    df = pd.DataFrame(token_data)  # dataframing because this is easier
    df.set_index("token", inplace=True)
    update_time = df[["timestampz"]].to_dict()
    df.drop(columns=["timestampz"], inplace=True)
    df.replace(np.nan, None, inplace=True)
    df_dict = df.to_dict()

    #preparing upload
    messages = {}

    for topic in df_dict.keys():
        messages[topic] = [
            {
                "token": k,
                "value": v,
                "timestampz": update_time["timestampz"][k],
                "source": "Coin_Gecko",
                # "GUID_functions": context.invocation_id,
            }
            for k, v in df_dict[topic].items()
            if v is not None
        ]

    msg.set(json.dumps(messages))