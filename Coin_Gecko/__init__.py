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
            try:
                logging.info(f"Fetching Coin Gecko data for token {token}")
                res = requests.get(
                    url=f"https://api.coingecko.com/api/v3/coins/{cg_id}"
                ).json()
            except Exception as e:
                 logging.error(f"Encountered an error when fetching Coin Gecko data for token {token} - {str(e)}")

            coin_data["timestampz"] = (
                datetime.datetime.utcnow()
                .replace(tzinfo=datetime.timezone.utc)
                .isoformat()
            )
            
            if 'market_data' in res.keys():
                if res['market_data']['current_price']['usd']:     
                    coin_data["current_price_usd"] = res["market_data"]["current_price"]["usd"]
                else:
                    logging.error(f"Unable to fetch current price for token {token}")

                if res["market_data"]["total_supply"]:
                    coin_data["total_supply"] = res["market_data"]["total_supply"]
                else:
                    logging.error(f"Unable to fetch total supply for token {token}")
                
                if res['market_data']['fully_diluted_valuation'] and res['market_data']['fully_diluted_valuation']['usd']:
                    coin_data["fully_diluted_valuation"] = res["market_data"][
                        "fully_diluted_valuation"
                    ]["usd"]
                else:
                    logging.error(f"Unable to fetch fully diluted valuation for token {token}")

                if res["market_data"]["max_supply"]:
                    coin_data["max_supply"] = res["market_data"]["max_supply"]
                    coin_data["fully_diluted_valuation_calculated"] = (
                        int(res["market_data"]["max_supply"])
                        * res["market_data"]["current_price"]["usd"]
                    )

                if res['market_data']['total_volume'] and res['market_data']['total_volume']['usd']:
                    coin_data["total_volume"] = res["market_data"]["total_volume"]["usd"]
                else:
                    logging.error(f"Unable to fetch total volume for token {token}")

                if res["market_data"]["ath"] and res["market_data"]["ath"]["usd"]:
                    coin_data["all_time_high"] = res["market_data"]["ath"]["usd"]
                else:
                    logging.error(f"Unable to fetch all time high (ath) for token {token}")

                if res["market_data"]["ath_date"] and res["market_data"]["ath_date"]["usd"]:
                    coin_data["all_time_high_date"] = res["market_data"]["ath_date"]["usd"]
                else:
                    logging.error(f"Unable to fetch all time high date (ath_date) for token {token}")
            else:
                logging.error(f"Unable to fetch any market data for token {token}")

            if 'links' in res.keys():
                if res["links"]["repos_url"] and res["links"]["repos_url"]["github"]:
                    coin_data["github_link"] = res["links"]["repos_url"]["github"]
                else:
                    logging.error(f"Unable to fetch github data for token {token}")
                
                if res["links"]["subreddit_url"]:
                    coin_data["subreddit_url"] = res["links"]["subreddit_url"]
                else:
                    logging.error(f"Unable to fetch subreddit url for token {token}")
                
                if res["links"]["twitter_screen_name"]:
                    coin_data["twitter_screen_name"] = res["links"]["twitter_screen_name"]
                else:
                    logging.error(f"Unable to fetch twitter screen name for token {token}")
            else:
                logging.error(f"Unable to fetch any links for token {token}")

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

    c = msg.set(json.dumps(messages))
    logging.info(c)
    return c