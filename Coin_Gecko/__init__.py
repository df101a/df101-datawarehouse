import datetime
import logging
import requests
import json
import time
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

    for coin in coin_id: #if statements deal with known KeyErrors
        coin_data = {}
        res = requests.get(url=f"https://api.coingecko.com/api/v3/coins/{coin}").json()
        coin_data['cg_id'] = res['id']
        coin_data['current_price_usd'] = res['market_data']['current_price']['usd']
        if res['market_data']['total_supply']:
            coin_data['total_supply'] = res['market_data']['total_supply']
            coin_data['fully_diluted_valuation'] = res['market_data']['fully_diluted_valuation']['usd']
        if res['market_data']['max_supply']:
            coin_data['max_supply'] = res['market_data']['max_supply']
            coin_data['fully_diluted_valuation_calculated'] = int(res['market_data']['max_supply'])*res['market_data']['current_price']['usd']
        
        coin_data['total_volume'] = res['market_data']['total_volume']['usd']
        
        coin_data['github_link'] = res['links']['repos_url']['github']
        coin_data['github_link'] = res['links']['subreddit_url']
        coin_data['twitter_screen_name'] = res['links']['twitter_screen_name']
        if res['market_data']['ath']:
            coin_data['all_time_high'] = res['market_data']['ath']
            coin_data['all_time_high_date'] = res['market_data']['ath_date']
        token_data.append(coin_data)
        time.sleep(1)

    msg.set(json.dumps(token_data))