import os
from dotenv import load_dotenv
import requests
import json
import pandas as pd
load_dotenv()

class CoinMarketCapCollector:
    def __init__(self,) -> None:
        headers = {'X-CMC_PRO_API_KEY': os.environ.get("coin-market-cap-token")}
        res = requests.get(url="https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest", headers=headers)
        with open('data_tmp.json', 'w') as f:
            f.write(json.dumps(res.json()))
        
        with open('tokens.json', 'r') as f:
            coins = json.load(f)
        
        coin_ids = []
        for coin in coins.keys():
            coin_ids.append(coins[coin]['cmc_id'])
        
        res = res.json()['data']
        for coin_data in res:
            if coin_data['id'] in coin_ids:
                print(f"Coin: {coin_data['name']}")
                print(f"Current price: {self.get_current_price(coin_data)}")
                print(f"Market capitalization fully diluted: {self.get_market_capitalization_full_diluted(coin_data)}")
                print(f"Trading volume: {self.get_trading_volume(coin_data)}")
                print(f"Max supply: {self.get_max_supply(coin_data)}")
                print(f"Total supply: {self.get_total_supply(coin_data)}")
                print()

    def get_trading_volume(self, coin_data):
        return coin_data['quote']['USD']['volume_24h'] # Is this correct?

    def get_market_capitalization_full_diluted(self, coin_data):
        return coin_data['quote']['USD']['fully_diluted_market_cap']

    def get_current_price(self, coin_data):
        return coin_data['quote']['USD']['price']

    def get_max_supply(self, coin_data):
        return coin_data['max_supply']

    def get_total_supply(self, coin_data):
        return coin_data['total_supply']

CoinMarketCapCollector()

