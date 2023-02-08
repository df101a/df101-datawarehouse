import requests
import json
import time

class DataCollector:
    def __init__(self, coins) -> None:
        self.data = {}
        for coin in coins:
            self.data[coin] = dict(requests.get(url=f"https://api.coingecko.com/api/v3/coins/{coin}").json())

    def get_total_supply(self, coin_id):
        return self.data[coin_id]['market_data']['total_supply']
    
    def get_max_supply(self, coin_id):
        return self.data[coin_id]['market_data']['max_supply']
    
    def get_total_volume(self, coin_id):
        return self.data[coin_id]['market_data']['total_volume']['usd']
    
    def get_current_price(self, coin_id):
        print(self.data[coin_id])
        return self.data[coin_id]['market_data']['current_price']['usd']
    
    def get_fully_diluted_valuation(self, coin_id):
        return self.data[coin_id]['market_data']['fully_diluted_valuation']['usd']
    
    def get_fully_diluted_valuation_calculated(self, coin_id):
        return float(self.data[coin_id]['market_data']['max_supply']) * float(self.data[coin_id]['market_data']['current_price']['usd'])
    
    def get_market_capitalization_tvl(self, coin_id):
        return float(self.data[coin_id]['market_data']['max_supply']) * float(self.data[coin_id]['market_data']['circulation_supply']['usd'])
    
    def get_github_link(self, coin_id):
        return self.data[coin_id]['links']['repos_url']['github']

    
with open('tokens.json', 'r') as f:
    coins_dict = json.load(f)
    coins = []
    for coin in coins_dict.keys():
        coins.append(coins_dict[coin]['cg_id'])

dc = DataCollector(coins)

all_coin_data = {}
for coin in coins:
    coin_data = {}
    print(coin)
    ## Market Data
    current_price = dc.get_current_price(coin)
    fully_diluted_valuation = dc.get_fully_diluted_valuation(coin)
    fully_diluted_valuation_calculated = dc.get_fully_diluted_valuation_calculated(coin)
    total_supply = dc.get_total_supply(coin)
    total_volume = dc.get_total_volume(coin)
    max_supply = dc.get_max_supply(coin)
    github_link = dc.get_github_link(coin)
    

    print(f"Current price: {current_price}")
    print(f"Fully diluted valuation: {fully_diluted_valuation}")
    print(f"Fully diluted valuation calculated {fully_diluted_valuation_calculated}")
    print(f"Total supply: {total_supply}")
    print(f"Total volume: {total_volume}")
    print(f"Max supply: {max_supply}")
    print(f"Github link: {github_link}")
    

    coin_data['current_price'] = current_price
    coin_data['fully_diluted_valuation'] = fully_diluted_valuation
    coin_data['fully_diluted_valuation_calculated'] = fully_diluted_valuation_calculated
    coin_data['total_supply'] = total_supply
    coin_data['total_volume'] = total_volume
    coin_data['max_supply'] = max_supply
    coin_data['github_link'] = github_link
    all_coin_data[coin] = coin_data

    time.sleep(1)

with open('sample_data/coingecko_data.json', 'w') as f:
    f.write(json.dumps(all_coin_data))