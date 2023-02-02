from pycoingecko import CoinGeckoAPI
from datetime import datetime
import json
from collections import defaultdict


class CoinGeckoIntegration:
    def __init__(self) -> None:
        self.cg = CoinGeckoAPI()
        self.res = defaultdict(list)
        self.res['ts'] = str(datetime.now())
        self.all_values = ['total_supply', 'max_supply', 'total_volume', 'current_price', 'fully_diluted_valuation', 'fully_diluted_valuation_calc', 'capitalization_tokens']
        self.calculated_values = ['capitalization_tokens', 'fully_diluted_valuation_calc']

    def get_all_coins(self,):
        with open('tokens.json') as tokens:
            tokens = json.load(tokens)
            tokens = [tokens[key]['cg_id'] for key in tokens.keys()]
            for t in tokens:
                tmp = {}
                for value in self.all_values:
                    tmp[value] = None
                self.res[t] = tmp

            return tokens

    def get_market_data(self,):
        coins = self.get_all_coins()
        coins_str = ','.join(coins)
        data = self.cg.get_coins_markets(id=coins_str, vs_currency='usd')
        data = [d for d in data if d['id'] in coins]
        return data

    def get_all_data(self,):
        data = self.get_market_data()
        for val in self.all_values:
            calculated_value = val in self.calculated_values
            self.get_value(val, data, calculated_value)
        return self.res

    def get_value(self, identifier, market_data, calculated_value=False):        
        data = market_data
        for coin in data:
            
            if calculated_value:
                if identifier == 'fully_diluted_valuation_calc':
                    if coin['max_supply'] is None:
                        self.res[coin['id']]['fully_diluted_valuation_calc'] = None
                    else:
                        self.res[coin['id']]['fully_diluted_valuation_calc'] = coin['current_price'] * coin['max_supply']
                elif identifier == 'capitalization_tokens':
                    self.res[coin['id']]['capitalization_tokens'] =  coin['current_price'] * coin['circulating_supply']
            else:
                assert identifier in coin.keys(), 'Data point is missing, is this a calculated column?'
                self.res[coin['id']][str(identifier)] = coin[identifier]
        


    
cgi = CoinGeckoIntegration()
data = cgi.get_all_data()

with open('sample_data/coingecko.json', 'w') as out:
    file = json.dumps(data)
    out.write(file)