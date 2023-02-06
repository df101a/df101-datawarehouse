import requests
import json

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
        return self.data[coin_id]['market_data']['current_price']['usd']
    
    def get_fully_diluted_valuation(self, coin_id):
        return self.data[coin_id]['market_data']['fully_diluted_valuation']['usd']
    
    def get_fully_diluted_valuation_calculated(self, coin_id):
        return float(self.data[coin_id]['market_data']['max_supply']) * float(self.data[coin_id]['market_data']['current_price']['usd'])
    
    def get_market_capitalization_tvl(self, coin_id):
        return float(self.data[coin_id]['market_data']['max_supply']) * float(self.data[coin_id]['market_data']['circulation_supply']['usd'])
    
    def get_github_link(self, coin_id):
        return self.data[coin_id]['links']['repos_url']['github']

    def get_github_commits(self, coin_id):
        # TODO 
        pass 
    
    def get_github_average_commits_per_week(self, coin_id):
        # TODO 
        pass

    def get_github_contributors(self, coin_id):
        return self.data[coin_id]['developer_data']['pull_request_contributors']

    def get_github_forks(self, coin_id):
        return self.data[coin_id]['developer_data']['forks']

    def get_github_watchers(self, coin_id):
        return self.data[coin_id]['developer_data']['subscribers']

    def get_github_stars(self, coin_id):
        return self.data[coin_id]['developer_data']['stars']

    def get_github_merged_pull_requests(self, coin_id):
        return self.data[coin_id]['developer_data']['pull_requests_merged']

    def get_github_open_issues(self, coin_id):
        return self.data[coin_id]['developer_data']['total_issues']

    def get_github_closed_issues(self, coin_id):
        return self.data[coin_id]['developer_data']['closed_issues']

    def get_github_commit_speed_per_contributor(self, coin_id):
        # TODO
        pass

    def get_github_capitalizastion_contribtors(self, coin_id):
        # TODO
        pass
    
with open('tokens.json', 'r') as f:
    coins_dict = json.load(f)
    coins = []
    for coin in coins_dict.keys():
        coins.append(coins_dict[coin]['cg_id'])

dc = DataCollector([coins[1]])

all_coin_data = {}
for coin in coins:
    coin_data = {}
    ## Market Data
    current_price = dc.get_current_price(coins[1])
    fully_diluted_valuation = dc.get_fully_diluted_valuation(coins[1])
    fully_diluted_valuation_calculated = dc.get_fully_diluted_valuation_calculated(coins[1])
    total_supply = dc.get_total_supply(coins[1])
    total_volume = dc.get_total_volume(coins[1])
    max_supply = dc.get_max_supply(coins[1])

    ## Github Data
    github_link = dc.get_github_link(coins[1])
    github_commits = dc.get_github_commits(coins[1])
    github_average_commits_per_week = dc.get_github_average_commits_per_week(coins[1])
    github_contributors = dc.get_github_contributors(coins[1])
    github_forks = dc.get_github_forks(coins[1])
    github_watchers = dc.get_github_watchers(coins[1])
    github_stars = dc.get_github_stars(coins[1])
    github_merged_pull_requests = dc.get_github_merged_pull_requests(coins[1])
    github_open_issues = dc.get_github_open_issues(coins[1])
    github_closed_issues = dc.get_github_closed_issues(coins[1])
    github_commit_speed_per_contributor = dc.get_github_commit_speed_per_contributor(coins[1])
    github_capitalization_contributors = dc.get_github_capitalizastion_contribtors(coins[1])

    print(f"Current price: {current_price}")
    print(f"Fully diluted valuation: {fully_diluted_valuation}")
    print(f"Fully diluted valuation calculated {fully_diluted_valuation_calculated}")
    print(f"Total supply: {total_supply}")
    print(f"Total volume: {total_volume}")
    print(f"Max supply: {max_supply}")
    print(f"Github link: {github_link}")
    print(f"Github commits: {github_commits}")
    print(f"Github average commits per week: {github_average_commits_per_week}")
    print(f"Github contributors: {github_contributors}")
    print(f"Github watchers: {github_watchers}")
    print(f"Github stars: {github_stars}")
    print(f"Github forks: {github_forks}")
    print(f"Github merged pull requests: {github_merged_pull_requests}")
    print(f"Github open issues: {github_open_issues}")
    print(f"Github closed issues: {github_closed_issues}")
    print(f"Github commit speed per contributor: {github_commit_speed_per_contributor}")
    print(f"Github capitalization contributors: {github_capitalization_contributors}")

    coin_data['current_price'] = current_price
    coin_data['fully_diluted_valuation'] = fully_diluted_valuation
    coin_data['fully_diluted_valuation_calculated'] = fully_diluted_valuation_calculated
    coin_data['total_supply'] = total_supply
    coin_data['total_volume'] = total_volume
    coin_data['max_supply'] = max_supply
    coin_data['github_link'] = github_link
    coin_data['github_commits'] = github_commits
    coin_data['github_average_commits_per_week'] = github_average_commits_per_week
    coin_data['github_contributors'] = github_contributors
    coin_data['github_watchers'] = github_watchers
    coin_data['github_stars'] = github_stars
    coin_data['github_forks'] = github_forks
    coin_data['github_merged_pull_requests'] = github_merged_pull_requests
    coin_data['github_open_issues'] = github_open_issues
    coin_data['github_closed_issues'] = github_closed_issues
    coin_data['github_commit_speed_per_contributor'] = github_commit_speed_per_contributor
    coin_data['github_capitalization_contributors'] = github_capitalization_contributors
    all_coin_data[coin] = coin_data

with open('sample_data/coin_data.json', 'w') as f:
    f.write(json.dumps(all_coin_data))