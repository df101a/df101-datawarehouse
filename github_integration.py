from dotenv import load_dotenv
import os
import requests
import json
import pandas as pd
load_dotenv()

class GithubAPI:
    def __init__(self, repo_name) -> None:
        self.access_token = os.environ.get("github-access-token")
        self.headers = {"Authorization": f"Bearer {self.access_token}"}

        self.basic_data = requests.get(f"https://api.github.com/repos/{repo_name}", headers=self.headers).json()
        self.contributors = requests.get(f"https://api.github.com/repos/{repo_name}/contributors", headers=self.headers).json()
        
        commit_path = f"https://api.github.com/repos/{repo_name}/stats/participation"
        self.commits_per_week = requests.get(commit_path, headers=self.headers).json()
        
        open_issue_path = f"https://api.github.com/search/issues?q=repo:{repo_name}+type:issue+state:open&page=0&per_page=1"
        self.open_issues = requests.get(open_issue_path, headers=self.headers).json()
        closed_issue_path = f"https://api.github.com/search/issues?q=repo:{repo_name}+type:issue+state:closed&page=0&per_page=1"
        self.closed_issues = requests.get(closed_issue_path, headers=self.headers).json()
        merged_pull_requests_path = f"https://api.github.com/search/issues?q=repo:{repo_name}+type:pr+is:merged&page=0&per_page=1"
        self.merged_pull_requests = requests.get(merged_pull_requests_path, headers=self.headers).json()
        #with open(f"sample_data/github-data-{path.split('/')[-2]}-{path.split('/')[-1]}.json", 'w') as f:
        #    json.dump(self.basic_data, f)

        
    
    def get_github_commits(self,):
        print(self.commits_per_week)
        return sum(self.commits_per_week['all'])
    
    def get_github_average_commits_per_week(self,):
        return sum(self.commits_per_week['all']) / len(self.commits_per_week['all']) ## Gets the github commits per week

    def get_github_contributors(self,):
        return len(self.contributors)

    def get_github_forks(self,):
        return self.basic_data['forks_count']

    def get_github_watchers(self,):
        return self.basic_data['subscribers_count']

    def get_github_stars(self,):
        return self.basic_data['stargazers_count']

    def get_github_merged_pull_requests(self,):
        return self.merged_pull_requests['total_count']

    def get_github_open_issues(self,):
        return self.open_issues['total_count']

    def get_github_closed_issues(self,):
        return self.closed_issues['total_count']

    def get_github_commit_speed_per_contributor(self,):
        # TODO
        pass

    def get_github_capitalizastion_contribtors(self,):
        # TODO
        pass


with open('tokens.json', 'r') as f:
    coins_dict = json.load(f)
    coins = []
    for coin in coins_dict.keys():
        coins.append(coins_dict[coin]['cg_id'])

dc = GithubAPI("octocat/Spoon-Knife")
all_coin_data = {}

for coin in coins:
    ## Github Data
    github_commits = dc.get_github_commits()
    github_average_commits_per_week = dc.get_github_average_commits_per_week()
    github_contributors = dc.get_github_contributors()
    github_forks = dc.get_github_forks()
    github_watchers = dc.get_github_watchers()
    github_stars = dc.get_github_stars()
    github_merged_pull_requests = dc.get_github_merged_pull_requests()
    github_open_issues = dc.get_github_open_issues()
    github_closed_issues = dc.get_github_closed_issues()
    github_commit_speed_per_contributor = dc.get_github_commit_speed_per_contributor()
    github_capitalization_contributors = dc.get_github_capitalizastion_contribtors()

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