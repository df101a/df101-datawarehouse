import datetime
import logging
import requests
import json
import pandas as pd
import numpy as np
import os
import azure.functions as func


def main(mytimer: func.TimerRequest, msg: func.Out[str]) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()
    
    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
        
    class GithubAPI:
        def __init__(self, repo_name) -> None:
            self.access_token = os.environ.get("github-access-token") # WILL EXPIRE IN MARCH 2024
            self.headers = {"Authorization": f"Bearer {self.access_token}"}
            
            try:
                logging.info(f"Fetching basic github data for repository {repo_name}")
                self.basic_data = requests.get(f"https://api.github.com/repos/{repo_name}", headers=self.headers).json()
            except Exception as e:
                logging.error(f"Encountered an error when fetching basic data for github repository {repo_name} -- {str(e)}")
                
            try:
                logging.info(f"Fetching contributor github data for repository {repo_name}")
                self.contributors = requests.get(f"https://api.github.com/repos/{repo_name}/contributors", headers=self.headers).json()
            except Exception as e:
                logging.error(f"Encountered an error when fetching contributor for github repository {repo_name} -- {str(e)}")
            
            try:
                logging.info(f"Fetching commit github data for repository {repo_name}")
                commit_path = f"https://api.github.com/repos/{repo_name}/stats/participation"
                self.commits_per_week = requests.get(commit_path, headers=self.headers).json()
            except Exception as e:
                logging.error(f"Encountered an error when fetching commit data for github repository {repo_name} -- {str(e)}")
            
            try:
                logging.info(f"Fetching open issue github data for repository {repo_name}")
                open_issue_path = f"https://api.github.com/search/issues?q=repo:{repo_name}+type:issue+state:open&page=0&per_page=1"
                self.open_issues = requests.get(open_issue_path, headers=self.headers).json()
            except Exception as e:
                logging.error(f"Encountered an error when fetching open issue data from github repository {repo_name} -- {str(e)}")
            
            try:
                logging.info(f"Fetching closed issue github data for repository {repo_name}")
                closed_issue_path = f"https://api.github.com/search/issues?q=repo:{repo_name}+type:issue+state:closed&page=0&per_page=1"
                self.closed_issues = requests.get(closed_issue_path, headers=self.headers).json()
            except Exception as e:
                logging.error(f"Encountered an error when fetching closed issue data from github repository {repo_name} -- {str(e)}")
            
            try:
                logging.info(f"Fetching merged pull requests github data for repository {repo_name}")
                merged_pull_requests_path = f"https://api.github.com/search/issues?q=repo:{repo_name}+type:pr+is:merged&page=0&per_page=1"
                self.merged_pull_requests = requests.get(merged_pull_requests_path, headers=self.headers).json()
            except Exception as e:
                logging.error(f"Encountered an error when fetching merged pull request data from github repository {repo_name} -- {str(e)}")

        
        def get_github_commits(self,):
            if 'all' in self.commits_per_week.keys():
                return self.commits_per_week['all']
            else:
                return 'N/A'
        
        def get_github_average_commits_per_week(self,):
            if 'all' in self.commits_per_week.keys():
                return sum(self.commits_per_week['all']) / len(self.commits_per_week['all']) ## Gets the github commits per week
            else:
                return 'N/A'
        
        def get_github_contributors(self,):
            return len(self.contributors)
        
        def get_github_forks(self,):
            return self.basic_data['forks_count']
        
        def get_github_watchers(self,):
            return self.basic_data['subscribers_count']
        
        def get_github_stars(self,):
            return self.basic_data['stargazers_count']

        def get_github_merged_pull_requests(self,):
            if 'total_count' in self.merged_pull_requests.keys():
                return self.merged_pull_requests['total_count']
            else:
                return 'N/A'

        def get_github_open_issues(self,):
            if 'total_count' in self.open_issues.keys():
                return self.open_issues['total_count']
            else:
                return 'N/A'

        def get_github_closed_issues(self,):
            if 'total_count' in self.open_issues.keys():
                return self.closed_issues['total_count']
            else:
                return 'N/A'
            
        def get_github_commit_speed_per_contributor(self,):
            # TODO
            pass

        def get_github_capitalizastion_contribtors(self,):
            # TODO
            pass

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
            continue
        
        dc=GithubAPI(repo_address)
        ## Github Data
        coin_data["timestampz"] = (
            datetime.datetime.utcnow()
            .replace(tzinfo=datetime.timezone.utc)
            .isoformat()
        )
        coin_data['github_commits'] = dc.get_github_commits()
        coin_data['github_average_commits_per_week'] = dc.get_github_average_commits_per_week()
        coin_data['github_contributors'] = dc.get_github_contributors()
        coin_data['github_forks'] = dc.get_github_forks()
        coin_data['github_watchers'] = dc.get_github_watchers()
        coin_data['github_stars'] = dc.get_github_stars()
        coin_data['github_merged_pull_requests'] = dc.get_github_merged_pull_requests()
        coin_data['github_open_issues'] = dc.get_github_open_issues()
        coin_data['github_closed_issues'] = dc.get_github_closed_issues()
        coin_data['github_commit_speed_per_contributor'] = dc.get_github_commit_speed_per_contributor()
        coin_data['github_capitalization_contributors'] = dc.get_github_capitalizastion_contribtors()
        token_data.append(coin_data)
        
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
        
    msg.set(json.dumps(messages))