from kafka.admin import KafkaAdminClient, NewTopic

admin_client = KafkaAdminClient(
    bootstrap_servers=""
)

topic_names_list = ['github_commits', 
                    'github_average_commits_per_week', 
                    'github_contributors', 
                    'github_forks', 
                    'github_watchers', 
                    'github_stars',
                    'github_merged_pull_requests'
                    'github_open_issues'
                    'github_closed_issues',
                    'github_commit_speed_per_contributor',
                    'github_capitalization_contributors',
                    'circulating_supply',
                    'circulating_supply_percentage',
                    'tokens_staked',
                    'assets_staked_on_chain',
                    'current_price_usd',
                    'market_capitalization_fully_diluted',
                    'trading_volume_24h',
                    'cmc_max_supply',
                    'total_supply',
                    'fully_diluted_valuation',
                    'all_time_high',
                    'repos_url',
                    'subreddit_url',
                    'twitter_screen_name'
                    ]

topic_list = []

for topic_name in topic_names_list:
    topic_list.append(NewTopic(name=topic_name, num_partitions=1, replication_factor=1))

admin_client.create_topics(new_topics=topic_list, validate_only=False)