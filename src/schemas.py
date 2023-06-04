from datetime import datetime, timezone

coingecko_schema = {
    'current_price_usd': str,
    'total_supply': str,
    'fully_diluted_valuation': str,
    'max_supply': str,
    'fully_diluted_valuation_calculated': str,
    'total_volume': str,
    'all_time_high': str,
    'repos_url': str,
    'subreddit_url': str,
    'twitter_screen_name': str,
    'telegram_channel_identifier':str,
    'github_contributors': str
}

coinmarketcap_schema = {
    'current_price_usd': str,
    'market_capitalization_fully_diluted': str,
    'trading_volume_24h': str,
    'max_supply': str,
    'total_supply': str
}

defilama_schema = {
    'TVL': str
}

etherscan_schema = {
    'gas_fee': str
}

github_schema = {
    'github_commits': str,
    'github_average_commits_per_week': str,
    'github_contributors': str,
    'github_forks': str,
    'github_watchers': str,
    'github_stars': str,
    'github_merged_pull_requests': str,
    'github_open_issues': str,
    'github_closed_issues': str,
    'github_commit_speed_per_contributor': str,
    'github_capitalization_contributors': str
}

messari_schema = {
    'circulating_supply': str,
    'circulating_supply_percentage': str,
    'tokens_staked': str,
    'assets_staked_on_chain': str,
    'price_usd': str,
    'total_supply': str
}

glassnode_schema = {
    'drop_from_ath': str,
    'active_validators_count': str,
    'inflation_rate': str,
    'gas_price_mean': str
}