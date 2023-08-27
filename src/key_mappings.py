coingecko_map = {
    'current_price_usd': 'market_data.current_price.usd',
    'total_supply': 'market_data.total_supply',
    'fully_diluted_valuation': 'market_data.fully_diluted_valuation.usd',
    'max_supply': 'market_data.max_supply',
    'total_volume': 'market_data.total_volume.usd',
    'all_time_high': 'market_data.ath.usd',
    'repos_url': 'links.repos_url.github',
    'subreddit_url': 'links.subreddit_url',
    'twitter_screen_name': 'links.twitter_screen_name',
    'telegram_channel_identifier': 'links.telegram_channel_identifier',
    'github_contributors': 'developer_data.pull_request_contributors',
    'twitter_followers': 'community_data.twitter_followers',
    'reddit_subscribers': 'community_data.reddit_subscribers',
    'telegram_channel_user_count': 'community_data.telegram_channel_user_count'
}

coinmarketcap_map = {
    'current_price_usd': 'quote.USD.price',
    'market_capitalization_fully_diluted': 'quote.USD.fully_diluted_market_cap',
    'trading_volume_24h':'quote.USD.volume_24h',
    'max_supply': 'max_supply',
    'total_supply': 'total_supply',
}

defilama_map = {
    'TVL': 'tvl'
}

etherscan_map = {
    'gas_fee': 'gas_fee'
}

github_map = {
    'github_forks': 'basic.forks_count',
    'github_watchers': 'basic.subscribers_count',
    'github_stars': 'basic.stargazers_count',
    'github_merged_pull_requests': 'mergedpullrequests.total_count',
    'github_open_issues': 'openissues.total_count',
    'github_closed_issues': 'closedissues.total_count'
}

messari_map = {
    'circulating_supply': 'messari.data.supply.circulating',
    'price_usd': 'messari.data.market_data.price_usd',
    'total_supply': 'cg.market_data.total_supply'
}

glassnode_map = {
    'drop_from_ath': 'drop_from_ath',
    'active_validators_count': 'active_validators_count',
    'inflation_rate': 'inflation_rate',
    'gas_price_mean': 'gas_price_mean'
}