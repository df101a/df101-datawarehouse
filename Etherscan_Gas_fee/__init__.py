import datetime
import logging
import os
import requests
import json

import azure.functions as func


def main(mytimer: func.TimerRequest, msg: func.Out[str]) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
    # my code starts here :) -------------------

    erc_20_gas = (
        int(
            requests.get(
                "https://api.etherscan.io/api",
                params={
                    "module": "gastracker",
                    "action": "gasoracle",
                    "apikey": os.environ.get("etherscan-access-token"),
                },
            ).json()["result"]["ProposeGasPrice"]
        )
        # converting Gwei to Wei
        * 10**9
    )
    with open('tokens.json', 'r') as f:
        tokens_dict = json.load(f)
    
    report_gas = []

    for token_id in tokens_dict:
        # print(token_id)
        if 'etherscan_symbol' in tokens_dict[token_id]:
            # print("Y")
            report_gas.append(
                {"token": token_id, "network": "erc20", "gas": erc_20_gas})
        else:
            # print("N")
            pass
    
    msg.set(json.dumps(report_gas))