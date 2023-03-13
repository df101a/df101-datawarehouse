import datetime
import logging
import os
import requests
import json

import azure.functions as func


def main(mytimer: func.TimerRequest, msg: func.Out[str]) -> None:
    utc_timestamp = (
        datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()
    )

    if mytimer.past_due:
        logging.info("The timer is past due!")

    logging.info("Python timer trigger function ran at %s", utc_timestamp)
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
    utc_timestamp = (
        datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()
    )

    with open("tokens.json", "r") as f:
        tokens_dict = json.load(f)

    token_data = []

    for token in token_data.keys():
        if "etherscan_symbol" in tokens_dict[token]:
            token_data.append(
                {
                    "token": token,
                    "value": erc_20_gas,
                    "timestampz": utc_timestamp,
                    "source": "EtherScan",
                    # "GUID_functions": context.invocation_id,
                }
            )
        else:
            # print("N")
            pass
    message = {"gas_fee": token_data}

    msg.set(json.dumps(message))
