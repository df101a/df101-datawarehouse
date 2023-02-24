import os
import requests
import json


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
    #converting Gwei to Wei
    * 10**9
)

with open('tokens.json', 'r') as f:
    tokens_dict = json.load(f)

report_gas = []
    
for token_id in tokens_dict:
    #print(token_id)
    if 'etherscan_symbol' in tokens_dict[token_id]:
        #print("Y")
        report_gas.append({"token":token_id,"network":"erc20","gas":erc_20_gas})
    else:
        #print("N")
        pass

with open('sample_data/etherscan_erc_20_gas_data.json', 'w') as f:
    f.write(json.dumps(report_gas))