import json

import requests

from config.env import token, fetch_agent, password
from utils import json_to_dataframe
from commons import Agent_Data


def get_agent_data():
    api_key = token
    url = f'{fetch_agent}'

    print(url)
    try:
        response = requests.get(url, auth=(api_key, password), verify=False)
        response.raise_for_status()
        response_json = response.json()
        print("data fetched")

        ticket = json_to_dataframe(response_json)
        print(ticket.columns.tolist())
        ticket.to_csv(Agent_Data, index=False)

    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
    except requests.exceptions.RequestException as req_err:
        print(f"Request error occurred: {req_err}")
    except json.JSONDecodeError as json_err:
        print(f"JSON decoding error occurred: {json_err}")


get_agent_data()
