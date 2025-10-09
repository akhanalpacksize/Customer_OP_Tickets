import json
import os
from datetime import datetime, timedelta
import time


import requests
import warnings
import urllib.parse
import pandas as pd

from commons import Ticket_ID_Data
from config.env import token, domain

HEADERS = {"Content-Type": "application/json"}
PER_PAGE = 100


from utils import create_folder_if_does_not_exist
from commons import Ticket_ID_Data

warnings.filterwarnings("ignore")


ticket_id = Ticket_ID_Data


def fetch_tickets_for_hour(start_time, end_time):
    tickets = []
    page = 1

    query = f"created_at :> '{start_time}' AND created_at :< '{end_time}'"
    encoded_query = urllib.parse.quote(f'"{query}"')
    url = f"{domain}/api/v2/search/tickets?query={encoded_query}&page={page}"
    print(url)

    while url:
        response = requests.get(url, auth=(token, "X"), headers=HEADERS, verify=False)

        # Rate limit
        if response.status_code == 429:
            print("Rate limit reached, sleeping 60s...")
            time.sleep(60)
            continue

        if response.status_code != 200:
            print(f"Error {response.status_code}: {response.text}")
            break

        try:
            data = response.json()
        except ValueError:
            print("JSON decode failed, skipping this page...")
            break

        tickets.extend(data.get("results", []))

        # Search API pagination is limited, so page increment
        if len(data.get("results", [])) < PER_PAGE:
            break
        page += 1
        url = f"{domain}/api/v2/search/tickets?query={encoded_query}&page={page}"

    return tickets

# ------------------------------
# Main loop: year → month → day → hour
# ------------------------------
def main():
    start_date = datetime(2022, 7, 28)
    today = datetime.today()

    all_rows = []

    current_date = start_date
    while current_date.date() <= today.date():
        for hour in range(6, 18):  # 6 AM to 6 PM
            hour_start = current_date.replace(hour=hour, minute=0, second=0)
            hour_end = hour_start + timedelta(hours=1)

            print(f"Fetching tickets from {hour_start} to {hour_end} ...")
            tickets = fetch_tickets_for_hour(hour_start.strftime("%Y-%m-%d %H:%M:%S"),
                                             hour_end.strftime("%Y-%m-%d %H:%M:%S"))
            print(f"  - Fetched {len(tickets)} tickets")

            for t in tickets:
                all_rows.append({
                    "id": t.get("id"),
                    "year": current_date.year,
                    "month": current_date.month,
                    "day": current_date.day,
                    "hour": hour
                })

        # Move to next day
        current_date += timedelta(days=1)

    # Save all ticket IDs to CSV
    df = pd.DataFrame(all_rows)
    df.to_csv(Ticket_ID_Data, index=False)
    print(f"All ticket IDs saved to {Ticket_ID_Data}")
    print(f"Total tickets fetched: {len(df)}")

if __name__ == "__main__":
    main()