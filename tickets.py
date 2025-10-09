import time
import requests
import warnings
import pandas as pd

from utils import create_folder_if_does_not_exist

warnings.filterwarnings("ignore")

from config.env import token, domain
from commons import *

PER_PAGE = 100
HEADERS = {"Content-Type": "application/json"}


create_folder_if_does_not_exist(API_Final_DIR)




def fetch_all_tickets():
    tickets = []
    page = 1
    url = f"{domain}/api/v2/tickets?include=stats&per_page=100&page={page}"

    while url:
        response = requests.get(url, auth=(token, "X"), headers=HEADERS, verify=False)

        # Handle rate limit
        if response.status_code == 429:
            print("Rate limit reached. Sleeping 60s...")
            time.sleep(60)
            continue

        if response.status_code != 200:
            print(f"❌ Error {response.status_code}: {response.text}")
            break

        try:
            data = response.json()
        except ValueError:
            print("❌ JSON decode failed, skipping page...")
            break

        tickets.extend(data)

        # Pagination via Link header
        link_header = response.headers.get("link")
        if link_header and 'rel="next"' in link_header:
            next_link = [l for l in link_header.split(",") if 'rel="next"' in l][0]
            url = next_link.split(";")[0].strip("<> ")
            page += 1
            print(f"➡️ Fetching next page ({page}) ...")
        else:
            url = None

    return tickets


def clean_lists(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.columns:
        df[col] = df[col].apply(
            lambda x: ", ".join(map(str, x))
            if isinstance(x, list)
            else x
        )
    return df


def main():
    print("📦 Fetching Freshdesk tickets (with stats)...")
    tickets = fetch_all_tickets()

    if not tickets:
        print("⚠️ No tickets found.")
        return

    # Flatten all JSON fields (including nested ones like stats.*)
    df = pd.json_normalize(tickets)

    # Convert list columns (e.g. tags, emails) to comma-separated strings
    df = clean_lists(df)

    # Save everything dynamically
    df.to_csv(Ticket_Data, index=False)
    print(f"💾 Saved {len(df)} tickets with {len(df.columns)} columns to {Ticket_Data}")
    print("🧾 Columns saved:")
    print(", ".join(df.columns))


if __name__ == "__main__":
    main()

