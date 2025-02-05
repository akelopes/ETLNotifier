#!/usr/bin/env python3

import json
import os
import requests
import pyodbc
import yaml
import time


WEBHOOK_URL = (
    "https://prod-68.westus.logic.azure.com:443/workflows/7c0fe23b4b714d889c9b60e13555ab2a/triggers/"
    "manual/paths/invoke?api-version=2016-06-01&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&"
    "sig=LS3VwR8Ng2G2nE-45nbjYvnvf2t2LXrgzcbqFdeybDc"
)

# Using ActiveDirectoryInteractive for MFA login, for Managed Identity use ActiveDirectoryMSI
CONN_STR = (
    "Driver={ODBC Driver 17 for SQL Server};"
    "Server=rebacentral-prod.database.windows.net;"
    "Database=rebacentral;"
    "Authentication=ActiveDirectoryInteractive;"
    "UID=alopes@getreba.com;"  # EDIT FOR AUTHENTICATION
)

CACHE_FILE = "cache.json"
QUERIES_FILE = "queries.yml"

MESSAGE_INTRO = "\r **[ETL Notifier]** [Automated Message] \n\n"


def load_cache():
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, "r") as f:
            return json.load(f)
    return {}


def save_cache(cache):
    with open(CACHE_FILE, "w") as f:
        json.dump(cache, f, indent=2)


def send_to_teams(message: str):
    payload = {
        "type": "message",
        "attachments": [
            {
                "contentType": "application/vnd.microsoft.teams.card.o365connector",
                "content": {
                    "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                    "@context": "http://schema.org/extensions",
                    "version": "1.2",
                    "content": message,
                },
            }
        ],
    }
    requests.post(WEBHOOK_URL, json=payload)


def cache_unique_key(row):
    return "|".join(
        str(row.get(k, "")) for k in ["AccountName", "Environment", "StartTime"]
    )


def main():
    with open(QUERIES_FILE, "r") as f:
        queries_data = yaml.safe_load(f)

    conn = pyodbc.connect(CONN_STR)
    cursor = conn.cursor()

    cache = load_cache()

    for name, qinfo in queries_data.items():
        cursor.execute(qinfo["sql"])
        cols = [desc[0] for desc in cursor.description]
        rows = [dict(zip(cols, row)) for row in cursor.fetchall()]

        current_keys = {cache_unique_key(r) for r in rows}
        existing_cache = cache.get(name, {})

        if name == "failures":
            cache[name] = {k: "confirmed" for k in current_keys} 
            new_items = [
                r
                for r in rows
                if cache_unique_key(r) in (current_keys - existing_cache.keys())
            ]

        else:
            # Staggering mechanism to avoid delays in Reba Central updates
            new_pending_items = {
                k: "pending" for k in (current_keys - existing_cache.keys())
            }
            confirmed_items = [
                r for r in rows if existing_cache.get(cache_unique_key(r)) == "pending"
            ]

            cache[name] = {
                **{
                    k: "confirmed"
                    for k in current_keys
                    # updates pending and maintain confirmed if it is still returning from the query.
                    if existing_cache.get(k) == "pending" or existing_cache.get(k) == "confirmed" 
                },
                **new_pending_items,
            }

            new_items = confirmed_items

        if not new_items:
            continue

        msg = MESSAGE_INTRO
        if len(new_items) == 1:
            r = new_items[0]

            if r.get("errorMessage") is not None:
                msg += qinfo["message_single"].format(r["AccountName"], r["Environment"], r["errorMessage"])
            else:
                msg += qinfo["message_single"].format(r["AccountName"], r["Environment"])

            send_to_teams(msg)
        else:
            msg += qinfo["message_multiple"]
            lines = []
            for r in new_items:
                lines.append(f" \n\n- **{r['AccountName']}**: **{r['Environment']}**")
            msg += "".join(lines)

            send_to_teams(msg)

    save_cache(cache)
    cursor.close()
    conn.close()


if __name__ == "__main__":
    while True:
        main()
        time.sleep(300)  # 5 Minutes
