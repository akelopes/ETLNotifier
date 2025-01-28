#!/usr/bin/env python3

import json
import os
import requests
import pyodbc
import yaml
from datetime import datetime


WEBHOOK_URL = (
    "https://prod-130.westus.logic.azure.com:443/workflows/7fa408686b1c4d519d6ae834788c9f5c/"
    "triggers/manual/paths/invoke?api-version=2016-06-01&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&"
    "sig=qQUfJYcqWTt31khH-IXvqRvz99FQn9wIR86-AFfdN8o"
)

# Using ActiveDirectoryInteractive for MFA login, for Managed Identity use ActiveDirectoryMSI
CONN_STR = (
    "Driver={ODBC Driver 17 for SQL Server};"
    "Server=rebacentral-prod.database.windows.net;"
    "Database=rebacentral;"
    "Authentication=ActiveDirectoryInteractive;"
    "UID=YourUser@domain.com;"
)

CACHE_FILE = "cache.json"
QUERIES_FILE = "queries.yml"


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
                "contentType": "application/vnd.microsoft.card.adaptive",
                "content": {
                    "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                    "type": "AdaptiveCard",
                    "version": "1.2",
                    "body": [{"type": "TextBlock", "text": message}],
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
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for name, qinfo in queries_data.items():
        cursor.execute(qinfo["sql"])
        cols = [desc[0] for desc in cursor.description]
        rows = [dict(zip(cols, row)) for row in cursor.fetchall()]

        new_items = []
        for r in rows:
            k = cache_unique_key(r)
            if k not in cache.get(name, {}):
                new_items.append(r)
                cache.setdefault(name, {})[k] = True

        if not new_items:
            continue

        if len(new_items) == 1:
            r = new_items[0]
            if name == "new_failure":
                msg = f"{r['AccountName']}-{r['Environment']} failed: {r.get('errorMessage','')} on {now_str}"
            elif name == "over_10h":
                msg = f"{r['AccountName']}-{r['Environment']} over 10h on {now_str}"
            else:
                msg = f"{r['AccountName']}-{r['Environment']} condition on {now_str}"
            send_to_teams(msg)
        else:
            msg = f"{len(new_items)} pipelines matched '{name}' on {now_str}.\n"
            lines = []
            for r in new_items:
                lines.append(f"{r['AccountName']} | {r['Environment']}")
            msg += "\n".join(lines)
            send_to_teams(msg)

    save_cache(cache)
    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()
