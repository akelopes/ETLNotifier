#!/usr/bin/env python3
import os
import sys
from datetime import datetime, timezone

from dotenv import load_dotenv
from pymongo import MongoClient, DESCENDING

load_dotenv()

CONN = os.environ["MONGO_CONNECTION_STRING"]
DB = os.environ.get("MONGO_DATABASE", "chuk")
COLLECTION = os.environ.get("MONGO_COLLECTION", "queue")
LIMIT = int(sys.argv[1]) if len(sys.argv) > 1 else 20

client = MongoClient(CONN)
col = client[DB][COLLECTION]

total = col.count_documents({})
docs = list(col.find({}, {"_id": 0}).sort("createdAt", DESCENDING).limit(LIMIT))

print(f"Total docs: {total}  |  Showing last {len(docs)}\n")
for d in docs:
    created = d.get("createdAt", "")
    if isinstance(created, datetime):
        created = created.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"[{created}] {d.get('type','?')} | status={d.get('status','?')} | source={d.get('source','?')}")
    print(f"  prompt   : {d.get('prompt','')}")
    print(f"  id       : {d.get('id','')}")
    print(f"  handoffId: {d.get('handoffId','')}")
    print()
