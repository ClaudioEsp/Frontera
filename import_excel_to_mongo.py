import os
from pathlib import Path

import pandas as pd
from pymongo import MongoClient
from dotenv import load_dotenv

# Load .env if you use it (optional)
BASE_DIR = Path(__file__).resolve().parent
ENV_PATH = BASE_DIR / ".env"
if ENV_PATH.exists():
    load_dotenv(ENV_PATH)

# --- CONFIG ---
EXCEL_PATH = r"Localidad Paris 2025-11-21.xlsx" 
SHEET_NAME = 0  # 0 = first sheet, or use the sheet name as a string

# Mongo config (change to your values or use env vars)
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB_NAME = "FRONTERA"
MONGO_COLLECTION_NAME = "CTS"
# ---------------

def main():

    try:
        # 1) Read Excel
        df = pd.read_excel(EXCEL_PATH, sheet_name=SHEET_NAME)
    except:
        print('Error loading file')

    # Optional: replace NaN with None so Mongo stores null
    df = df.where(pd.notnull(df), None)

    # 2) Convert rows to list of dicts
    records = df.to_dict(orient="records")

    if not records:
        print("No rows found in the Excel file.")
        return

    # 3) Connect to MongoDB
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB_NAME]
    collection = db[MONGO_COLLECTION_NAME]

    print(MONGO_URI)
    print(MONGO_DB_NAME)
    print(MONGO_COLLECTION_NAME)


    # 4) Insert into Mongo
    result = collection.insert_many(records)
    print(f"Inserted {len(result.inserted_ids)} documents into '{MONGO_COLLECTION_NAME}'")

    # 5) Quick check: print first document
    first_doc = collection.find_one()
    print("Example document in Mongo:")
    print(first_doc)

if __name__ == "__main__":
    main()
