# orchestrator/jobs/fix_full_raw_unwrap.py

import os
from pymongo import MongoClient

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")

DB_NAME = "dispatchtrack"
ROUTES_COLLECTION = "routes"


def run():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    routes = db[ROUTES_COLLECTION]

    count = 0

    cursor = routes.find({
        "full_raw": {"$exists": True},
        "full_raw.route": {"$exists": True} #### CHANGE THIS
    })

    for doc in cursor:
        response = doc["full_raw"]["route"] #### AND THIS

        routes.update_one(
            {"_id": doc["_id"]},
            {
                "$set": {"full_raw": response}
            }
        )
        count += 1

    client.close()
    print(f"Unwrapped {count} route documents.")


if __name__ == "__main__":
    run()
