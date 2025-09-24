
import random
import time
from pymongo.errors import OperationFailure

def worker_mongo(coll, tid, ts, accounts, max_retries=20):
    db = coll.database
    account_id = random.choice(accounts)
    retries = 0
    with db.client.start_session() as session:
        while retries < max_retries:
            try:
                with session.start_transaction():
                    doc = coll.find_one({"account_id": account_id}, session=session)
                    coll.update_one(
                        {"_id": doc["_id"]},
                        {"$set": {"address": f"NewAddr-{tid}", "last_writer_tid": tid}},
                        session=session,
                    )
                return {"ok": True, "retries": retries}
            except OperationFailure:
                retries += 1
                time.sleep(random.uniform(0.05, 0.2))
    return {"ok": False, "retries": retries}
