
import random
import time
from pymongo import ReturnDocument

TIE_COLL = "tie_breaker"

def now_ns():
    return int(time.time_ns())

class OCCTransaction:
    def __init__(self, coll, tid, ts):
        self.coll = coll
        self.thread_id = tid
        self.thread_ts = ts
        self.read_snapshot = None
        self.target_id = None

  #----------- Read Phase------------

    def read_phase(self, filter_doc):
        doc = self.coll.find_one(filter_doc)
        if not doc:
            raise RuntimeError("Document not found")
        self.read_snapshot = doc
        self.target_id = doc["_id"]

  #----------- Validation Phase------------

    def try_validate_and_commit(self, new_fields):
        if self.read_snapshot is None:
            raise RuntimeError("No read snapshot")

        read_version = self.read_snapshot["version"]
        account_id = self.read_snapshot["account_id"]

        # Step 1: Inserting intents in tie-breaker collection

        self.coll.database[TIE_COLL].insert_one({
            "account_id": account_id,
            "thread_id": self.thread_id,
            "thread_ts": self.thread_ts,
            "ts": now_ns(),
        })

        # Step 2: Checking for contenders (tie-breaker)/determining the winner

        contenders = list(self.coll.database[TIE_COLL].find({"account_id": account_id}))
        if len(contenders) > 1:
            winner = min(contenders, key=lambda x: x["thread_ts"])
            if winner["thread_id"] != self.thread_id:
                self.coll.database[TIE_COLL].delete_one({"thread_id": self.thread_id})
                return False

        # Step 3: CAS update/ Write Phase

        filter_doc = {"_id": self.target_id, "version": read_version}
        update_doc = {
            "$set": {
                **new_fields,
                "version": self.thread_ts,
                "last_commit_ts": self.thread_ts,
                "last_writer_tid": self.thread_id,
            }
        }
        updated = self.coll.find_one_and_update(filter_doc, update_doc, return_document=ReturnDocument.AFTER)

        # Step 4: Cleanup the tie-breaker collection

        self.coll.database[TIE_COLL].delete_one({"thread_id": self.thread_id})
        return updated is not None

def worker_occ(coll, tid, ts, accounts):
    txn = OCCTransaction(coll, tid, ts)
    account_id = random.choice(accounts)
    retries = 0
    while True:
        txn.read_phase({"account_id": account_id})
        ok = txn.try_validate_and_commit({"address": f"NewAddr-{tid}"})
        if ok:
            return {"ok": True, "retries": retries}
        else:
            retries += 1
            time.sleep(random.uniform(0.05, 0.2))
