# ----This file will handle setup, benchmarking, fairness, and plotting----

import threading, time, statistics, csv, certifi
import matplotlib.pyplot as plt
from pymongo import MongoClient
from bson.int64 import Int64

from NoSQL_STM import worker_occ
from mongo_tx import worker_mongo

MONGO_URI = "mongodb+srv://<username>:<password>@cluster0.gldzlv1.mongodb.net/?retryWrites=true&w=majority"
DB_NAME = "CUSTOMER"
CUSTOMER_COLL = "accounts"
TIE_COLL = "tie_breaker"
NUM_THREADS = 100

# ---------- Setup ----------
def make_unique_thread_ts(base: int, tid: int) -> int:
    return base + tid

def setup_db(client):
    client.drop_database(DB_NAME)
    db = client[DB_NAME]
    db.create_collection(
        CUSTOMER_COLL,
        validator={
            "$jsonSchema": {
                "bsonType": "object",
                "required": ["account_id", "version", "last_commit_ts"],
                "properties": {
                    "account_id": {"bsonType": "string"},
                    "version": {"bsonType": "long"},
                    "last_commit_ts": {"bsonType": "long"},
                },
            }
        },
    )
    db.create_collection(TIE_COLL)
    return db

def seed_data(coll, num_accounts=1000):
    coll.delete_many({})
    docs = []
    for i in range(num_accounts):
        docs.append({
            "account_id": f"acc-{i+1}",
            "name": f"User{i+1}",
            "address": f"Addr{i+1}",
            "phone": f"555-{i+1:03d}",
            "version": Int64(0),
            "last_commit_ts": Int64(0),
            "last_writer_tid": None,
        })
    coll.insert_many(docs)

def gini_coefficient(values):
    if not values: return 0
    sorted_vals = sorted(values)
    n = len(values)
    cumvals = [sum(sorted_vals[:i+1]) for i in range(n)]
    return (n + 1 - 2 * sum(cumvals) / cumvals[-1]) / n

# ---------- Benchmark ----------
def run_benchmark(worker_func, coll, accounts, n_threads=NUM_THREADS, label="System"):
    threads, latencies, retries, results, raw_records = [], [], [], [], []
    def timed_worker(tid, ts, txn_id):
        start = time.time()
        result = worker_func(coll, tid, ts, accounts)
        end = time.time()
        latency_ms = (end - start) * 1000
        latencies.append(latency_ms)
        retries.append(result["retries"])
        raw_records.append({"txn_id": txn_id,"thread_id": tid,"latency_ms": latency_ms,"retries": result["retries"]})
        results.append({"tid": tid, "latency": latency_ms, "retries": result["retries"], "ok": result["ok"]})
    base = int(time.time_ns())
    for i in range(n_threads):
        tid = f"{label}-thread-{i+1}"
        ts = make_unique_thread_ts(base, i+1)
        t = threading.Thread(target=timed_worker, args=(tid, ts, i+1))
        threads.append(t)
        t.start()
    for t in threads: t.join()

    # Save raw CSV
    with open(f"{label}_raw.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["txn_id","thread_id","latency_ms","retries"])
        writer.writeheader(); writer.writerows(raw_records)

    # Stats
    avg, med, mn, mx = statistics.mean(latencies), statistics.median(latencies), min(latencies), max(latencies)
    total_retries, throughput, fairness = sum(retries), n_threads / (sum(latencies) / 1000), gini_coefficient(latencies)

    # Save summary
    with open(f"{label}_summary.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["System","Transactions","Avg_Latency","Median_Latency","Min_Latency","Max_Latency","Total_Retries","Throughput","Gini_Fairness"])
        writer.writerow([label,n_threads,avg,med,mn,mx,total_retries,throughput,fairness])

    print(f"\n--- {label} Results ---")
    print(f"Transactions: {n_threads}, Avg latency: {avg:.2f} ms, Throughput: {throughput:.2f} tx/sec")

    return results

def main():
    client = MongoClient(MONGO_URI, retryWrites=False, tls=True, tlsCAFile=certifi.where())

    # NoSQL-STM
    db = setup_db(client); coll = db[CUSTOMER_COLL]; seed_data(coll, 1000)
    accounts = [d["account_id"] for d in coll.find({}, {"account_id": 1})]
    occ_results = run_benchmark(worker_occ, coll, accounts, label="NoSQL-STM")

    # Mongo
    db = setup_db(client); coll = db[CUSTOMER_COLL]; seed_data(coll, 1000)
    accounts = [d["account_id"] for d in coll.find({}, {"account_id": 1})]
    mongo_results = run_benchmark(worker_mongo, coll, accounts, label="MongoDB TX")

    # Plots
    occ_lat = [r["latency"] for r in occ_results]; mongo_lat = [r["latency"] for r in mongo_results]
    plt.plot(occ_lat, label="NoSQL-STM"); plt.plot(mongo_lat, label="MongoDB TX")
    plt.xlabel("Transaction #"); plt.ylabel("Latency (ms)")
    plt.title("Latency per Transaction"); plt.legend(); plt.savefig("latency_cdf.png"); plt.close()

    plt.hist(occ_lat, bins=50, alpha=0.5, label="NoSQL-STM"); plt.hist(mongo_lat, bins=50, alpha=0.5, label="MongoDB TX")
    plt.xlabel("Latency (ms)"); plt.ylabel("Density")
    plt.title("Latency Distribution (Histogram)"); plt.legend(); plt.savefig("latency_histogram.png"); plt.close()

    occ_total, mongo_total = sum(r["retries"] for r in occ_results), sum(r["retries"] for r in mongo_results)
    plt.bar(["NoSQL-STM","MongoDB TX"], [occ_total, mongo_total], color=["blue","orange"])
    plt.ylabel("Total Retries"); plt.title("Total Retry Comparison"); plt.savefig("throughput_comparison.png"); plt.close()

if __name__ == "__main__":
    main()
