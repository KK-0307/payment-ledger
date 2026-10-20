#!/usr/bin/env python3
"""Generates bench/scenarios/contention/*.json and bench/scenarios/workloads/*.json
(python3, standard library only).

The matrix varies three things over the suite's baseline scenario (01):
  workload     zipf   the suite's Zipf(1.1) mix over 200 merchants (~21 % to the top one)
               hot50  50 % of operations go to one merchant, the rest follow the Zipf mix
               hot90  90 % of operations go to one merchant
  concurrency  8, 32, 128 closed-loop workers (offered load grows with it)
  operations   1,000 and 10,000 payment operations
plus open-loop arrivals at fixed rates (10,000 operations, at most 256 in
flight), for the zipf and hot90 workloads.

Everything else is copied from 01-baseline, so a cell differs from the suite
in nothing but what its name says.
"""
import json
import os

OUT = os.path.join(os.path.dirname(__file__), "..", "bench", "scenarios", "contention")
os.makedirs(OUT, exist_ok=True)

WORKLOADS = {
    "zipf": (0.0, "Zipf(1.1) merchant mix over 200 merchants, as in the suite"),
    "hot50": (0.5, "50% of operations to one merchant, the rest Zipf(1.1) over the others"),
    "hot90": (0.9, "90% of operations to one merchant, the rest Zipf(1.1) over the others"),
}


def base(name, desc, rows, hot):
    sc = {
        "name": name,
        "description": desc,
        "seed": 20240601,
        "dataset": "data/ulb-creditcard.csv",
        "offset": 0,
        "rows": rows,
        "merchants": 200,
        "merchant_zipf_s": 1.1,
        "fraud_payment_method": "pm_card_radarBlock",
        "refund_fraction": 0.02,
        "load": {"mode": "closed", "concurrency": 32},
        "client": {"timeout_ms": 5000, "max_attempts": 12, "backoff_base_ms": 50, "backoff_max_ms": 2000},
        "server": {"instances": 1, "idempotency_mode": "on"},
        "processor": {"kind": "sim"},
        "drain_timeout_s": 120,
    }
    if hot:
        sc["hot_merchant_fraction"] = hot
    return sc


def write(sc):
    with open(os.path.join(OUT, sc["name"] + ".json"), "w") as f:
        json.dump(sc, f, indent=2)
        f.write("\n")


count = 0
for wl, (hot, wdesc) in WORKLOADS.items():
    for conc in (8, 32, 128):
        for rows in (1000, 10000):
            sc = base(f"{wl}-c{conc}-n{rows}",
                      f"Contention matrix, closed loop: {wdesc}; {conc} workers; {rows} operations. No injected faults.", rows, hot)
            sc["load"]["concurrency"] = conc
            write(sc)
            count += 1
for wl in ("zipf", "hot90"):
    hot, wdesc = WORKLOADS[wl]
    for rate in (1000, 2500, 5000):
        sc = base(f"{wl}-open{rate}-n10000",
                  f"Contention matrix, open loop at {rate} arrivals/s (dataset arrivals time-scaled), at most 256 in flight: {wdesc}; 10000 operations. No injected faults.",
                  10000, hot)
        sc["load"] = {"mode": "open", "concurrency": 256, "target_rate": rate}
        write(sc)
        count += 1
print(f"wrote {count} scenarios to {os.path.normpath(OUT)}")

# ---------------------------------------------------------------- workloads
# Performance workloads with realistic shapes, no injected faults (the fault
# scenarios 02-09 are the correctness runs; keeping the two apart keeps both
# interpretable). Source: the ULB dataset, which is always available after
# `make dataset`.
WL = os.path.join(os.path.dirname(__file__), "..", "bench", "scenarios", "workloads")
os.makedirs(WL, exist_ok=True)


def wl(sc):
    with open(os.path.join(WL, sc["name"] + ".json"), "w") as f:
        json.dump(sc, f, indent=2)
        f.write("\n")


n = 0
for wlname, (hot, wdesc) in (("zipf", WORKLOADS["zipf"]), ("hot90", WORKLOADS["hot90"])):
    sc = base(f"steady-{wlname}", f"Reference: steady closed loop, 32 workers, {wdesc}; 20000 operations.", 20000, hot)
    wl(sc); n += 1
    sc = base(f"bursty-{wlname}",
              f"Bursty arrivals: open loop, mean 1500 arrivals/s delivered in 1 s bursts (6000/s) every 4 s, at most 256 in flight; {wdesc}; 20000 operations.",
              20000, hot)
    sc["load"] = {"mode": "open", "concurrency": 256, "target_rate": 1500, "burst": {"period_s": 4, "duty": 0.25}}
    wl(sc); n += 1
    sc = base(f"duplicates-{wlname}",
              f"Duplicate requests: closed loop, 32 workers; 20% of operations are sent twice at once with the same Idempotency-Key; {wdesc}; 20000 operations.",
              20000, hot)
    sc["load"]["duplicate_fraction"] = 0.2
    wl(sc); n += 1
print(f"wrote {n} scenarios to {os.path.normpath(WL)}")

# External datasets: the files are not in the repository (licences, size);
# see docs/DATASETS.md. Scenarios fail with a clear message if the
# normalized file is missing or too small.
EXT = os.path.join(os.path.dirname(__file__), "..", "bench", "scenarios", "external")
os.makedirs(EXT, exist_ok=True)
for name, dataset, desc in (
    ("paysim-payments", "data/paysim.csv", "PaySim PAYMENT rows (customer pays merchant), merchants from nameDest"),
    ("aml-credit-card", "data/aml-hi-small.csv", "IBM AML HI-Small, Credit Card rows in US Dollar, merchants from the receiving account"),
):
    for mode in ("steady", "duplicates"):
        sc = base(f"{name}-{mode}", f"{desc}. Closed loop, 32 workers" + ("; 20% of operations sent twice at once." if mode == "duplicates" else "."), 20000, 0)
        sc["dataset"] = dataset
        sc["merchant_zipf_s"] = 0  # unused: the dataset has merchant keys
        if mode == "duplicates":
            sc["load"]["duplicate_fraction"] = 0.2
        with open(os.path.join(EXT, sc["name"] + ".json"), "w") as f:
            json.dump(sc, f, indent=2)
            f.write("\n")
print("wrote external scenarios to", os.path.normpath(EXT))
