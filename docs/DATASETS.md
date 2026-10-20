# Datasets and workload adapters

The benchmark replays public transaction datasets through the payment API.
This page says what each adapter does with a dataset, what the result is and
is not, and how to get the data. **No dataset is committed to the repository**
(size and licences); `data/` is git-ignored.

None of these datasets contains Stripe payment events or authoritative
double-entry accounting records. They supply *shapes*: amounts, arrival times
and which merchant (or account) is paid. Everything else (the processor, the
ledger entries, fees, refunds, faults) is produced by this project and its
Stripe simulator. Results therefore say how this system behaves under such
shapes, nothing about any commercial processor.

## Normalized format

Every adapter writes `row,time_s,amount_minor,currency,merchant_key,label`
(see [`internal/dataset`](../internal/dataset/dataset.go)) plus a
`<name>.manifest.json` with the source URL, licence, citation, SHA-256 and
MD5 of the raw file, row counts and per-reason drop counts. A scenario names
the normalized file, a window (`offset`, `rows`) and a seed; the file's
SHA-256 is recorded in every result.

How the runner uses a row:

| field | use |
|---|---|
| `amount_minor`, `currency` | the payment's amount |
| `time_s` | arrival time in open-loop scenarios (scaled to the scenario's rate) |
| `merchant_key` | if non-empty, hashed (with the scenario seed) onto the scenario's merchants, so a key that occurs often is a busy merchant. If empty, merchants are assigned by the scenario's Zipf mix or `hot_merchant_fraction` |
| `label` = 1 | paid with the scenario's `fraud_payment_method` (default: a declining test card) |

Rows below 50 minor units (the processor minimum) or above 99,999,999 are
dropped and counted in the manifest.

## Supported sources

| source | adapter | rows used | has merchant | licence | status here |
|---|---|---|---|---|---|
| ULB credit card fraud (284,807 European card transactions, 2 days, 2013) | `dataset fetch-ulb` | all | no (assigned by the scenario) | published on OpenML as "Public"; the Kaggle mirror lists ODbL | downloaded, checksum-verified, **used for every published result** |
| PaySim (synthetic mobile money, calibrated on one month of real logs from an African country) | `dataset paysim` | `PAYMENT` (default), optionally `TRANSFER`, `CASH_OUT`, `CASH_IN`, `DEBIT` | yes (`nameDest`) | CC BY-SA 4.0 (Kaggle listing) | adapter implemented and unit-tested on a hand-written fixture; **not run on the real file** |
| IBM AML-Data (synthetic multi-agent bank world with laundering labels) | `dataset aml` | `Credit Card` payments in one currency (configurable) | partly: the receiving account | CDLA-Sharing-1.0 for the data; the IBM/AML-Data repository code is Apache-2.0 | adapter implemented and unit-tested on a hand-written fixture; **not run on the real file** |

The last two need a Kaggle login to download, which this project's
automation does not have. Their adapters are covered by unit tests built from
fixtures in the documented column layout, so a column or format surprise in
the real files would show up as an error naming the missing column, not as
silently wrong data, but no benchmark number in this repository comes from
PaySim or IBM AML data.

### ULB

```bash
make dataset        # downloads from OpenML, verifies the published MD5, writes data/ulb-creditcard.csv
go run ./cmd/dataset stats -file data/ulb-creditcard.csv
```

Amounts are EUR, rounded to cents. There is no merchant field, so scenarios
assign merchants with a Zipf(1.1) mix over 200 merchants (about 21 % of
traffic to the top one) or with `hot_merchant_fraction`. The `Class` label
marks 492 fraud rows (458 after the minimum-amount filter). Measured by
`dataset stats` on the normalized file (281,404 rows): amounts are strongly
uneven, with median 22.81, p90 205.77, p99 1,025.76 and maximum 25,691.16
EUR; the mean rate over the two days is 1.63 rows/s with a peak of 36 rows in
one second.

### PaySim

Download (needs a Kaggle account and API token; see Kaggle's documentation):

```bash
pip install kaggle   # then place the API token as the Kaggle docs describe
kaggle datasets download -d ealaxi/paysim1 -p data/raw/paysim --unzip
go run ./cmd/dataset paysim -input data/raw/paysim/PS_20174392719_1491204439457_log.csv \
    -currency USD            # writes data/paysim.csv and data/paysim.manifest.json
```

Transformation:

- Only `type == PAYMENT` is kept by default: a customer paying a merchant
  (`nameDest` starts with `M`). `-types PAYMENT,TRANSFER,CASH_OUT` also keeps
  customer-to-customer and cash-agent flows; those are *not* card payments and
  are offered for contention experiments only.
- `amount` → minor units (×100, rounded). PaySim's currency is unnamed, so the
  currency is a label you choose (`-currency`, default `USD`).
- `step` is an hour; the position inside the hour is unknown, so rows are
  spread deterministically (seeded) within their hour.
- `merchant_key` = `nameDest`; `label` = `isFraud`.
- `-limit N` stops after N kept rows.

Limitations: synthetic data calibrated on a private mobile-money log, not card
payments; amounts and balances follow PaySim's simulator; `isFlaggedFraud` and
the balance columns are ignored.

### IBM AML

Download (Kaggle; the dataset has twelve files and is about 41.6 GB in total,
so fetch one transaction file):

```bash
kaggle datasets download -d ealtman2019/ibm-transactions-for-anti-money-laundering-aml \
    -f HI-Small_Trans.csv -p data/raw/aml --unzip
go run ./cmd/dataset aml -input data/raw/aml/HI-Small_Trans.csv -name aml-hi-small
# writes data/aml-hi-small.csv and data/aml-hi-small.manifest.json
```

Columns used (as in the file and in IBM's own `Multi-GNN/format_kaggle_files.py`):
`Timestamp` (`2006/01/02 15:04`), `To Bank`, the second `Account` column (the
receiver), `Amount Received`, `Receiving Currency`, `Payment Currency`,
`Payment Format`, `Is Laundering`.

Transformation:

- Only `Payment Format == "Credit Card"` rows are kept (`-formats` selects
  others: Wire, ACH, Cheque, Cash, Reinvestment, Bitcoin are account-to-account
  transfers rather than payments to a merchant).
- Only rows paid and received in one currency are kept
  (`-currency-name "US Dollar" -currency USD` by default), so no FX is involved.
  Zero-decimal currencies such as the yen are not supported.
- `amount_minor` = `Amount Received` ×100; `time_s` = seconds after the first
  kept row, spread deterministically within the minute (the file has minute
  resolution).
- `merchant_key` = `<To Bank>:<receiving account>`. These are accounts of
  individuals and companies in a simulated world, not merchants of record.
- `Is Laundering` is **not** used as the fraud label by default (laundering
  is not card fraud, and labelled rows would be declined); `-laundering-label`
  carries it into `label`.
- `-limit N` stops after N kept rows.

Limitations: synthetic multi-agent simulation; transactions are transfers
between bank accounts, so there is no card authorisation, decline, refund or
chargeback behaviour in the data; activity concentrates on whichever
accounts the simulation made busy, which the benchmark treats as merchants.

## Workload scenarios

Performance and correctness runs are kept apart:

| directory | what | faults |
|---|---|---|
| `bench/scenarios/0*.json` | the original suite: baseline, latency, fault injection (processor errors, lost responses, database faults, API crashes, chaos) | yes; these are the correctness runs |
| `bench/scenarios/contention/` | generated matrix over workload (suite Zipf mix, 50 % and 90 % of operations to one merchant), concurrency 8/32/128, 1,000 and 10,000 operations, and open-loop arrival rates | none |
| `bench/scenarios/workloads/` | steady, bursty (`load.burst`) and duplicate-request (`load.duplicate_fraction`) shapes on the Zipf and 90 %-hot-merchant mixes, 20,000 operations | none |
| `bench/scenarios/external/` | PaySim and IBM AML replays (steady and with duplicate requests), 20,000 operations. Need the normalized files above and fail with a clear message without them | none |

`scripts/gen-scenarios.py` regenerates the three generated directories.

What each shape exercises:

| shape | how it is built | what it stresses |
|---|---|---|
| high-volume merchant | `hot_merchant_fraction` or the Zipf mix, or a dataset's own merchant keys | row-lock contention on one merchant's payable account |
| uneven amounts | the dataset's amounts (ULB: heavy-tailed, see above) | fee arithmetic and balances; amounts do not change locking |
| bursty traffic | open loop at a mean rate, arrivals squeezed into the first `duty` fraction of each period | queueing, connection pool, rate estimator reaction |
| high account contention | hot merchant, few merchants, high concurrency | the cost sharding exists to remove |
| duplicate requests and retries | `duplicate_fraction` sends a second request with the same `Idempotency-Key` concurrently; the client also retries 409/429/5xx with the same key | the in-flight path of the idempotency mechanism. The oracle checks that both answers name the same payment |
| concurrent processing | `load.concurrency` workers, or open-loop in-flight cap | everything above together |

The second request of a duplicated operation is not counted in throughput or
latency; `load.duplicate_requests` in the result counts them.
