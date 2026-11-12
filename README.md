# telecom-lakehouse-streaming-platform

End-to-end big data platform for a mobile operator's usage data (call, SMS and
mobile-data records), built on Hadoop, Kafka, Spark and an Iceberg lakehouse.
Runs locally in Docker.

## Flow

```
CDR generator -> Kafka -> Spark Structured Streaming -> Iceberg (HDFS)
                                                         bronze -> silver -> gold
                                                              |
                                              Spark batch jobs -> reports / dashboard
```

Airflow and shell scripts run the jobs; PII (phone numbers) is masked before the
silver layer.

## Layout

```
docker/        # compose files for HDFS, YARN, Kafka, Spark, Airflow
generator/     # synthetic CDR event producer
spark_jobs/    # streaming and batch jobs
scripts/       # shell helpers (start, stop, backfill, health checks)
```

## Run the Hadoop cluster
One NameNode, two DataNodes (HDFS replication 2), a ResourceManager and a NodeManager.
The image is built locally from Apache's Hadoop 3.4.3 tarball.

```bash
scripts/up.sh   # build, start, wait until healthy, then run both smoke tests below
```

Or step by step:

```bash
docker compose -f docker/compose.hadoop.yml up -d --build --wait
scripts/hdfs-smoke.sh         # 2 live DataNodes, a running NodeManager, an HDFS write/read, replication 2
scripts/spark-yarn-smoke.sh   # runs SparkPi on YARN in cluster mode and checks YARN reports it SUCCEEDED
docker compose -f docker/compose.hadoop.yml down   # add -v to also delete the HDFS data volumes
```

Spark jobs are submitted from a short-lived `spark-client` container (compose profile `spark`), built from `docker/spark/Dockerfile`: the Spark image plus the Iceberg and Kafka connector jars, each pinned by sha256.
For the SparkPi smoke test the driver runs on the cluster (cluster mode). For the streaming job the driver stays in the `spark-client` container (client mode) and only the executors run on YARN, because the NodeManager image has no Python. The NodeManager image uses Java 17 to match Spark 4.

NameNode UI: http://localhost:9870. ResourceManager UI: http://localhost:8088.
Both ports are published on loopback only, so they are not reachable from other machines.
The NameNode formats HDFS only on first start, so restarts keep your data.

## Run Kafka
A single broker in KRaft mode (no ZooKeeper), started from its own compose file.

```bash
docker compose -f docker/compose.kafka.yml up -d --wait
scripts/kafka-smoke.sh   # broker reachable, topic cdr.raw created if missing (3 partitions), a message round-trips
docker compose -f docker/compose.kafka.yml down   # add -v to also delete the Kafka data volume
```

Two listeners: containers use `kafka:19092`, and clients on your Mac use `localhost:9092`, published on loopback only.
Topic auto-creation is off, so a mistyped topic name fails instead of silently creating a new topic.
`cdr.raw` has 3 partitions, and replication factor 1 because there is one broker. Partitions are the scale-out unit, so more can be added later.
Kafka data lives in a named volume, so restarts keep your topics and messages.

## Generate CDR events
A Python generator (in a container, nothing to install) writes synthetic call, SMS and data records to `cdr.raw`, with the problems real feeds have, so the later layers have something to fix.

```bash
docker compose -f docker/compose.kafka.yml --profile generator run --rm generator --count 10000
# --rate 50 paces it at 50 events/s; --count 0 streams until stopped; --seed and --start choose the events
cd generator && python3 -m unittest   # unit tests, standard library only
```

| Behaviour | Detail |
|---|---|
| Mix | 70% data, 20% voice, 10% SMS; 10,000 subscribers |
| Exact duplicates | 2% of events are sent again, unchanged, up to 100 events later |
| Near-duplicates | 1% are sent again with the same `event_id`, a corrected `charge_paisa` and a later `emitted_at`; the silver layer (planned) will keep the latest |
| Late events | 5% have `event_time` earlier than `emitted_at`: mostly 1-10 minutes, 10% of them 1-3 hours, so some arrive after a 10 minute watermark |
| Key | Messages are keyed by `msisdn`, so one subscriber's events stay in one partition, in order |
| Phone numbers | All numbers are in `+88010xxxxxxxx`; Bangladesh mobile numbers start 013 to 019, so none belongs to a real person |

Runs are reproducible: the same `--seed`, `--start` and `--step-ms` give identical events and `event_id`s. Running it twice therefore re-sends the same events as duplicates, which is the replay case the pipeline must absorb; use a different `--seed` for new events. `--start now` is not reproducible, so give each such run its own `--seed`.
Delivery uses an idempotent producer with `acks=all`. The first failed delivery stops generation and the run exits non-zero; a missing topic ends the run within about 30s, when the flush gives up. Stopping it with Ctrl-C or `docker stop` ends generation, then flushes what is queued (a second stop during that flush does not cut it short, and the service allows 40s for it); a finite run stopped early prints `stopped early: N of M events` and exits 0.
Bad flag values are rejected before anything starts: `--start` needs a timezone and a year from 1970 to 2100, `--step-ms` is 1 to 86,400,000, `--rate` and `--count` are 0 or more, and `--topic` and `--bootstrap` cannot be empty.

### Check the events with a console consumer
```bash
scripts/cdr-verify.sh   # generates 1,000 events, then reads cdr.raw back with kafka-console-consumer
```
It prints the first 5 events as the consumer shows them, then summarises everything in `cdr.raw`: messages against distinct `event_id`s, exact duplicates, near-duplicates, late events (and how many are over 10 minutes late), and an example of each. It exits non-zero if the topic is empty or has no duplicates or late events. `cdr.raw` keeps what earlier runs wrote, so the counts grow with each run.

## Bronze layer: Kafka to Iceberg
A Spark Structured Streaming job reads `cdr.raw` and appends every message, untouched, to the Iceberg table `lake.bronze.cdr_raw` on HDFS (Hadoop catalog, warehouse `hdfs://namenode:8020/warehouse`, partitioned by day of the Kafka timestamp). Columns: Kafka topic, partition, offset and timestamp, the key, the raw JSON value, and `ingested_at`. Bronze does no parsing or dedup; that is silver's job.

Start the Hadoop stack first, then Kafka: Kafka joins the Hadoop network so the YARN executors can reach it.

```bash
docker compose -f docker/compose.hadoop.yml up -d --wait && docker compose -f docker/compose.kafka.yml up -d --wait
scripts/kafka-smoke.sh            # creates the cdr.raw topic (auto-creation is off, so nothing else will)
scripts/bronze-stream.sh --once   # ingest what is in Kafka now on YARN, then stop; without --once it runs until Ctrl-C
scripts/bronze-smoke.sh           # about 2 minutes: bronze rows equal Kafka messages after a load, a second load and a rerun
scripts/lake-reset.sh             # delete every lake table and checkpoint and recreate an empty cdr.raw (they must be reset together)
```

The table, its checkpoint and the Kafka topic belong together: if you delete the Kafka volume or recreate `cdr.raw`, run `scripts/lake-reset.sh` too, or bronze would resume from the old topic's offsets.

**Restart safety.** The job keeps a checkpoint on HDFS (`/checkpoints/bronze_cdr_raw`) of which Kafka offsets Iceberg has already committed, and each micro-batch is one atomic Iceberg commit. A stop or a crash therefore resumes from the last commit. The smoke test checks that bronze holds exactly one row per Kafka message (rows equal Kafka's total end offsets, and distinct `(partition, offset)` pairs equal rows). It was also checked by hard-stopping (SIGKILL) the driver while the generator was still producing: Kafka held 9,883 messages at the crash and bronze was 338 behind; after the restart bronze held exactly 10,598 rows for 10,598 messages.

## Silver layer: clean, deduplicated, masked
A Spark batch job (`spark_jobs/silver_cdr.py`, run on YARN) builds `lake.silver.cdr` from bronze: one typed row per `event_id`, partitioned by day of `event_time`.

```bash
scripts/spark-yarn.sh silver_cdr.py          # process the bronze data added since the last run; --full reads all of bronze again
scripts/silver-smoke.sh                      # about 4 minutes, starts from an empty lake (runs lake-reset.sh): checks everything below
MSISDN_SALT=... scripts/spark-yarn.sh silver_cdr.py   # the salt for the phone-number hashes; the default is a local dev value
```

| Rule | What silver does |
|---|---|
| Deduplicate | One row per `event_id`. An exact re-send changes nothing; a correction (same `event_id`, later `emitted_at`) replaces the original. |
| Validate | A record must be valid JSON with an `event_id`, a known type (`VOICE`, `SMS`, `DATA`), a Bangladesh mobile number (`+8801` and 9 digits), a counterparty for voice and SMS, a non-blank cell, parseable times with `event_time` not after `emitted_at`, and sensible duration, segments, bytes and charge. Failures go to `lake.silver.cdr_rejected` with the reason (`invalid msisdn`, `invalid duration`, ...), keyed by Kafka partition and offset so a rerun does not add them twice. The payload is not copied there, because it holds raw phone numbers; find the original in bronze by partition and offset. |
| Mask phone numbers | A number never reaches silver or the quarantine table. Each becomes a salted SHA-256 hash (to count per subscriber and join) and a masked form (`+88010******34`). The smoke test scans every string column of both tables for any number in the accepted format and finds none. |
| Late events | No special case: a row goes to the partition of its `event_time`, whenever it arrives, and replaces an older copy of itself if it is newer, even if the correction also changes `event_time` (the row moves to the new day). Silver has no watermark, so nothing is dropped for being late. |
| Incremental | The job reads only the bronze snapshots added since its last run (`lake.silver.progress` holds the last snapshot) and `MERGE`s them on `event_id`. The merge has to look at all of silver, because a correction can change `event_time`; narrowing it by time is a possible later optimisation for the performance work, if event time is made immutable. If the job stops before saving its progress, the next run reads the same snapshots and reaches the same result. |

The smoke test uses 1,000 generated events, 5 deliberately bad records (including a blank cell) and one event that is corrected later, then checks: 1,001 silver rows and 1,001 distinct ids; the 5 bad records quarantined with the right reasons; silver equal to the latest copy of every event in bronze (the run includes 7 corrected events); a rerun that finds nothing new; a replay of the same events that grows bronze but not silver; and 500 events dated two days earlier that land in their own day's partitions with no duplicates. The late event "fix-time" is corrected in a later batch with a different `event_time` on a different day, and silver still holds a single row, with the corrected values, on the new day.

## Design questions
The questions every production pipeline has to answer. Answers are filled in as
each part is built; planned approach in italics.

| Question | Approach |
|---|---|
| Reruns | Bronze: a checkpoint plus atomic Iceberg commits, so a restart resumes without loss or duplicates (tested with a hard kill). Silver: `MERGE` on `event_id` that only replaces a row with a later copy, so a rerun or a replay of the same events changes nothing (tested). |
| Duplicates | Bronze keeps every copy as received. Silver keeps one row per `event_id`, the latest `emitted_at` winning, so corrections beat originals (tested). |
| Late data | Silver is a batch `MERGE` with no watermark: a late event is upserted into the partition of its `event_time` however late it is (tested with events two days late). Gold will recompute the days a batch touched. |
| Backfill | *Reprocess one date partition at a time with partition overwrite, without touching other days.* |
| 100x volume | *Partition by event date, compact small files, scale Kafka partitions and Spark executors; documented with measured timings.* |

## Versions
Pinned for a MacBook (Apple Silicon, arm64) running Docker Engine 28.1.1 and Compose v2.36.

| Tool | Version | Source | arm64 |
|---|---|---|---|
| Hadoop (HDFS, YARN) | 3.4.3 | Apache release tarball `hadoop-3.4.3-aarch64.tar.gz`, built into a local image | Yes |
| Java (Hadoop image) | 17.0.21 | `eclipse-temurin:17-jre-jammy`, pinned by digest in `docker/hadoop/Dockerfile` | Yes |
| Spark | 4.0.5 | `apache/spark:4.0.5-scala2.13-java17-python3-ubuntu`, pinned by digest in `docker/spark/Dockerfile` | Yes |
| Iceberg | 1.11.0 | `iceberg-spark-runtime-4.0_2.13`, sha256-pinned in `docker/spark/Dockerfile` | n/a (JVM jar) |
| Spark Kafka connector | 4.0.5 | `spark-sql-kafka-0-10_2.13` and `spark-token-provider-kafka-0-10_2.13`, sha256-pinned | n/a (JVM jar) |
| kafka-clients, commons-pool2 | 3.9.1, 2.12.0 | Maven Central, sha256-pinned; the versions Spark 4.0.5's connector depends on | n/a (JVM jar) |
| Kafka | 4.3.2 | `apache/kafka:4.3.2`, pinned by digest in `docker/compose.kafka.yml` (KRaft mode; Java 21 inside the image) | Yes |
| Python (generator) | 3.12.16 | `python:3.12-slim`, pinned by digest in `generator/Dockerfile` | Yes |
| confluent-kafka | 2.16.1 | PyPI, pinned in `generator/requirements.txt`; prebuilt arm64 wheel | Yes |

The official `apache/hadoop` images are amd64 only, so the Hadoop image is built locally from
Apache's arm64 tarball instead of running under emulation. Iceberg runtime `4.0_2.13` 1.11.0 with Spark 4.0.5
is confirmed: the bronze job runs on YARN and the smoke test passes.

## Development workflow
- **Branches:** trunk-based. `main` is always working; work happens on short-lived
  branches (`feat/`, `fix/`, `docs/`, `chore/`), one issue per branch. Direct pushes
  to `main` are blocked.
- **Commits and PR titles:** Conventional Commits (`feat: add HDFS and YARN compose file`).
  CI checks the PR title, which becomes the squash commit message.
- **Pull requests:** the body says `Closes #<issue>`. Squash-merge only; the branch is
  deleted after merging.
- **Review:** this is a solo project, so each PR gets an automated Copilot review and
  CI checks. The author reads the review and merges.
- **Releases:** when a phase milestone is fully closed, the release is tagged
  (`v0.1.0` after phase 1 through `v1.0.0` after phase 7).

## Roadmap
Progress is tracked in GitHub milestones and issues (one milestone per phase):
Hadoop cluster, Kafka events, bronze layer, silver and gold, orchestration and
shell, reporting and performance, wrap-up.

## Status
In build. Running and tested: HDFS and YARN, Spark on YARN, Kafka (KRaft), the CDR generator, the Kafka to Iceberg bronze layer, and the silver layer (deduplicated, validated, masked). Next: gold tables, then a safe one-day backfill.
