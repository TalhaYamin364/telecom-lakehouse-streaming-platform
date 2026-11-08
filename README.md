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

## Design questions
The questions every production pipeline has to answer. Answers are filled in as
each part is built; planned approach in italics.

| Question | Approach |
|---|---|
| Reruns | *Writes are idempotent: Iceberg `MERGE` keyed on `event_id`; streaming uses checkpoints.* |
| Duplicates | *Deduplicate on `event_id` in silver.* |
| Late data | *Watermark in streaming; silver upserts late events into the right partition.* |
| Backfill | *Reprocess one date partition at a time with partition overwrite, without touching other days.* |
| 100x volume | *Partition by event date, compact small files, scale Kafka partitions and Spark executors; documented with measured timings.* |

## Versions
Pinned for a MacBook (Apple Silicon, arm64) running Docker Engine 28.1.1 and Compose v2.36.

| Tool | Version | Source | arm64 |
|---|---|---|---|
| Hadoop (HDFS, YARN) | 3.4.3 | Apache release tarball `hadoop-3.4.3-aarch64.tar.gz`, built into a local image | Yes |
| Spark | 4.0.5 | `apache/spark:4.0.5-scala2.13-java17-python3-ubuntu` | Yes |
| Iceberg | 1.11.0 | `iceberg-spark-runtime-4.0_2.13` from Maven Central | n/a (JVM jar) |
| Kafka | 4.3.2 | `apache/kafka:4.3.2` (KRaft mode) | Yes |

The official `apache/hadoop` images are amd64 only, so the Hadoop image is built locally from
Apache's arm64 tarball instead of running under emulation. The Iceberg and Spark pairing
(runtime artifact `4.0_2.13` with Spark 4.0.x) is confirmed when the bronze layer is built.

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
Planning.
