# Flink-Kafka Streaming Pipeline

A Docker-based playground for Apache Flink with Kafka and MySQL integration. A
Flink SQL job consumes JSON messages carrying an `id` and a `value` from a Kafka
topic, looks up each `id` against a MySQL reference table in real time, and
writes the upper-cased value plus the matched tier label to another Kafka
topic. Kafka runs in **KRaft mode** (no Zookeeper).

## At a Glance

**Input message** (you produce this to `input-topic`):
```json
{"id": 1, "value": "hello world"}
```

**Output message** (Flink writes this to `output-topic`):
```json
{"value": "HELLO WORLD", "tier": "GOLD"}
```

**What the Flink job does for every single message:**
1. **Reads** the message off `input-topic`.
2. **Looks up `id`** against MySQL's `membership_tiers` table — a live,
   per-message point-query (not a one-time bulk read), cached for 10 seconds
   per `id` to avoid hammering MySQL. The seeded table maps `1 → GOLD`,
   `2 → SILVER`, `3 → BRONZE`.
3. **Transforms** `value` with `UPPER()`.
4. **Combines** both results and writes `{"value": ..., "tier": ...}` to
   `output-topic`. If `id` doesn't match any MySQL row, `tier` comes through
   as `null` instead of the message being dropped.

In short: the Flink job is a **live enrichment pipeline** — it reaches into
MySQL on every message to attach extra context, rather than just transforming
the Kafka message in isolation.

## Project Structure

```
.
├── docker-compose.yml         # Orchestrates all services
├── build-all-images.sh        # Builds every custom image used by the compose file
├── kafka-image/
│   ├── Dockerfile             # Kafka (KRaft) with auto-created topics
│   └── entrypoint.sh          # Boots Kafka, then creates input-topic and output-topic
├── flink-jobmanager/
│   └── Dockerfile             # Flink JobManager + Kafka/JDBC connector jars
├── flink-taskmanager/
│   └── Dockerfile             # Flink TaskManager + Kafka/JDBC connector jars
├── flink-sql-job/
│   ├── Dockerfile             # Job-submitter image (SQL Client + connectors)
│   └── script.sql             # The Flink SQL streaming job
├── mysql-init/
│   └── init.sql               # Seeds the `membership_tiers` lookup table on first boot
└── flink-kafka/
    └── Dockerfile             # Base Flink image with Kafka connectors
```

## Services

- **kafka**: Message broker in KRaft mode. Auto-creates `input-topic` and `output-topic` on startup.
- **kafka-ui**: Web interface for Kafka — http://localhost:8080
- **mysql**: Seeded with a `membership_tiers` lookup table on every startup (via `mysql-init/init.sql`, no persistent volume — data resets fresh each `docker compose up`). Queried live, per Kafka message, as a lookup/enrichment source (with a short-TTL cache).
- **flink-jobmanager**: Manages job execution and serves the dashboard — http://localhost:8081
- **flink-taskmanager**: Executes job tasks.
- **flink-sql-job**: Short-lived container that submits `script.sql` to the cluster via the Flink SQL Client and then exits. The job keeps running on the JobManager/TaskManager afterward.

## Quick Start

The compose file references **locally built images** (e.g. `kafka-local:7.5.0`,
`flink-jobmanager:1.18.1`), so you must build them first.

1. Build all images:
   ```bash
   ./build-all-images.sh
   ```

2. Start all services:
   ```bash
   docker compose up -d
   ```

3. Verify services are running:
   ```bash
   docker compose ps
   ```

   Note: `flink-sql-job` will show as `Exited (0)` — that is expected. It only
   submits the job and exits; use `docker ps -a` to see it.

## What Goes in Each Topic

Both topics are created automatically by the Kafka container — you do **not** need
to create them manually.

| Topic          | Direction        | What to put in it                                                                 |
| -------------- | ---------------- | --------------------------------------------------------------------------------- |
| `input-topic`  | **You produce**  | JSON objects with `id` and `value` fields, e.g. `{"id": 1, "value": "hello world"}`. |
| `output-topic` | **Flink writes** | Do **not** produce here manually. Flink writes the upper-cased `value` plus the matched `tier` label. |

The job's Kafka tables use the `json` format, so every message must be a JSON
object with `id` and `value` fields. Flink upper-cases `value` and looks up
`id` against MySQL's `membership_tiers` table, writing both fields out as
`{"value": ..., "tier": ...}`. `tier` is `null` if `id` doesn't match any row
in MySQL — see "How the Job Works" below.

## Monitoring the Job in Real Time

### 1. Produce Test Messages

- Open Kafka UI: http://localhost:8080
- Go to **Topics** → **input-topic** → **Produce Message**
- Enter a JSON object in the message **value** field, for example:
  ```
  {"id": 1, "value": "hello world"}
  ```
  ```
  {"id": 2, "value": "flink is running"}
  ```
  ```
  {"id": 999, "value": "mystery"}
  ```
  (`id` 999 deliberately doesn't match any row in MySQL — see below.)
- Click **Produce**.

### 2. View Processed Output

- Go to **Topics** → **output-topic** and view the messages.
- Each input message appears upper-cased, enriched with its looked-up tier:

  | input-topic                              | output-topic                               |
  | ----------------------------------------- | ------------------------------------------- |
  | `{"id":1,"value":"hello world"}`          | `{"value":"HELLO WORLD","tier":"GOLD"}`     |
  | `{"id":2,"value":"flink is running"}`     | `{"value":"FLINK IS RUNNING","tier":"SILVER"}` |
  | `{"id":999,"value":"mystery"}`            | `{"value":"MYSTERY","tier":null}`           |

### 2b. Watch Results Live from Flink (Print Sink)

The job also writes every enriched row to a `print` sink, which prints to the
TaskManager's stdout. Follow it with:

```bash
docker compose logs -f flink-taskmanager
```

Each row shows up as `enriched:<subtask>> +I[HELLO WORLD, GOLD]`. If nothing
appears there, check the dashboard: **Task Managers → (your TM) → Stdout**.

### 3. Verify the Lookup Join

Check the seeded lookup table directly:

```bash
docker compose exec mysql mysql -uflinkuser -pflinkpass flinklab -e "SELECT * FROM membership_tiers;"
```

You should see ids `1`/`2`/`3` mapped to `GOLD`/`SILVER`/`BRONZE`. Producing a
Kafka message with one of those ids should immediately show the matching tier
on `output-topic`; producing one with any other id (like `999` above) should
still produce an output row, just with `tier: null` instead of being dropped.

### 4. Check the Flink Dashboard

- Open the Flink Web UI: http://localhost:8081
- Under **Jobs → Running Jobs** you should see a running job (the SQL Client
  auto-generates the job name from the `INSERT INTO` statement, so it won't be
  named `Kafka Stream Job` anymore).
- Open the job to view its graph, metrics, and throughput.

> Note: "Bytes/Records Received" and "Bytes/Records Sent" only count data
> exchanged **between Flink tasks** — not data read from or written to Kafka
> or MySQL. The source, lookup join, projection, and sink are all chained into
> a single task by the SQL optimizer (confirmed on the job graph: `Source:
> input_table -> LookupJoin -> Calc -> output_table: Writer -> Committer`), so
> these counters stay at 0 even while records flow. This is an inherent
> limitation of this simple single-operator pipeline.

## How the Job Works

`script.sql` defines three tables:

1. `input_table` — a Kafka source (`input-topic`, `json` format), exposing
   `id`, `` `value` ``, and a computed `proc_time AS PROCTIME()` column.
   `'json.ignore-parse-errors' = 'true'` is set here so a malformed (non-JSON)
   message is silently skipped instead of crashing the job.
2. `mysql_table` — a JDBC **lookup** source over MySQL's `membership_tiers`
   table, exposing `id` and `tier`. It also sets `lookup.cache`,
   `lookup.partial-cache.max-rows`, and `lookup.partial-cache.expire-after-write`
   to cache lookup results for 10 seconds (see "Lookup Cache Behavior" below).
3. `output_table` — a Kafka sink (`output-topic`, `json` format), exposing
   `value` and `tier`.

`value` is a reserved SQL keyword, so the column name must be backtick-quoted
everywhere it's used. A single statement joins the two sources:

```sql
INSERT INTO output_table
SELECT UPPER(i.`value`) AS `value`, m.`tier` AS `tier`
FROM input_table AS i
LEFT JOIN mysql_table FOR SYSTEM_TIME AS OF i.proc_time AS m
ON i.`id` = m.`id`;
```

`FOR SYSTEM_TIME AS OF i.proc_time` is Flink's **lookup join** syntax — not
time travel, just "for each Kafka record, go look up the current matching row
in MySQL at the moment this record is processed." This is why `input_table`
needs the `proc_time` column: a lookup join requires a processing-time
attribute on its driving (left) side. `LEFT JOIN` guarantees every Kafka
message produces exactly one output row — if `id` doesn't match any row in
`membership_tiers`, `tier` comes through as `null` instead of the message
being dropped.

Unlike a plain source read, MySQL is **not** read once at startup here — each
distinct `id` triggers a live (or cached) JDBC query against MySQL for the
life of the job. Because this is pure Flink SQL, **no Python or PyFlink is
needed anywhere in the stack** — the Kafka and JDBC connector jars already
present in the Flink images are sufficient.

### Lookup Cache Behavior (Live but Cached)

`mysql_table`'s `lookup.partial-cache.expire-after-write = '10s'` means a
lookup result (match or no-match) for a given `id` is cached for up to 10
seconds. If you change a row in MySQL, a Kafka message with that `id` arriving
within the cache window will still see the **old** value; after the cache
entry expires, the next message with that `id` triggers a fresh MySQL query
and picks up the change. There's no need to resubmit the job to see new MySQL
data — unlike the old design, it's just a matter of the cache window elapsing
(or removing the cache entirely for always-live reads, at the cost of hitting
MySQL on every single message).

## Rebuilding After a Code Change

If you change `script.sql`, rebuild and restart the submitter:

```bash
docker compose up -d --build --force-recreate flink-sql-job
```

Note that re-running the submitter starts a **second**, independent
`INSERT INTO` job reading from `earliest-offset` — Flink does not automatically
replace the previous job. Cancel the old one from the dashboard if you don't
want both running.

## Stopping Services

```bash
docker compose down
```

To remove volumes as well:

```bash
docker compose down -v
```

## Troubleshooting

**Job not showing on the dashboard?**
- The Flink UI is a single-page app and does not always auto-refresh — hard-refresh (Ctrl+Shift+R) and check **Jobs → Running Jobs**.
- Confirm the backend actually has the job: `curl -s http://localhost:8081/jobs`.
- Check the submitter logs: `docker compose logs flink-sql-job`.

**`flink-sql-job` exits non-zero?**
- Check `docker compose logs flink-sql-job` for connector/format errors from the SQL Client.
- Confirm the job actually submitted to the remote cluster rather than failing to connect — the `FLINK_PROPERTIES` on this service point the SQL Client at `flink-jobmanager`.

**No messages appearing in output-topic?**
- Verify you are producing valid JSON to `input-topic` (e.g. `{"id": 1, "value": "hello world"}`).
- Confirm the job is `RUNNING` on the dashboard.
- Check that the TaskManager is registered: `curl -s http://localhost:8081/taskmanagers`.

**Sent a malformed (non-JSON) message by accident?**
- `input_table` is created with `'json.ignore-parse-errors' = 'true'`, so a message
  that isn't valid JSON is silently dropped instead of crashing the job — later valid
  messages still get processed normally. No action needed.

**`mysql` unhealthy / `flink-sql-job` never starts?**
- Check `docker compose logs mysql` — first boot can take a few seconds to
  initialize and run `mysql-init/init.sql`.
- Confirm connectivity manually: `docker compose exec mysql mysqladmin ping -h localhost -u root -prootpass`.
- `flink-sql-job`'s `depends_on` gates on `mysql: condition: service_healthy`, so
  it won't even start until MySQL reports healthy.

**Updated a row in MySQL but still see the old tier (or still null)?**
- Expected within the cache window — see "Lookup Cache Behavior" above. Lookup
  results are cached for 10 seconds (`lookup.partial-cache.expire-after-write`).
  Wait past that window and send another message with the same `id` to see the
  refreshed value; no job resubmission needed.
