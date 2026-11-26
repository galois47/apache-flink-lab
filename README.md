# Flink-Kafka Streaming Pipeline

A Docker Compose-based playground for Apache Flink with Kafka and MySQL integration. A
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
{"value": "HELLO WORLD", "tier": "<tier for id 1 in MySQL>"}
```

> The `tier` in the output is **not fixed**: it is whatever `membership_tiers`
> in Mock Database A holds for that `id`. The seed data lives in
> `mock-database-a/init.sql`, so editing those rows changes what the job
> emits. See "How Input Maps to Output" below.

**What the Flink job does for every single message:**
1. **Reads** the message off `input-topic`.
2. **Looks up `id`** against MySQL's `membership_tiers` table — a live,
   per-message point-query (not a one-time bulk read), cached for 10 seconds
   per `id` to avoid hammering MySQL. Which `id` maps to which tier is defined
   by the rows in `mock-database-a/init.sql`.
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
├── docker-compose.yml      # All services, the network, volumes and the kafka-init topic list
├── flink-connectors/
│   └── Dockerfile          # One Flink image (+ Kafka/JDBC connector jars) used for JobManager, TaskManager and the job submitter
├── flink-sql-job/
│   └── script.sql          # The Flink SQL streaming job (mounted into the submitter)
├── mock-database-a/
│   └── init.sql            # Seeds Mock Database A (`membership_tiers`, the Flink lookup source)
└── mock-database-b/
    └── init.sql            # Seeds Mock Database B (`customer_regions`; provisioned, not yet used by the job)
```

## Services

| Service | Role | Host ports |
| --- | --- | --- |
| `kafka` | Message broker in KRaft mode (no Zookeeper) | `9092` (host clients: `localhost:9092`), `9101`, `29093` |
| `kafka-init` | One-shot: creates every topic in its `TOPICS` variable, then exits | – |
| `kafka-ui` | Web interface for Kafka | `8080` → http://localhost:8080 |
| `mock-database-a` | MySQL, seeded from `mock-database-a/init.sql` (`membership_tiers`) | `3306` |
| `mock-database-b` | MySQL, seeded from `mock-database-b/init.sql` (`customer_regions`) | `3307` |
| `flink-jobmanager` | Manages job execution and serves the dashboard | `8081` → http://localhost:8081, `6123` |
| `flink-taskmanager` | Executes job tasks | – |
| `flink-sql-job` | One-shot: submits `script.sql` through the Flink SQL Client, then exits. The job keeps running on the JobManager/TaskManager | – |

Details worth knowing:

- Containers talk to each other by service name on `kafka-network` — for example the job uses `kafka:29092` and `mock-database-a:3306`. From the host, use `localhost:9092` for Kafka.
- **Mock Database A** is the live, per-message lookup source for the Flink job (with a short-TTL cache). **Mock Database B** is provisioned alongside it but not referenced by `script.sql` yet.
- Both databases follow one naming convention — for suffix `a` / `b`: container `mock-database-<x>`, and database name, user and password all `mock_database_<x>` (MySQL root password: `rootpass`). They have no persistent volume: the seed SQL runs whenever the container is (re)created, so data resets on `docker compose down` or `--force-recreate`, not on every `up`.
- **Kafka data is also ephemeral** (`/var/lib/kafka/data` is a `tmpfs`). Topics and messages are lost when the container is recreated; `kafka-init` re-creates the topics on the next `up`. To keep them across recreates, replace the `tmpfs` entry in `docker-compose.yml` with a named volume. The broker uses the stock `confluentinc/cp-kafka` image with a fixed `CLUSTER_ID` (required in KRaft mode).
- Container names are pinned to the service names, so `docker exec` / `docker logs` use plain names such as `kafka` or `flink-taskmanager`.

**Startup order** (enforced with healthchecks and `depends_on`, so there is no manual waiting):
1. `kafka` becomes healthy → `kafka-init` creates the topics → `kafka-ui` starts.
2. `flink-jobmanager` becomes healthy → `flink-taskmanager` starts.
3. `mock-database-a` / `mock-database-b` start and become healthy.
4. `flink-sql-job` starts once the JobManager is healthy, the TaskManager has started, `kafka-init` has completed and both databases are healthy. It then waits 20 seconds before submitting the job.

## Prerequisites

- Docker Engine with the Compose v2 plugin (`docker compose version`).
- These host ports free: `3306`, `3307`, `6123`, `8080`, `8081`, `9092`, `9101`, `29093`.
- Internet access on the first build (the Flink image downloads the connector jars).

The only locally built image is `flink-connectors:1.18.1`, built from `flink-connectors/Dockerfile`; everything else is pulled from a registry.

## Quick Start

1. Start all services (Compose builds the Flink image automatically if it is missing):
   ```bash
   docker compose up -d
   ```
   Add `--build` to force a rebuild, or run `docker compose build --no-cache` for a clean build.

2. Verify services are running:
   ```bash
   docker compose ps -a
   ```

   Note: `kafka-init` and `flink-sql-job` show as `Exited (0)` — that is expected. They only create the topics / submit the job and then exit.

3. Wait roughly 20–30 seconds after `up` returns (the submitter sleeps 20 seconds first), then confirm the job is running:
   ```bash
   curl -s http://localhost:8081/jobs
   ```
   You should see one job with `"status":"RUNNING"`.

## What Goes in Each Topic

The `kafka-init` service creates three topics (its `TOPICS` variable in `docker-compose.yml`) — you do **not** need
to create them manually. The job only uses the first two; `middle-topic` is created but currently unused.

| Topic          | Direction        | What to put in it                                                                 |
| -------------- | ---------------- | --------------------------------------------------------------------------------- |
| `input-topic`  | **You produce**  | JSON objects with `id` and `value` fields, e.g. `{"id": 1, "value": "hello world"}`. |
| `output-topic` | **Flink writes** | Do **not** produce here manually. Flink writes the upper-cased `value` plus the matched `tier` label. |

The job's Kafka tables use the `json` format, so every message must be a JSON
object with `id` and `value` fields. Flink upper-cases `value` and looks up
`id` against MySQL's `membership_tiers` table, writing both fields out as
`{"value": ..., "tier": ...}`. `tier` is `null` if `id` doesn't match any row
in MySQL — see "How the Job Works" below.

## How Input Maps to Output

The output is driven by **two inputs**: the message you produce and the rows in
the lookup table, so the exact output values depend on your SQL seed data
rather than on anything in this README.

| Output field | Where it comes from |
| ------------ | ------------------- |
| `value`      | The input message's `value`, upper-cased by the job (`UPPER(value)`). |
| `tier`       | The `tier` column of the `membership_tiers` row (Mock Database A) whose `id` equals the input message's `id`. `null` if there is no such row. |

To see what any given `id` will produce right now, query the live table:

```bash
docker exec mock-database-a mysql -umock_database_a -pmock_database_a mock_database_a -e "SELECT * FROM membership_tiers;"
```

Example, assuming the table currently contains the row `(id=1, tier=X)`:

| You produce to `input-topic`    | Flink writes to `output-topic`            |
| ------------------------------- | ----------------------------------------- |
| `{"id":1,"value":"hello"}`      | `{"value":"HELLO","tier":"X"}`            |
| `{"id":<id not in table>,"value":"hello"}` | `{"value":"HELLO","tier":null}` |

To change the possible tiers (or add ids), edit `mock-database-a/init.sql` and
re-seed — see "Changing SQL Without Rebuilding Images". The job itself needs no
change as long as the table keeps its `id` / `tier` columns. Adding *columns*
or a different table means updating `script.sql` and, for new output fields,
the `output_table` / `print_table` definitions as well.

## Defining Kafka Topics

The topics live in the `TOPICS` environment variable (space separated) of the `kafka-init` service in `docker-compose.yml`:

```yaml
TOPICS: input-topic output-topic middle-topic
```

Add a name there, then run `docker compose run --rm kafka-init` against a running cluster (or just `docker compose up -d` for a fresh start). Creation is idempotent, so existing topics are untouched. Partitions and replication factor are `TOPIC_PARTITIONS` / `TOPIC_REPLICATION_FACTOR` right below it (both `1` for this single broker). Removing a name does **not** delete an existing topic.

## Monitoring the Job in Real Time

### 1. Produce Test Messages

- Open Kafka UI: http://localhost:8080
- Go to **Topics** → **input-topic** → **Produce Message**
- Enter a JSON object in the message **value** field, using an `id` that exists
  in `membership_tiers` (check with the query under "How Input Maps to Output"),
  for example:
  ```
  {"id": 1, "value": "hello world"}
  ```
  ```
  {"id": 2, "value": "flink is running"}
  ```
  and one with an `id` that is **not** in the table to see the `null` case:
  ```
  {"id": 999, "value": "mystery"}
  ```
- Click **Produce**.

### 2. View Processed Output

- Go to **Topics** → **output-topic** and view the messages.
- Each input message appears upper-cased, enriched with its looked-up tier.
  The tier values come from your `membership_tiers` rows (`<tier for id N>`
  below is whatever the table holds for that `id`):

  | input-topic                              | output-topic                               |
  | ----------------------------------------- | ------------------------------------------- |
  | `{"id":1,"value":"hello world"}`          | `{"value":"HELLO WORLD","tier":"<tier for id 1>"}` |
  | `{"id":2,"value":"flink is running"}`     | `{"value":"FLINK IS RUNNING","tier":"<tier for id 2>"}` |
  | `{"id":999,"value":"mystery"}`            | `{"value":"MYSTERY","tier":null}` (no row for 999) |

### 2b. Watch Results Live from Flink (Print Sink)

The job also writes every enriched row to a `print` sink, which prints to the
TaskManager's stdout. Follow it with:

```bash
docker logs -f flink-taskmanager
```

Each row shows up as `enriched> +I[HELLO WORLD, <tier>]` (with higher parallelism the prefix becomes `enriched:<subtask>>`). If nothing
appears there, check the dashboard: **Task Managers → (your TM) → Stdout**.

### 3. Verify the Lookup Join

Check the seeded lookup table directly:

```bash
docker exec mock-database-a mysql -umock_database_a -pmock_database_a mock_database_a -e "SELECT * FROM membership_tiers;"
```

You should see exactly the `id` / `tier` rows seeded from
`mock-database-a/init.sql`. Producing a Kafka message with one of those ids
should show the matching tier on `output-topic`; producing one with any other id (like `999` above) should
still produce an output row, just with `tier: null` instead of being dropped.

### 4. Check the Flink Dashboard

- Open the Flink Web UI: http://localhost:8081
- Under **Jobs → Running Jobs** you should see a running job. The SQL Client
  auto-generates its name from the statement set, e.g.
  `insert-into_default_catalog.default_database.output_table,default_catalog.default_database.print_table`.
- Open the job to view its graph, metrics, and throughput.

> Note: "Bytes/Records Received" and "Bytes/Records Sent" only count data
> exchanged **between Flink tasks** — not data read from or written to Kafka
> or MySQL. The source, lookup join, projection and both sinks are all chained
> into a single task by the SQL optimizer (confirmed on the job graph: `Source:
> input_table -> LookupJoin -> Calc -> (output_table: Writer -> Committer,
> Sink: print_table)`), so these counters stay at 0 even while records flow.
> This is an inherent limitation of this simple single-task pipeline.

## How the Job Works

`script.sql` defines four tables, a view, and one statement set:

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
4. `print_table` — a `print` sink (identifier `enriched`) that writes every row
   to the TaskManager's stdout, handy for watching results with
   `docker logs -f flink-taskmanager`.

`value` is a reserved SQL keyword, so the column name must be backtick-quoted
everywhere it's used. The enrichment is defined once as a temporary view that
joins the two sources, then written to both sinks from a single job with a
statement set:

```sql
CREATE TEMPORARY VIEW enriched AS
SELECT UPPER(i.`value`) AS `value`, m.`tier` AS `tier`
FROM input_table AS i
LEFT JOIN mysql_table FOR SYSTEM_TIME AS OF i.proc_time AS m
ON i.`id` = m.`id`;

EXECUTE STATEMENT SET
BEGIN
    INSERT INTO output_table SELECT * FROM enriched;
    INSERT INTO print_table  SELECT * FROM enriched;
END;
```

The planner inlines the view and shares the source and the lookup join between
the two inserts, so each Kafka message is looked up once and written to both
`output-topic` and the TaskManager's stdout.

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

## Changing SQL Without Rebuilding Images

Neither the Flink SQL job nor the MySQL seed data is baked into an image. `docker-compose.yml` mounts both files into their containers at start, so **editing them never requires rebuilding the image**.

| What you edit | File | How to apply |
| --- | --- | --- |
| Flink SQL job | `flink-sql-job/script.sql` (mounted into the submitter) | `docker compose up -d --force-recreate --no-deps flink-sql-job` |
| MySQL tables / seed data | `mock-database-a/init.sql` / `mock-database-b/init.sql` (mounted into the matching container) | `docker compose up -d --force-recreate --no-deps mock-database-a` (or `-b`) — the container is recreated and re-seeded; Kafka and Flink keep running |

Notes:

- Resubmitting (`docker compose up -d --force-recreate --no-deps flink-sql-job`) works against an already-running JobManager and TaskManager. It starts a **second**, independent job (the same statement set) reading from `earliest-offset` — Flink does not automatically replace the previous job. Cancel the old one from the dashboard if you don't want both running.
- MySQL only runs the files in `mock-database-a/` and `mock-database-b/` on a fresh database, so changes to `init.sql` need that database container recreated (command above; `docker compose down && docker compose up -d` recreates everything). Flink's JDBC lookup keeps working across the recreate — once the 10 s cache expires the next message queries the new instance. To change a running database without restarting, run the SQL by hand: `docker exec -i mock-database-a mysql -umock_database_a -pmock_database_a mock_database_a < mock-database-a/init.sql` (re-running `init.sql` as-is fails on the duplicate `INSERT` rows).
- If you add a MySQL table that Flink should read, also declare it in `script.sql` with a new `CREATE TABLE ... WITH ('connector' = 'jdbc', ...)`.

You only need to rebuild (`docker compose build`) when you change `flink-connectors/Dockerfile` (e.g. adding a connector jar).

## Removing the Built Image

To stop the stack and delete the image Compose built (`flink-connectors:1.18.1`):

```bash
docker compose down --rmi local
```

Pulled images (`confluentinc/cp-kafka`, `mysql`, `kafka-ui`, ...) are not touched.

## Stopping Services

`docker compose down` removes all containers and `kafka-network`. Because Kafka and MySQL data are ephemeral, this also discards all topics, messages and database contents; the next `up` re-creates and re-seeds them:

```bash
docker compose down
```

To remove the Flink named volumes as well (`apache-flink-lab_flink-jobmanager`, `apache-flink-lab_flink-taskmanager`; prefix is the project directory name):

```bash
docker compose down -v
```

If an older compose setup left stray containers behind, add `--remove-orphans`.

## Troubleshooting

**Useful commands:**
- Status: `docker compose ps -a`
- Logs: `docker compose logs -f <service>`
- Shell / command in a container: `docker exec -it <container> bash`
- Restart one service: `docker compose restart <service>`

**Job not showing on the dashboard?**
- The Flink UI is a single-page app and does not always auto-refresh — hard-refresh (Ctrl+Shift+R) and check **Jobs → Running Jobs**.
- Confirm the backend actually has the job: `curl -s http://localhost:8081/jobs`.
- Check the submitter logs: `docker logs flink-sql-job`.

**`flink-sql-job` exits non-zero?**
- Check `docker logs flink-sql-job` for connector/format errors from the SQL Client.
- Confirm the job actually submitted to the remote cluster rather than failing to connect — the `FLINK_PROPERTIES` on this service point the SQL Client at `flink-jobmanager`.

**No messages appearing in output-topic?**
- Verify you are producing valid JSON to `input-topic` (e.g. `{"id": 1, "value": "hello world"}`).
- Confirm the job is `RUNNING` on the dashboard.
- Check that the TaskManager is registered: `curl -s http://localhost:8081/taskmanagers`.

**Sent a malformed (non-JSON) message by accident?**
- `input_table` is created with `'json.ignore-parse-errors' = 'true'`, so a message
  that isn't valid JSON is silently dropped instead of crashing the job — later valid
  messages still get processed normally. No action needed.

**`Bind for 0.0.0.0:3306 failed: port is already allocated` (or 8080, 8081, 9092, ...)?**
- Another process (e.g. a local MySQL on 3306) already uses a host port from the table under "Services". Stop it, or change the left side of the port mapping in `docker-compose.yml`.

**Topics missing?**
- Check `docker compose logs kafka-init` and re-run it with `docker compose run --rm kafka-init`.

**`mock-database-a` / `mock-database-b` unhealthy, or `flink-sql-job` never starts?**
- Check `docker logs mock-database-a` (or `-b`) — first boot can take a few seconds to
  initialize and run `init.sql`.
- Confirm connectivity manually: `docker exec mock-database-a mysqladmin ping -h localhost -u root -prootpass`.
- Compose waits for both databases to report healthy before starting `flink-sql-job`.

**Updated a row in MySQL but still see the old tier (or still null)?**
- Expected within the cache window — see "Lookup Cache Behavior" above. Lookup
  results are cached for 10 seconds (`lookup.partial-cache.expire-after-write`).
  Wait past that window and send another message with the same `id` to see the
  refreshed value; no job resubmission needed.
