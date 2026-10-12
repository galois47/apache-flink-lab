# Flink-Kafka Streaming Pipeline

A Docker-based playground for Apache Flink with Kafka integration. A Flink SQL job
consumes JSON messages from one Kafka topic, transforms them, and writes the
result to another topic. Kafka runs in **KRaft mode** (no Zookeeper).

## Project Structure

```
.
├── docker-compose.yml         # Orchestrates all services
├── build-all-images.sh        # Builds every custom image used by the compose file
├── kafka-image/
│   ├── Dockerfile             # Kafka (KRaft) with auto-created topics
│   └── entrypoint.sh          # Boots Kafka, then creates input-topic and output-topic
├── flink-jobmanager/
│   └── Dockerfile             # Flink JobManager + Kafka connector jars
├── flink-taskmanager/
│   └── Dockerfile             # Flink TaskManager + Kafka connector jars
├── flink-sql-job/
│   ├── Dockerfile             # Job-submitter image (SQL Client + connectors)
│   └── script.sql             # The Flink SQL streaming job
└── flink-kafka/
    └── Dockerfile             # Base Flink image with Kafka connectors
```

## Services

- **kafka**: Message broker in KRaft mode. Auto-creates `input-topic` and `output-topic` on startup.
- **kafka-ui**: Web interface for Kafka — http://localhost:8080
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
| `input-topic`  | **You produce**  | JSON objects with a `value` field, e.g. `{"value": "hello world"}`. |
| `output-topic` | **Flink writes** | Do **not** produce here manually. Flink writes the transformed (upper-cased) result. |

The job's Kafka tables use the `json` format, so every message must be a JSON
object with a `value` field. Flink reads that field, upper-cases it, and
writes a JSON object with the same shape to `output-topic`.

## Monitoring the Job in Real Time

### 1. Produce Test Messages

- Open Kafka UI: http://localhost:8080
- Go to **Topics** → **input-topic** → **Produce Message**
- Enter a JSON object in the message **value** field, for example:
  ```
  {"value": "hello world"}
  ```
  ```
  {"value": "flink is running"}
  ```
- Click **Produce**.

### 2. View Processed Output

- Go to **Topics** → **output-topic** and view the messages.
- Each input message appears upper-cased in real time:

  | input-topic                         | output-topic                        |
  | ------------------------------------ | ------------------------------------ |
  | `{"value":"hello world"}`            | `{"value":"HELLO WORLD"}`            |
  | `{"value":"flink is running"}`       | `{"value":"FLINK IS RUNNING"}`       |

### 3. Check the Flink Dashboard

- Open the Flink Web UI: http://localhost:8081
- Under **Jobs → Running Jobs** you should see a running job (the SQL Client
  auto-generates the job name from the `INSERT INTO` statement, so it won't be
  named `Kafka Stream Job` anymore).
- Open the job to view its graph, metrics, and throughput.

> Note: "Bytes/Records Received" and "Bytes/Records Sent" only count data
> exchanged **between Flink tasks** — not data read from or written to Kafka.
> Since the source, projection, and sink are chained into a single task by the
> SQL optimizer, these counters stay at 0 even while records flow. This is an
> inherent limitation of this simple single-operator pipeline.

## How the Job Works

`script.sql` defines two Kafka-backed tables using Flink's Kafka SQL connector
and the `json` format:

1. `input_table` — reads from `input-topic`, exposing a single `` `value` `` column.
   `'json.ignore-parse-errors' = 'true'` is set here so a malformed (non-JSON)
   message is silently skipped instead of crashing the job.
2. `output_table` — writes to `output-topic`, same shape.

`value` is a reserved SQL keyword, so the column name must be backtick-quoted
everywhere it's used. A single statement wires the two tables together:

```sql
INSERT INTO output_table
SELECT UPPER(`value`) FROM input_table;
```

Every row read from `input-topic` is upper-cased and written to `output-topic`.
Because this is pure Flink SQL, **no Python or PyFlink is needed anywhere in
the stack** — the Kafka SQL connector jar already present in the Flink images
is sufficient.

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
- Verify you are producing valid JSON to `input-topic` (e.g. `{"value": "hello world"}`).
- Confirm the job is `RUNNING` on the dashboard.
- Check that the TaskManager is registered: `curl -s http://localhost:8081/taskmanagers`.

**Sent a malformed (non-JSON) message by accident?**
- `input_table` is created with `'json.ignore-parse-errors' = 'true'`, so a message
  that isn't valid JSON is silently dropped instead of crashing the job — later valid
  messages still get processed normally. No action needed.
