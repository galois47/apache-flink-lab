# Flink + Kafka + MySQL lab

A Flink SQL job reads JSON from Kafka `input-topic`, enriches each message with a live MySQL lookup, and writes to `output-topic`. See `README.md` for behavior and usage.

## Container tooling: Docker + Compose
- This repo uses **Docker Engine with Compose v2**. `docker-compose.yml` is the source of truth for the stack. Do not reintroduce Podman, `podman run` scripts or `Containerfile`s.
- Add new services to `docker-compose.yml`. Use `healthcheck` + `depends_on: condition: service_healthy` (or `service_completed_successfully` for one-shot jobs) for startup ordering.
- The only locally built image is `flink-connectors:1.18.1` (`flink-connectors/Dockerfile`, built by Compose). Kafka uses the stock `confluentinc/cp-kafka` image with a fixed `CLUSTER_ID` env var.
- `container_name` is pinned on every service so hostnames (`kafka`, `flink-jobmanager`) and `docker exec` / `docker logs` commands stay stable.

## Layout
- `docker-compose.yml`: all services; Kafka topics are the `TOPICS` env var of the `kafka-init` one-shot service
- `flink-connectors/Dockerfile`: single Flink image with Kafka/JDBC jars, used for JobManager, TaskManager and the SQL submitter (different commands)
- `flink-sql-job/script.sql`: the job; mounted into the submitter, so edits need only `docker compose up -d --force-recreate --no-deps flink-sql-job` (no rebuild)
- `mock-database-a/init.sql`, `mock-database-b/init.sql`: seed the two MySQL containers (`membership_tiers` is the Flink lookup in A; B holds `customer_regions`, unused by the job so far)
- Database naming convention: container `mock-database-<x>`; database, user and password `mock_database_<x>`; host ports A=3306, B=3307

## Common commands
- Build: `docker compose build`
- Start / stop: `docker compose up -d`, `docker compose down [-v]`
- Add topics: edit `TOPICS` in `docker-compose.yml`, then `docker compose run --rm kafka-init`
- Resubmit job: `docker compose up -d --force-recreate --no-deps flink-sql-job` (starts an additional job; cancel the old one in the dashboard)
- Logs: `docker compose logs -f flink-taskmanager`
