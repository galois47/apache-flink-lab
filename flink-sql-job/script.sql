CREATE TABLE input_table (
    `id` INT,
    `value` STRING,
    proc_time AS PROCTIME()
) WITH (
    'connector' = 'kafka',
    'topic' = 'input-topic',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'flink-sql-enrichment-job',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true',
    'scan.startup.mode' = 'earliest-offset'
);

CREATE TABLE mysql_table (
    `id` INT,
    `tier` STRING
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://mysql:3306/flinklab',
    'table-name' = 'membership_tiers',
    'username' = 'flinkuser',
    'password' = 'flinkpass',
    'lookup.cache' = 'PARTIAL',
    'lookup.partial-cache.max-rows' = '1000',
    'lookup.partial-cache.expire-after-write' = '10s',
    'lookup.max-retries' = '3'
);

CREATE TABLE output_table (
    `value` STRING,
    `tier` STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'output-topic',
    'properties.bootstrap.servers' = 'kafka:29092',
    'format' = 'json'
);

-- Print sink: writes each row to the TaskManager stdout, e.g.
-- "enriched:2> +I[HELLO, GOLD]". Follow with: docker compose logs -f flink-taskmanager
CREATE TABLE print_table (
    `value` STRING,
    `tier` STRING
) WITH (
    'connector' = 'print',
    'print-identifier' = 'enriched'
);

-- Enrichment lookup join: every Kafka message is looked up against the MySQL
-- membership_tiers table by id, as of the message's processing time. LEFT JOIN
-- guarantees every Kafka message produces exactly one output row, with
-- tier = NULL when the id doesn't match any row in MySQL. Unlike the previous
-- UNION ALL design, MySQL is never read once at startup -- each distinct id
-- triggers a fresh (or cached, see lookup.partial-cache.* above) JDBC query.
--
-- The same result is written to two sinks (Kafka and print) from a single job
-- via a statement set; the planner shares the source and the lookup join.
EXECUTE STATEMENT SET
BEGIN
    INSERT INTO output_table
    SELECT UPPER(i.`value`) AS `value`, m.`tier` AS `tier`
    FROM input_table AS i
    LEFT JOIN mysql_table FOR SYSTEM_TIME AS OF i.proc_time AS m
    ON i.`id` = m.`id`;

    INSERT INTO print_table
    SELECT UPPER(i.`value`) AS `value`, m.`tier` AS `tier`
    FROM input_table AS i
    LEFT JOIN mysql_table FOR SYSTEM_TIME AS OF i.proc_time AS m
    ON i.`id` = m.`id`;
END;
