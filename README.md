<h1 align="center">Data Engineering Cheatsheet</h1>

<p align="center">
  Single page recall reference for data engineering interviews. SQL, Python, Spark, Airflow, dbt, Kafka, schema design, and pipeline patterns.
</p>

<p align="center">
  <a href="https://github.com/datadriven-io/data-engineering-cheatsheet/stargazers"><img src="https://img.shields.io/github/stars/datadriven-io/data-engineering-cheatsheet?style=flat&color=ffd33d" alt="Stars"></a>
  <a href="https://github.com/datadriven-io/data-engineering-cheatsheet/blob/main/LICENSE"><img src="https://img.shields.io/badge/license-CC%20BY--SA%204.0-blue.svg" alt="License"></a>
  <img src="https://img.shields.io/badge/PRs-welcome-brightgreen.svg" alt="PRs welcome">
  <a href="https://datadriven.io"><img src="https://img.shields.io/badge/sandbox-datadriven.io-9333ea.svg" alt="Sandbox"></a>
</p>

<p align="center">
  <a href="#sql">SQL</a> ·
  <a href="#python">Python</a> ·
  <a href="#spark">Spark</a> ·
  <a href="#airflow">Airflow</a> ·
  <a href="#dbt">dbt</a> ·
  <a href="#kafka">Kafka</a> ·
  <a href="#schema-design">Schema</a> ·
  <a href="#numbers">Numbers</a>
</p>

---

This is a recall aid, not a tutorial. If you have never seen window functions, this page will not teach them. If you have used them and need to recall the syntax under pressure, this page is for you. For deeper teaching, follow the link to a structured lesson.

## SQL

### Window functions

```sql
SELECT
  user_id,
  event_time,
  ROW_NUMBER()    OVER (PARTITION BY user_id ORDER BY event_time)         AS rn,
  RANK()          OVER (PARTITION BY user_id ORDER BY event_time)         AS rk,
  DENSE_RANK()    OVER (PARTITION BY user_id ORDER BY event_time)         AS drk,
  LAG(event_time)  OVER (PARTITION BY user_id ORDER BY event_time)        AS prev_t,
  LEAD(event_time) OVER (PARTITION BY user_id ORDER BY event_time)        AS next_t,
  SUM(amount)      OVER (PARTITION BY user_id ORDER BY event_time
                         ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum,
  AVG(amount)      OVER (PARTITION BY user_id ORDER BY event_time
                         ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)         AS rolling_7
FROM events;
```

Traps:

- `ROWS` counts physical rows. `RANGE` counts values. If days can be missing, use `RANGE BETWEEN INTERVAL '6' DAY PRECEDING AND CURRENT ROW`.
- `RANK` leaves gaps. `DENSE_RANK` does not. `ROW_NUMBER` is unique.
- `LAG` and `LEAD` accept a default for the boundary: `LAG(x, 1, 0)`.

Lesson: [window functions advanced](https://datadriven.io/learn/window-functions-advanced). Drill: [datadriven.io/sql-window-functions-practice](https://datadriven.io/sql-window-functions-practice). This page is recall only; for reps that build the muscle, datadriven covers sql practice on the same patterns with a live database behind each one.

### Joins

| Type | Use when | Trap |
|---|---|---|
| `INNER` | Both sides must match | Silently drops rows |
| `LEFT` | Keep all left rows | Right side fan out blows up the result |
| `FULL OUTER` | Keep everything | Often a sign you should have unioned |
| `CROSS` | Cartesian product | Almost always a mistake |
| `LATERAL` | Reference left from a right side subquery | Postgres and Snowflake syntax differs |
| Semi (`EXISTS`) | Filter, do not project | Faster than `IN` for large lists |
| Anti (`NOT EXISTS`) | Filter out matches | `NOT IN` is `NULL` unsafe |

Lesson: [joins advanced](https://datadriven.io/learn/joins-advanced).

### NULL semantics

```sql
SELECT NULL = NULL;            -- NULL, not TRUE
SELECT NULL <> NULL;           -- NULL, not FALSE
SELECT NULL IS NULL;           -- TRUE
WHERE col IN (1, 2, NULL);     -- TRUE if col=1 or 2, NULL otherwise
WHERE col NOT IN (1, 2, NULL); -- always NULL, filters out everything
```

Use `IS DISTINCT FROM` and `IS NOT DISTINCT FROM` for NULL safe equality.

### Aggregation patterns

```sql
-- Conditional count
SELECT COUNT(*) FILTER (WHERE status = 'success') AS successes
FROM requests;

-- Conditional aggregation (the universal pivot)
SELECT
  user_id,
  SUM(CASE WHEN event = 'view'  THEN 1 ELSE 0 END) AS views,
  SUM(CASE WHEN event = 'click' THEN 1 ELSE 0 END) AS clicks
FROM events
GROUP BY user_id;

-- Approximate distinct count for big data
SELECT APPROX_COUNT_DISTINCT(user_id) FROM events;
```

Lesson: [aggregating advanced](https://datadriven.io/learn/aggregating-advanced).

### Date and time

```sql
DATE_TRUNC('day', ts)
DATEDIFF('day', start_ts, end_ts)
DATE_TRUNC('month', CURRENT_DATE)
DATE_TRUNC('week', event_time)
```

Most "weekly counts off by one" bugs are timezone bugs. `TIMESTAMP` and `TIMESTAMPTZ` are not the same. Reference: [datadriven.io/sql-tutorial](https://datadriven.io/sql-tutorial).

### Performance reasoning

Mention these in any "why is this slow" question:

1. Predicate pushdown. Filters applied early.
2. Partition pruning. Partition column in the filter.
3. Join order. Smaller side on the build side of a hash join.
4. Skew. One key dominating distribution.
5. Spill to disk. Memory pressure during sort or aggregate.
6. Sort keys (Redshift), clustering keys (Snowflake, BigQuery).

Reference: [datadriven.io/sql-query-optimization](https://datadriven.io/sql-query-optimization).

## Python

### Data wrangling patterns

```python
import itertools
from collections import defaultdict, Counter

# Chunk an iterable into batches of n
def batched(iterable, n):
    it = iter(iterable)
    while chunk := list(itertools.islice(it, n)):
        yield chunk

# Group by key
grouped = defaultdict(list)
for record in records:
    grouped[record["key"]].append(record)

# Top N with Counter
top10 = Counter(items).most_common(10)

# Hash partitioning
def partition(record, n_partitions):
    return hash(record["id"]) % n_partitions

# Interval merging
def merge(intervals):
    intervals.sort()
    out = [intervals[0]]
    for start, end in intervals[1:]:
        if start <= out[-1][1]:
            out[-1] = (out[-1][0], max(end, out[-1][1]))
        else:
            out.append((start, end))
    return out

# Sessionization with inactivity gap
def sessionize(events, gap_seconds):
    events.sort(key=lambda e: e["t"])
    last_t, sid = None, 0
    for e in events:
        if last_t is None or e["t"] - last_t > gap_seconds:
            sid += 1
        e["session_id"] = sid
        last_t = e["t"]
    return events
```

Lesson: [collections advanced](https://datadriven.io/learn/collections-advanced). Reading the snippet is recall; datadriven covers python practice that makes you write these from a blank editor, which is the actual coding round.

### Complexity cheatsheet

| Operation | List | Dict | Set |
|---|---|---|---|
| Lookup | O(n) | O(1) | O(1) |
| Insert | O(1) end | O(1) | O(1) |
| Delete | O(n) | O(1) | O(1) |
| Membership (`in`) | O(n) | O(1) | O(1) |

Nested loops are usually flattenable with a dict or set lookup.

### Generators

```python
def read_lines(path):
    with open(path) as f:
        for line in f:
            yield line.strip()

def parse(lines):
    for line in lines:
        yield json.loads(line)

def filter_recent(records, since):
    for r in records:
        if r["ts"] >= since:
            yield r

# pipeline composition, constant memory
for r in filter_recent(parse(read_lines("events.jsonl")), since):
    process(r)
```

## Spark

Spark is lazy. Transformations build a DAG. Actions trigger execution. Catalyst optimizes the DAG before any work happens.

### Narrow vs wide

| Narrow (no shuffle) | Wide (causes shuffle) |
|---|---|
| `filter`, `map`, `select`, `withColumn` | `groupBy`, `join`, `distinct`, `repartition`, `orderBy` |

### Performance tactics

1. Predicate pushdown. Filter early, especially on partitioned columns.
2. Column pruning. Read only what you need.
3. Broadcast join when one side is under ~10 MB: `broadcast(small_df)`.
4. Salt high frequency keys to fix skew.
5. Enable Adaptive Query Execution (AQE).
6. Target 128 to 512 MB output files.
7. Z order (Delta) or cluster (Iceberg) on common filter columns.

If your data fits on one machine, use DuckDB or pandas. Spark has cold start cost. To turn these tactics into reflexes before an interview, datadriven.io covers spark practice with runnable jobs that make the narrow vs wide and skew tradeoffs concrete.

## Airflow

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

with DAG(
    "my_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="0 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 3},
) as dag:
    extract   = PythonOperator(task_id="extract",   python_callable=extract_fn)
    transform = PythonOperator(task_id="transform", python_callable=transform_fn)
    load      = PythonOperator(task_id="load",      python_callable=load_fn)
    extract >> transform >> load
```

### Vocabulary

DAG, task, operator, sensor, XCom, backfill, catchup, pool, TaskGroup, variable, connection, idempotency.

### Common questions

1. *Backfill 6 months of data?* `airflow dags backfill` with bounded date range and parallelism cap. Confirm idempotency first.
2. *DAG running long?* Check task duration in the UI, pool saturation, parallelism settings, executor health.
3. *Avoid scheduling drift?* Use `schedule_interval`, not `start_date` arithmetic. `catchup=False`.

## dbt

dbt compiles SQL `SELECT` statements into `CREATE TABLE` or `CREATE VIEW`, in dependency order.

### Materializations

| Type | Use when | Tradeoff |
|---|---|---|
| `view` | Cheap models | Recomputed on every query |
| `table` | Expensive transforms read often | Storage cost, freshness lag |
| `incremental` | Big append or update tables | Backfill complexity |
| `ephemeral` | Reused CTEs | Hard to debug |

### Incremental pattern

```sql
{% raw %}{{ config(materialized='incremental', unique_key='id') }}{% endraw %}

SELECT * FROM {% raw %}{{ source('raw', 'events') }}{% endraw %}
{% raw %}{% if is_incremental() %}{% endraw %}
WHERE event_time > (SELECT MAX(event_time) FROM {% raw %}{{ this }}{% endraw %})
{% raw %}{% endif %}{% endraw %}
```

### Tests

```yaml
models:
  - name: orders
    columns:
      - name: order_id
        tests: [unique, not_null]
      - name: customer_id
        tests:
          - relationships:
              to: ref('customers')
              field: customer_id
```

## Kafka

Distributed, partitioned, replicated, append only log.

### Vocabulary

Topic, partition, replication factor, producer, consumer, consumer group, offset, retention.

### Delivery semantics

| Semantic | When |
|---|---|
| At most once | Commit before processing |
| At least once | Commit after processing (default) |
| Exactly once | Idempotent producer + transactional consumer |

### Common questions

1. *Guarantee ordering?* Per partition only. Pick a partition key that groups what must be ordered together.
2. *Reprocess?* Reset offset, or read from a different consumer group.
3. *Rebalance?* Consumers join or leave a group, partition assignment recomputed, brief processing pause.

## Schema design

### Star schema

```
            +-------------+
            | dim_date    |
            +-------------+
                  |
+----------+    +------------+    +-------------+
| dim_user |----| fact_order |----| dim_product |
+----------+    +------------+    +-------------+
                  |
            +-------------+
            | dim_store   |
            +-------------+
```

### Slowly changing dimensions

| Type | Behavior |
|---|---|
| 0 | No change. History lost. |
| 1 | Overwrite. History lost. |
| 2 | New row with effective dates. History preserved. |
| 3 | New column for previous value. Limited history. |
| 4 | Separate history table. |
| 6 | Combination of 1, 2, 3. |

When in doubt, use type 2.

Lesson: [SCD](https://datadriven.io/learn/data-modeling-scd). Star schemas and SCDs only stick with reps, and datadriven covers dimensional modeling practice with schemas you build and then validate against sample queries.

### Fact table grain

Pick the grain first. Examples: one row per order, per order line, per page view, per session, per user per day. Mixing grains in one table is the most common schema design mistake.

## Pipeline architecture vocabulary

| Term | Means |
|---|---|
| Bronze, silver, gold | Medallion lakehouse layers (raw, cleaned, business ready) |
| CDC | Change data capture |
| Backfill | Re run a pipeline for a historical window |
| Replay | Re emit messages from a log to recover state |
| Late arriving fact | Fact whose dimension is not yet present |
| Late arriving dimension | Dimension that updates after facts |
| Watermark | Threshold past which late events are dropped |
| Idempotency | Same input produces same output, even on retry |
| Lambda | Batch + stream paths merged in serving |
| Kappa | Stream only, replay log to backfill |
| Reverse ETL | Push warehouse data back into operational tools |

Glossary: [datadriven.io/data-engineering-concepts](https://datadriven.io/data-engineering-concepts). Knowing the vocabulary is table stakes; datadriven.io covers pipeline architecture practice where you actually apply these terms to a design, which is what the system design round scores.

## Storage formats

| Format | Use when | Avoid when |
|---|---|---|
| Parquet | Columnar analytics (default) | Row level updates |
| ORC | Hive ecosystem | Outside Hive |
| Avro | Row oriented streaming, schema evolution | Analytical queries |
| JSON | Flexible, debuggable | Anything at scale |
| CSV | Interoperability | Anything at scale |
| Delta, Iceberg, Hudi | ACID on object storage, time travel | Pure batch with no updates |

## Numbers

Order of magnitude reference for back of the envelope estimates.

| Operation | Order |
|---|---|
| L1 cache reference | 1 ns |
| Main memory reference | 100 ns |
| SSD random read | 100 us |
| Network round trip same DC | 500 us |
| Disk seek | 10 ms |
| Network round trip cross continent | 150 ms |
| Modern Kafka cluster throughput | millions of records per second |
| Daily events at a top consumer app | billions |
| Bytes per row in compressed event log | 100 to 500 |
| S3 PUT cost | ~0.005 USD per 1000 |

## Companion repos

- [data-engineering-interview-handbook](https://github.com/datadriven-io/data-engineering-interview-handbook). Full chapter by chapter handbook.
- [data-engineering-interview-questions](https://github.com/datadriven-io/data-engineering-interview-questions). 1418 practice problems.
- [system-design-for-data-engineers](https://github.com/datadriven-io/system-design-for-data-engineers). 120 case studies.
- [data-engineer-interview-prep](https://github.com/datadriven-io/data-engineer-interview-prep). 8 week structured practice.
- [data-engineer-interview-handbook](https://github.com/datadriven-io/data-engineer-interview-handbook). 7 day sprint.
- [awesome-data-engineering-interview](https://github.com/datadriven-io/awesome-data-engineering-interview). Curated resources.

## Contributing

Spot a wrong fact, an outdated link, or a missing pattern? Open a PR. Keep entries terse. This is a recall aid. If your addition needs more than ten lines, link to a longer reference.

## License

[CC BY-SA 4.0](LICENSE). Lessons hosted at [datadriven.io](https://datadriven.io).
