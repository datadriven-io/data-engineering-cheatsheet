# The Data Engineering Cheatsheet

> One page (well, one long page) reference for everything a data engineer needs to recall under interview pressure. SQL, Python, Spark, Airflow, dbt, Kafka, schema design, and the operational vocabulary that separates seniors from staff.

[![License: CC BY-SA 4.0](https://img.shields.io/badge/License-CC_BY--SA_4.0-lightgrey.svg)](https://creativecommons.org/licenses/by-sa/4.0/)

## How to use this cheatsheet

This is a recall aid, not a tutorial. If you have never seen window functions before, this page will not teach them to you. If you have used them and just need to remember the syntax under time pressure, this page is for you.

For deep teaching on any topic, follow the link to a structured lesson with runnable examples.

## Contents

1. [SQL](#sql)
2. [Python](#python)
3. [Spark](#spark)
4. [Airflow](#airflow)
5. [dbt](#dbt)
6. [Kafka](#kafka)
7. [Schema design](#schema-design)
8. [Pipeline architecture vocabulary](#pipeline-architecture-vocabulary)
9. [Storage formats and tradeoffs](#storage-formats-and-tradeoffs)
10. [The numbers every DE should know](#the-numbers-every-de-should-know)

## SQL

### Window functions

The single highest leverage SQL topic for DE interviews. Memorize the syntax.

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

**Traps to remember:**

- `ROWS` counts physical rows. `RANGE` counts values. If days can be missing, use `RANGE BETWEEN INTERVAL '6' DAY PRECEDING AND CURRENT ROW`, not `ROWS`.
- `RANK` leaves gaps. `DENSE_RANK` does not. `ROW_NUMBER` is unique.
- `LAG` and `LEAD` accept a default for the boundary case: `LAG(x, 1, 0)`.
- `PARTITION BY` resets the window. `ORDER BY` sorts within it.

Drill: <https://datadriven.io/sql-window-functions-practice>. Lesson: <https://datadriven.io/learn/window-functions-advanced>.

### Joins

| Type | When to use | Trap |
|---|---|---|
| `INNER` | Both sides must have a match | Silently drops rows you expected to keep |
| `LEFT` | Keep all rows from the left | Beware of right side fan out blowing up the result |
| `FULL OUTER` | Keep everything from both sides | Often a sign you should have unioned instead |
| `CROSS` | Cartesian product | Almost always a mistake |
| `LATERAL` | Reference left side from a subquery on the right | Postgres and Snowflake syntax differs |
| Semi (`EXISTS`) | Filter, do not project | Faster than `IN` for large lists |
| Anti (`NOT EXISTS`) | Filter out matches | Beware of `NULL` semantics with `NOT IN` |

Lesson: <https://datadriven.io/learn/joins-advanced>.

### NULL semantics, the three valued logic trap

```sql
SELECT NULL = NULL;      -- NULL, not TRUE
SELECT NULL <> NULL;     -- NULL, not FALSE
SELECT NULL IS NULL;     -- TRUE
WHERE col IN (1, 2, NULL);     -- TRUE if col=1 or 2, NULL otherwise
WHERE col NOT IN (1, 2, NULL); -- always NULL, filters out everything
```

Use `IS DISTINCT FROM` and `IS NOT DISTINCT FROM` when you want NULL safe equality.

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

-- Distinct count
SELECT COUNT(DISTINCT user_id) FROM events;

-- Approximate distinct count for big data
SELECT APPROX_COUNT_DISTINCT(user_id) FROM events;
```

Lesson: <https://datadriven.io/learn/aggregating-advanced>.

### Date and time

```sql
-- Truncate to day
DATE_TRUNC('day', ts)

-- Difference in days
DATEDIFF('day', start_ts, end_ts)

-- First day of month
DATE_TRUNC('month', CURRENT_DATE)

-- Bucket events by week
DATE_TRUNC('week', event_time)
```

Watch out for timezone semantics. `TIMESTAMP` and `TIMESTAMPTZ` are not the same. Most "weekly counts are off by one" bugs are timezone bugs.

Reference: <https://datadriven.io/sql-tutorial>.

### Performance reasoning

Things to mention in any "why is this slow" question:

1. Predicate pushdown. Are filters being applied early?
2. Partition pruning. Is the partition column in the filter?
3. Join order. Smaller side on the build side of a hash join.
4. Skew. One key dominating the distribution.
5. Spill to disk. Memory pressure during sort or aggregate.
6. Sort keys (Redshift) or clustering keys (Snowflake, BigQuery).

Reference: <https://datadriven.io/sql-query-optimization>.

## Python

### Data wrangling patterns

```python
# Chunk an iterable into batches of n
def batched(iterable, n):
    it = iter(iterable)
    while True:
        chunk = list(itertools.islice(it, n))
        if not chunk: return
        yield chunk

# Group by key
from collections import defaultdict
grouped = defaultdict(list)
for record in records:
    grouped[record["key"]].append(record)

# Counter for top N
from collections import Counter
top10 = Counter(items).most_common(10)

# Hash partitioning
def partition_key(record, n_partitions):
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

# Sessionization (gap based)
def sessionize(events, gap_seconds):
    events.sort(key=lambda e: e["t"])
    session, last_t, sid = [], None, 0
    for e in events:
        if last_t is None or e["t"] - last_t > gap_seconds:
            sid += 1
        e["session_id"] = sid
        last_t = e["t"]
    return events
```

Lesson: <https://datadriven.io/learn/collections-advanced>.

### Complexity cheatsheet

| Operation | List | Dict | Set |
|---|---|---|---|
| Lookup | O(n) | O(1) | O(1) |
| Insert | O(1) end | O(1) | O(1) |
| Delete | O(n) | O(1) | O(1) |
| Iteration | O(n) | O(n) | O(n) |
| Membership (`in`) | O(n) | O(1) | O(1) |

If you find yourself writing nested loops, ask whether a dict or set can flatten it.

Lesson: <https://datadriven.io/learn/complexity-advanced>.

### Generators and iterators

Use generators when memory matters more than indexing. Almost every "process this huge file" question has a generator answer.

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

# pipeline composition
for r in filter_recent(parse(read_lines("events.jsonl")), since):
    process(r)
```

## Spark

### The mental model

Spark is lazy. Transformations build a DAG. Actions trigger execution. The DAG gets optimized by Catalyst before any work happens.

```python
df = spark.read.parquet("s3://bucket/events/")  # transformation, no work
df = df.filter(df.event_type == "purchase")     # transformation, no work
df = df.groupBy("user_id").sum("amount")        # transformation, no work
df.write.parquet("s3://bucket/agg/")            # action, work happens
```

### Narrow vs wide transformations

| Narrow | Wide (causes shuffle) |
|---|---|
| `filter`, `map`, `select`, `withColumn` | `groupBy`, `join`, `distinct`, `repartition`, `orderBy` |

Shuffles are expensive. Minimize them.

### Performance tactics

1. **Predicate pushdown.** Filter as early as possible, especially on partitioned columns.
2. **Column pruning.** Only read the columns you need. Parquet makes this free.
3. **Broadcast join.** When one side is under ~10 MB, force `broadcast(small_df)`.
4. **Skew.** Salt high frequency keys to spread the load.
5. **AQE (Adaptive Query Execution).** Enable it. Spark 3 onward.
6. **File size.** Aim for 128 to 512 MB output files. Too small and metadata kills you, too large and you lose parallelism.
7. **Z ordering** (Delta) or **clustering** (Iceberg) for read locality on common filter columns.

### When NOT to use Spark

If your data fits in memory on one machine, use DuckDB or pandas. Spark has a real cold start cost.

## Airflow

### DAG basics

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

with DAG(
    "my_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="0 * * * *",          # hourly
    catchup=False,                 # do not backfill on resume
    max_active_runs=1,             # serialize runs
    default_args={"retries": 3},
) as dag:
    t1 = PythonOperator(task_id="extract",   python_callable=extract)
    t2 = PythonOperator(task_id="transform", python_callable=transform)
    t3 = PythonOperator(task_id="load",      python_callable=load)
    t1 >> t2 >> t3
```

### Vocabulary you should know

- **DAG.** Directed acyclic graph of tasks.
- **Task.** A unit of work. Maps to an operator.
- **Operator.** A task type. Python, Bash, SQL, KubernetesPod, etc.
- **Sensor.** A task that polls for a condition.
- **XCom.** Cross task communication. Use sparingly. Not for big data.
- **Backfill.** Re run historical schedules.
- **Catchup.** Should Airflow auto run missed schedules on startup. Almost always `False`.
- **Pool.** Concurrency limiter for shared resources.
- **TaskGroup.** Visual grouping of tasks. No execution semantics.
- **Variable / Connection.** Stored config and credentials.
- **Idempotency.** Critical. Tasks must produce the same output for the same `logical_date`.

### Common interview questions

1. "How would you backfill 6 months of data?" Use `airflow dags backfill` with bounded date range and a parallelism cap. Confirm idempotency first.
2. "Your DAG is running long. What do you check?" Task duration in the UI, pool saturation, dependencies between tasks, parallelism settings, executor health.
3. "How do you avoid scheduling drift?" Use `schedule_interval`, not `start_date` arithmetic. Use `catchup=False`.

## dbt

### The mental model

dbt is a SQL compiler with a dependency graph. You write SQL `SELECT` statements. dbt wraps them in `CREATE TABLE` or `CREATE VIEW` and runs them in dependency order.

### Materializations

| Type | When | Tradeoff |
|---|---|---|
| `view` | Cheap models, no big data scans | Recomputed on every query |
| `table` | Expensive transforms read frequently | Storage cost, freshness lag |
| `incremental` | Big tables with append only or update only patterns | Backfill complexity |
| `ephemeral` | Reused CTEs without materialization | Hard to debug |

### Incremental pattern

```sql
{% raw %}{{ config(materialized='incremental', unique_key='id') }}{% endraw %}

SELECT * FROM {% raw %}{{ source('raw', 'events') }}{% endraw %}
{% raw %}{% if is_incremental() %}{% endraw %}
WHERE event_time > (SELECT MAX(event_time) FROM {% raw %}{{ this }}{% endraw %})
{% raw %}{% endif %}{% endraw %}
```

### Tests, the easy win

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

### Vocabulary

- **Model.** A `SELECT` statement. The unit of dbt.
- **Source.** A reference to a raw table.
- **Ref.** `{% raw %}{{ ref('model_name') }}{% endraw %}`. The dependency mechanism.
- **Seed.** CSV files compiled into tables. Use sparingly.
- **Snapshot.** SCD type 2 implementation in dbt.
- **Macro.** Reusable Jinja function.
- **Exposure.** Downstream consumer of dbt output.

## Kafka

### The mental model

A distributed, partitioned, replicated, append only log.

- **Topic.** A named log.
- **Partition.** A slice of a topic. Messages within a partition are ordered.
- **Replication factor.** How many copies of each partition.
- **Producer.** Writes messages, picks partition by key (or round robin).
- **Consumer.** Reads from one or more partitions. Tracks offsets.
- **Consumer group.** A set of consumers sharing offsets. One partition is read by exactly one consumer in the group at a time.
- **Offset.** Monotonic position in a partition. Consumer commits this when done.
- **Retention.** Time or size based. Old messages eventually drop off.

### Delivery semantics

| Semantic | When |
|---|---|
| At most once | Commit before processing. Lose on crash. |
| At least once | Commit after processing. Reprocess on crash. The default. |
| Exactly once | Idempotent producer plus transactional consumer. Possible since Kafka 0.11, with caveats. |

### Common interview questions

1. "How do you guarantee ordering?" Per partition only. Pick a partition key that groups what must be ordered together.
2. "How do you reprocess?" Reset consumer offset to earlier point. Or read from a different consumer group.
3. "What is rebalance?" Consumers join or leave a group, partition assignment is recomputed. Causes brief processing pause.

## Schema design

### Star schema

Fact table at the center. Dimension tables around it. Joins on surrogate keys.

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
| 2 | Add new row with effective dates. History preserved. |
| 3 | Add new column for previous value. Limited history. |
| 4 | History table separate from current table. |
| 6 | Combination of 1, 2, and 3. |

When in doubt, use type 2. It is the only one that preserves full history.

Lesson: <https://datadriven.io/learn/data-modeling-scd>.

### Fact table grain

The grain is the unit of one row. Pick it before anything else. Examples:

- One row per order
- One row per order line item
- One row per page view
- One row per session
- One row per user per day

Mixing grains in one table is the most common schema design mistake.

### When to denormalize

Almost never on the operational side. Often on the analytics side. The tradeoff is read speed vs write complexity vs storage cost. In a warehouse, denormalize. In an OLTP database, normalize.

Lesson: <https://datadriven.io/learn/data-modeling-normalization>.

## Pipeline architecture vocabulary

| Term | What it means |
|---|---|
| Bronze / silver / gold | Medallion lakehouse layers. Raw, cleaned, business ready. |
| CDC | Change data capture. Replicate row level changes from a source database. |
| Backfill | Re run a pipeline for a historical time window. |
| Replay | Re emit messages from a log to recover state. |
| Late arriving fact | A fact whose dimension is not yet present. Hold or fill or reject. |
| Late arriving dimension | A dimension that updates after facts have been processed. Backfill or use SCD2. |
| Watermark | A timestamp threshold past which late events are dropped. |
| Idempotency | Same input produces same output, even on retry. |
| Exactly once | Each input record affects state exactly one time. |
| At least once | Each input record affects state one or more times. The default. |
| Lambda architecture | Batch path plus stream path, merged in serving. |
| Kappa architecture | Stream only, replay log to backfill. |
| Medallion | Lakehouse layered model. |
| Reverse ETL | Push warehouse data back into operational tools. |

A longer glossary lives at <https://datadriven.io/data-engineering-concepts>.

## Storage formats and tradeoffs

| Format | Use when | Avoid when |
|---|---|---|
| Parquet | Columnar analytics. The default. | Row level updates needed |
| ORC | Hive ecosystem | Outside the Hive ecosystem |
| Avro | Row oriented streaming. Schema evolution. | Analytical queries |
| JSON | Flexible schema, debuggable | Anything at scale |
| CSV | Interoperability, debugging | Anything at scale |
| Delta / Iceberg / Hudi | ACID on object storage, time travel | Pure batch with no updates |

## The numbers every DE should know

Memorize these. They are the basis for the back of the envelope estimates interviewers expect.

| Operation | Order of magnitude |
|---|---|
| L1 cache reference | 1 ns |
| Main memory reference | 100 ns |
| SSD random read | 100 us |
| Network round trip same DC | 500 us |
| Disk seek | 10 ms |
| Network round trip cross continent | 150 ms |
| Records per second a modern Kafka cluster handles | millions |
| Daily events at a top consumer app | billions |
| Bytes per row in a typical event log (compressed) | 100 to 500 |
| S3 PUT cost | ~0.005 USD per 1000 |
| Snowflake credit cost | ~3 USD per credit (varies) |

## Topic specific deep dives

This cheatsheet is the recall layer. For learning the underlying concepts, follow these:

- SQL: <https://datadriven.io/sql-interview-questions>
- Python: <https://datadriven.io/python-interview-questions>
- Data modeling: <https://datadriven.io/data-modeling-interview-questions>
- Pipeline architecture: <https://datadriven.io/data-pipeline-interview-questions>
- System design framework: <https://datadriven.io/data-engineering-system-design>
- Concept index: <https://datadriven.io/data-engineering-concepts>

## Contributing

Spot a wrong fact, an outdated link, or a missing pattern? Open a PR. Keep entries terse. This file is a recall aid. If your addition needs more than ten lines, link to a longer reference instead.

## License

CC BY-SA 4.0. Lessons and runnable examples are hosted at <https://datadriven.io>.
