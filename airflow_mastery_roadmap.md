# Apache Airflow — Complete Mastery Roadmap
### For Data Engineers with ~2 Years Experience
### Last Updated: April 2026

---

## How This Roadmap Is Structured

```
🟢 ESSENTIAL    → Must know for 2 YOE. Will be asked in interviews. Non-negotiable.
🟡 IMPORTANT    → Expected at 2 YOE. Shows depth. Often differentiates candidates.
🔵 BONUS        → Advanced. Shows you're ahead of your level. Creates "wow" moments in interviews.
```

---

## PHASE 1: FOUNDATION (Week 1-2)
> Goal: Understand what Airflow is, why it exists, and how it works internally.

---

### 1.1 What is Apache Airflow? 🟢

- Airflow is a **workflow orchestration platform** — it does NOT process data itself.
- It **schedules, monitors, and manages** the execution of tasks in a defined order.
- Think of it as a **conductor of an orchestra** — it tells each instrument (task) when to play, but doesn't play itself.

**Key distinction to understand:**
```
Airflow ≠ ETL Tool (like Informatica or Spark)
Airflow = Orchestrator that TELLS ETL tools WHEN and in WHAT ORDER to run
```

**Why Airflow exists:**
- Before Airflow, people used cron jobs — but cron has no dependency management, no retry logic, no monitoring, no alerting.
- Airflow was created at **Airbnb in 2014** by Maxime Beauchemin, open-sourced in 2015, became Apache top-level project in 2019.
- It is now the **#1 most used orchestration tool** in data engineering worldwide.

**When to use Airflow vs alternatives:**
| Use Airflow When | Don't Use Airflow When |
|---|---|
| Scheduling batch data pipelines | Real-time/sub-second streaming (use Kafka/Flink) |
| Orchestrating multi-step ETL/ELT | Simple single-script cron jobs |
| Managing dependencies between tasks | Extremely low-latency requirements |
| Need monitoring, retries, alerting | Event-driven microservice orchestration (use Step Functions/Temporal) |

---

### 1.2 Core Architecture 🟢

Understanding this is **critical** — it is the #1 most asked Airflow interview question.

```
┌─────────────────────────────────────────────────────────┐
│                    AIRFLOW ARCHITECTURE                  │
│                                                         │
│  ┌──────────┐    ┌───────────┐    ┌──────────────────┐  │
│  │ Webserver │    │ Scheduler │    │    Executor       │  │
│  │ (UI/API)  │    │           │    │                  │  │
│  │           │    │ - Parses  │    │ - Runs tasks     │  │
│  │ - Monitor │    │   DAG dir │    │ - Types:         │  │
│  │ - Trigger │    │ - Creates │    │   Sequential     │  │
│  │ - Logs    │    │   DAG Runs│    │   Local          │  │
│  │ - Graph   │    │ - Queues  │    │   Celery         │  │
│  │   view    │    │   tasks   │    │   Kubernetes     │  │
│  └─────┬─────┘    └─────┬─────┘    └────────┬─────────┘  │
│        │                │                    │            │
│        └────────────────┼────────────────────┘            │
│                         │                                 │
│                   ┌─────▼──────┐                          │
│                   │  Metadata  │                          │
│                   │  Database  │                          │
│                   │ (Postgres) │                          │
│                   └────────────┘                          │
└─────────────────────────────────────────────────────────┘
```

**Components explained:**

| Component | What It Does | Interview Tip |
|---|---|---|
| **Scheduler** | Parses DAG files, determines which tasks to run, sends them to the executor | "The scheduler is the brain of Airflow" |
| **Webserver** | Flask-based UI for monitoring DAGs, viewing logs, triggering runs | Airflow 3.0 uses React + FastAPI instead |
| **Executor** | Determines HOW and WHERE tasks actually run | Most important architectural decision |
| **Metadata DB** | PostgreSQL/MySQL database storing DAG state, task status, XComs, connections | Never use SQLite in production |
| **Worker** | The process that actually executes the task code | Only in Celery/Kubernetes executors |
| **DAG Directory** | Folder where your DAG Python files live (default: ~/airflow/dags/) | Scheduler scans this periodically |

**Must-know interview point:**
> "The scheduler continuously loops: it parses DAG files → checks schedule → creates DagRun → creates TaskInstances → sends to executor → executor runs on worker → updates metadata DB → webserver reads DB to show status."

---

### 1.3 Key Terminology 🟢

Master these terms — they appear in every interview and conversation:

| Term | Definition | Example |
|---|---|---|
| **DAG** | Directed Acyclic Graph — a collection of tasks with defined dependencies. No cycles allowed. | `etl_pipeline_dag` |
| **Task** | A single unit of work defined by an operator | `extract_from_api` |
| **Task Instance** | A specific run of a task at a specific time | `extract_from_api` running at 2026-04-13 |
| **DAG Run** | A single execution of the entire DAG | Pipeline running for 2026-04-13 |
| **Operator** | A class/template that defines what a task does | `PythonOperator`, `BashOperator` |
| **Sensor** | A special operator that WAITS for a condition | `S3KeySensor` waits for a file in S3 |
| **Hook** | Interface to external systems (DB, cloud, API) | `PostgresHook`, `S3Hook` |
| **Connection** | Stored credentials for external systems | DB connection string stored securely |
| **Variable** | Key-value configuration stored in metadata DB | `env=production` |
| **XCom** | Cross-communication — mechanism to pass data between tasks | Task A passes file path to Task B |
| **Pool** | Limits concurrent task execution (e.g., max 5 DB queries at once) | `database_pool` with 5 slots |
| **Queue** | Named queue for routing tasks to specific workers | `high_priority_queue` |
| **Trigger Rule** | Defines when a task runs based on upstream task status | `all_success`, `one_failed`, `none_failed` |
| **logical_date** | The date the DAG run is "logically" for (replaces `execution_date` in Airflow 3) | Running on Apr 14 for Apr 13's data |
| **Backfill** | Running a DAG for historical dates | Re-running pipeline for past 30 days |
| **Catchup** | Whether Airflow should create runs for missed schedules | `catchup=True` or `False` |

**Critical concept — execution_date / logical_date:**
> This confuses 90% of beginners. If your DAG is scheduled `@daily` and runs at midnight:
> - On April 14 at 00:00, the `logical_date` is **April 13** (it processes YESTERDAY's data)
> - This is because the DAG runs **at the END of the interval**, not the beginning
> - Airflow 3.0 renamed `execution_date` → `logical_date` to reduce confusion

---

### 1.4 Installation & Local Setup 🟢

**Option A: pip install (quickest for learning)**
```bash
# Create virtual environment
python -m venv airflow_venv
source airflow_venv/bin/activate   # Linux/Mac
# airflow_venv\Scripts\activate    # Windows

# Install Airflow
pip install apache-airflow==2.10.4  # or latest stable

# Initialize database
airflow db init

# Create admin user
airflow users create \
    --username admin \
    --password admin \
    --firstname Naman \
    --lastname Jain \
    --role Admin \
    --email namanjn619@gmail.com

# Start services (in separate terminals)
airflow webserver --port 8080
airflow scheduler
```

**Option B: Docker Compose (recommended — production-like)**
```bash
# Download official docker-compose
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.10.4/docker-compose.yaml'

# Create required directories
mkdir -p ./dags ./logs ./plugins ./config
echo -e "AIRFLOW_UID=$(id -u)" > .env

# Start everything
docker compose up -d

# Access UI at http://localhost:8080 (admin/airflow)
```

**Special Point:** Always use Docker Compose for practice — it mirrors real production setups with separate containers for webserver, scheduler, worker, Redis, and PostgreSQL. This is exactly what companies run.

---

## PHASE 2: DAG DEVELOPMENT (Week 2-4)
> Goal: Write DAGs confidently, understand operators, dependencies, and scheduling.

---

### 2.1 Writing Your First DAG 🟢

Every DAG file needs these components:

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# 1. Default arguments (applied to all tasks in this DAG)
default_args = {
    'owner': 'naman',
    'depends_on_past': False,
    'email': ['namanjn619@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=30),
}

# 2. DAG definition
with DAG(
    dag_id='my_first_dag',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule='@daily',                    # Cron or preset
    start_date=datetime(2026, 1, 1),
    end_date=None,                        # Optional
    catchup=False,                        # Don't backfill missed runs
    max_active_runs=1,                    # Only 1 DAG run at a time
    tags=['tutorial', 'etl'],
) as dag:

    # 3. Task definitions
    def extract_data(**kwargs):
        print("Extracting data...")
        return {'row_count': 1000}

    def transform_data(**kwargs):
        ti = kwargs['ti']
        data = ti.xcom_pull(task_ids='extract')
        print(f"Transforming {data['row_count']} rows...")

    extract = PythonOperator(
        task_id='extract',
        python_callable=extract_data,
    )

    transform = PythonOperator(
        task_id='transform',
        python_callable=transform_data,
    )

    load = BashOperator(
        task_id='load',
        bash_command='echo "Loading data into warehouse..."',
    )

    # 4. Dependencies
    extract >> transform >> load
```

---

### 2.2 DAG Parameters Deep Dive 🟢

| Parameter | What It Does | Recommended Value |
|---|---|---|
| `dag_id` | Unique identifier for the DAG | Use snake_case, descriptive names |
| `schedule` | When to run (cron expression or preset) | `'@daily'`, `'0 2 * * *'`, `None` |
| `start_date` | When the DAG starts being eligible for scheduling | Fixed past date, NEVER `datetime.now()` |
| `end_date` | When the DAG stops being scheduled | Usually `None` |
| `catchup` | Create DAG runs for all missed intervals since start_date | `False` in most cases |
| `max_active_runs` | Max concurrent DAG runs | `1` for most ETL pipelines |
| `max_active_tasks` | Max concurrent tasks within a single DAG run | Depends on resource limits |
| `dagrun_timeout` | Max time for entire DAG run before it's marked failed | Set based on SLA |
| `tags` | Labels for filtering in UI | `['etl', 'production', 'team_name']` |
| `default_args` | Arguments applied to ALL tasks (retries, owner, etc.) | Always define retries |
| `doc_md` | Markdown documentation shown in Airflow UI | Always document your DAGs |

**Must-know — Cron Expressions:**
```
┌───────────── minute (0-59)
│ ┌───────────── hour (0-23)
│ │ ┌───────────── day of month (1-31)
│ │ │ ┌───────────── month (1-12)
│ │ │ │ ┌───────────── day of week (0-6, Sun=0)
│ │ │ │ │
* * * * *

Examples:
  '0 2 * * *'     → Every day at 2:00 AM
  '0 */6 * * *'   → Every 6 hours
  '30 9 * * 1-5'  → Mon-Fri at 9:30 AM
  '0 0 1 * *'     → First day of every month at midnight

Presets:
  @once      → Run once only
  @hourly    → 0 * * * *
  @daily     → 0 0 * * *
  @weekly    → 0 0 * * 0
  @monthly   → 0 0 1 * *
  @yearly    → 0 0 1 1 *
  None       → Only triggered manually or by external trigger
```

---

### 2.3 Operators — The Building Blocks 🟢

#### Core Operators (Must Know)

| Operator | Use Case | Example |
|---|---|---|
| `PythonOperator` | Run any Python function | Data transformation, API calls |
| `BashOperator` | Run bash/shell commands | Shell scripts, CLI tools |
| `EmptyOperator` | Placeholder / no-op task | DAG flow control, branching joins |
| `BranchPythonOperator` | Conditionally choose which downstream path to execute | If weekday → path A, else → path B |
| `ShortCircuitOperator` | Skip all downstream tasks if condition is False | Skip pipeline if no new data |
| `TriggerDagRunOperator` | Trigger another DAG from within a DAG | Master DAG triggers child DAGs |

#### Database Operators 🟢

| Operator | Use Case |
|---|---|
| `PostgresOperator` | Run SQL on PostgreSQL |
| `MySqlOperator` | Run SQL on MySQL |
| `MsSqlOperator` | Run SQL on SQL Server |
| `SqlSensor` | Wait until SQL query returns results |

#### Cloud Operators 🟡

| Operator | Use Case |
|---|---|
| `S3CreateObjectOperator` | Write data to S3 |
| `S3ToRedshiftOperator` | Copy S3 data into Redshift |
| `GCSToBigQueryOperator` | Load GCS data into BigQuery |
| `SnowflakeOperator` | Run queries on Snowflake |
| `EmrAddStepsOperator` | Submit Spark jobs to AWS EMR |
| `DatabricksSubmitRunOperator` | Submit jobs to Databricks |

#### Transfer Operators 🟡

| Operator | Use Case |
|---|---|
| `S3ToGCSOperator` | Move data from AWS S3 to Google Cloud Storage |
| `MySqlToGCSOperator` | Export MySQL table to GCS |
| `PostgresToGCSOperator` | Export Postgres table to GCS |

**Special Point:** You don't need to memorize all operators. Know the core ones deeply, and know that **provider packages** exist for virtually every service (AWS, GCP, Azure, Snowflake, Databricks, Slack, etc.). The skill is knowing WHICH operator to look for, not memorizing APIs.

---

### 2.4 Sensors 🟢

Sensors are special operators that **wait/poll** until a condition is met.

| Sensor | What It Waits For |
|---|---|
| `FileSensor` | A file to appear on the filesystem |
| `S3KeySensor` | A file/key to appear in S3 bucket |
| `ExternalTaskSensor` | A task in ANOTHER DAG to complete |
| `SqlSensor` | A SQL query to return rows |
| `HttpSensor` | An HTTP endpoint to return a specific response |
| `DateTimeSensor` | A specific datetime to be reached |

**Sensor Modes:**
```
mode='poke'    → Sensor occupies a worker slot continuously, checks every poke_interval
                 Use for: Short waits (< 5 minutes)
                 
mode='reschedule' → Sensor releases the worker slot between checks, reschedules itself
                    Use for: Long waits (> 5 minutes) — saves resources
```

**Must-know interview point:**
> "Always use `mode='reschedule'` for sensors that might wait a long time. In `poke` mode, the sensor holds a worker slot the entire time, which can exhaust your worker pool and create a deadlock where sensors hold all slots and actual tasks can't run."

---

### 2.5 Task Dependencies 🟢

```python
# Method 1: Bitshift operators (most common, most readable)
task_a >> task_b >> task_c          # A → B → C (linear)
task_a >> [task_b, task_c]          # A → B and A → C (fan-out)
[task_a, task_b] >> task_c          # A and B → C (fan-in)

# Method 2: set_downstream / set_upstream
task_a.set_downstream(task_b)
task_c.set_upstream(task_b)

# Method 3: chain() for complex dependencies
from airflow.models.baseoperator import chain
chain(task_a, [task_b, task_c], task_d)
# A → B,C → D (parallel middle, sequential start/end)

# Method 4: cross_downstream() for cross-product dependencies
from airflow.models.baseoperator import cross_downstream
cross_downstream([task_a, task_b], [task_c, task_d])
# A → C, A → D, B → C, B → D
```

---

### 2.6 Trigger Rules 🟡

By default, a task only runs when ALL upstream tasks succeed. Trigger rules change this:

| Trigger Rule | When Task Runs |
|---|---|
| `all_success` (default) | All upstream tasks succeeded |
| `all_failed` | All upstream tasks failed |
| `all_done` | All upstream tasks completed (success, fail, or skip) |
| `one_success` | At least one upstream task succeeded |
| `one_failed` | At least one upstream task failed |
| `none_failed` | No upstream task has failed (but some may be skipped) |
| `none_skipped` | No upstream task was skipped |
| `always` | Task always runs regardless of upstream status |

**Real-world use case:**
```python
# Run cleanup task regardless of whether pipeline succeeded or failed
cleanup = PythonOperator(
    task_id='cleanup_temp_files',
    python_callable=cleanup,
    trigger_rule='all_done',   # Run even if upstream failed
)

# Send alert only if something failed
alert = PythonOperator(
    task_id='send_failure_alert',
    python_callable=send_slack_alert,
    trigger_rule='one_failed',
)
```

---

### 2.7 XCom (Cross-Communication) 🟢

**What:** Mechanism for tasks to share small pieces of data.

```python
# PUSHING data to XCom
def extract(**kwargs):
    data = {'file_path': 's3://bucket/data.parquet', 'row_count': 50000}
    kwargs['ti'].xcom_push(key='extract_result', value=data)
    # OR simply return — return value is auto-pushed with key 'return_value'
    return data

# PULLING data from XCom
def transform(**kwargs):
    ti = kwargs['ti']
    result = ti.xcom_pull(task_ids='extract', key='extract_result')
    # OR if the task returned data:
    result = ti.xcom_pull(task_ids='extract')  # pulls 'return_value' key
    print(f"Processing {result['row_count']} rows from {result['file_path']}")
```

**Critical rules:**
1. XCom data is stored in the **metadata database** — only pass SMALL data (paths, counts, status flags)
2. **NEVER** pass DataFrames, large datasets, or binary data through XCom
3. For large data, write to S3/GCS and pass the **path** via XCom
4. XCom has a size limit (depends on DB, typically ~48KB for MySQL, ~1GB for PostgreSQL)
5. In Airflow 3.0, you can use **Object Storage XCom backend** for larger payloads

**Anti-pattern vs correct pattern:**
```
WRONG:  Task A reads 1M rows → pushes to XCom → Task B pulls 1M rows
RIGHT:  Task A reads 1M rows → writes to S3 → pushes S3 path to XCom → Task B reads from S3
```

---

## PHASE 3: INTERMEDIATE CONCEPTS (Week 4-6)
> Goal: Write production-quality DAGs with proper patterns and error handling.

---

### 3.1 TaskFlow API (Decorator-based DAGs) 🟢

The modern, Pythonic way to write DAGs. This is the **recommended approach** from Airflow 2.0+.

```python
from airflow.decorators import dag, task
from datetime import datetime

@dag(
    dag_id='taskflow_etl',
    schedule='@daily',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['etl', 'taskflow'],
)
def taskflow_etl():

    @task()
    def extract():
        """Extract data from source"""
        return {'data': [1, 2, 3, 4, 5], 'source': 'api'}

    @task()
    def transform(raw_data: dict):
        """Transform extracted data"""
        transformed = [x * 2 for x in raw_data['data']]
        return {'transformed': transformed, 'count': len(transformed)}

    @task()
    def load(processed_data: dict):
        """Load data to warehouse"""
        print(f"Loading {processed_data['count']} records...")

    # Dependencies are AUTOMATIC based on function calls
    raw = extract()
    processed = transform(raw)
    load(processed)

# Instantiate the DAG
taskflow_etl()
```

**Why TaskFlow is better:**
- No manual XCom push/pull — data flows naturally through function arguments
- Dependencies are inferred from function calls — no `>>` needed
- Cleaner, more readable code
- Less boilerplate than traditional `PythonOperator`

**Special interview point:**
> "TaskFlow API uses XCom under the hood, so the same size limitations apply. The syntactic sugar doesn't change the underlying mechanism."

---

### 3.2 Hooks and Connections 🟢

**Hooks** are interfaces to external systems. **Connections** store the credentials.

```python
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

def extract_from_postgres(**kwargs):
    # Hook uses Connection ID configured in Airflow UI/env vars
    hook = PostgresHook(postgres_conn_id='my_postgres_db')
    
    # Run query and get results
    df = hook.get_pandas_df(sql="SELECT * FROM users WHERE created_at > %s",
                            parameters=['2026-01-01'])
    
    # Write to S3
    s3_hook = S3Hook(aws_conn_id='my_aws')
    s3_hook.load_string(
        string_data=df.to_csv(index=False),
        key='raw/users/2026-04-13.csv',
        bucket_name='my-data-lake',
        replace=True,
    )
```

**Setting up Connections:**
```
Via UI:       Admin → Connections → Add
Via CLI:      airflow connections add 'my_postgres_db' --conn-uri 'postgresql://user:pass@host:5432/db'
Via Env Var:  AIRFLOW_CONN_MY_POSTGRES_DB='postgresql://user:pass@host:5432/db'
```

**Must-know:** In production, NEVER store credentials in DAG code. Always use Connections (stored encrypted in metadata DB) or external secrets backends (AWS Secrets Manager, HashiCorp Vault).

---

### 3.3 Variables 🟡

Key-value pairs stored in metadata DB for configuration.

```python
from airflow.models import Variable

# Set variable
Variable.set('environment', 'production')
Variable.set('config', {'db_name': 'analytics', 'batch_size': 1000}, serialize_json=True)

# Get variable
env = Variable.get('environment')                                    # Returns 'production'
config = Variable.get('config', deserialize_json=True)               # Returns dict
batch_size = Variable.get('batch_size', default_var=500)             # With default
```

**Performance Warning:**
> Every `Variable.get()` call queries the metadata database. If you call it at the **top level** of your DAG file (outside task functions), it runs every time the scheduler parses the DAG (every 30 seconds by default). This can overload your DB.

```python
# WRONG — DB query every scheduler parse cycle
my_var = Variable.get('my_config')  # Top level = BAD

# RIGHT — DB query only when task actually executes
@task()
def my_task():
    my_var = Variable.get('my_config')  # Inside task = GOOD
```

**Alternative for top-level access:** Use Jinja templating:
```python
BashOperator(
    task_id='use_var',
    bash_command='echo {{ var.value.environment }}',  # Resolved at runtime, not parse time
)
```

---

### 3.4 Branching 🟡

Execute different paths based on conditions:

```python
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator

def choose_branch(**kwargs):
    day = kwargs['logical_date'].weekday()
    if day < 5:  # Monday-Friday
        return 'process_weekday_data'
    else:
        return 'process_weekend_data'

branch = BranchPythonOperator(
    task_id='branch_on_day',
    python_callable=choose_branch,
)

weekday = EmptyOperator(task_id='process_weekday_data')
weekend = EmptyOperator(task_id='process_weekend_data')

# Join point must use trigger_rule to avoid being skipped
join = EmptyOperator(task_id='join', trigger_rule='none_failed_min_one_success')

branch >> [weekday, weekend] >> join
```

**Interview point:** "When using branching, downstream tasks on non-chosen branches are marked as `skipped`. The join task must use `trigger_rule='none_failed_min_one_success'` or `'none_failed'` — otherwise it will also be skipped because not ALL upstream tasks succeeded."

---

### 3.5 Jinja Templating 🟢

Airflow uses Jinja2 templates to inject runtime values into task parameters:

```python
# These template variables are available in templated fields
BashOperator(
    task_id='templated_task',
    bash_command="""
        echo "Logical date: {{ ds }}"
        echo "Previous date: {{ prev_ds }}"
        echo "Next date: {{ next_ds }}"
        echo "DAG ID: {{ dag.dag_id }}"
        echo "Task ID: {{ task.task_id }}"
        echo "Run ID: {{ run_id }}"
        echo "Variable: {{ var.value.my_variable }}"
        echo "Connection: {{ conn.my_db.host }}"
    """,
)

# Use in SQL
PostgresOperator(
    task_id='load_daily',
    sql="""
        INSERT INTO daily_summary
        SELECT * FROM raw_events
        WHERE event_date = '{{ ds }}'
    """,
)
```

**Key template variables:**
| Variable | Value | Example |
|---|---|---|
| `{{ ds }}` | logical_date as YYYY-MM-DD string | `2026-04-13` |
| `{{ ds_nodash }}` | logical_date as YYYYMMDD | `20260413` |
| `{{ ts }}` | logical_date as ISO timestamp | `2026-04-13T00:00:00+00:00` |
| `{{ prev_ds }}` | Previous schedule's logical_date | `2026-04-12` |
| `{{ next_ds }}` | Next schedule's logical_date | `2026-04-14` |
| `{{ dag_run.conf }}` | Conf dict passed when triggering DAG | For parameterized runs |
| `{{ params.key }}` | User-defined params | Custom values |
| `{{ macros.ds_add(ds, 7) }}` | Date arithmetic | `2026-04-20` |

**Special point:** Not all operator fields are templated. Check the operator's `template_fields` attribute to know which parameters support Jinja. For `PythonOperator`, the function arguments are NOT templated — use `op_kwargs` with templated strings, or access via `kwargs['ds']` inside the function.

---

### 3.6 Error Handling & Retries 🟢

```python
default_args = {
    'retries': 3,                                  # Retry 3 times before failing
    'retry_delay': timedelta(minutes=5),           # Wait 5 min between retries
    'retry_exponential_backoff': True,             # 5 min, 10 min, 20 min...
    'max_retry_delay': timedelta(minutes=60),      # Cap at 60 min
    'email_on_failure': True,                      # Email on final failure
    'email_on_retry': False,                       # Don't spam on retries
    'execution_timeout': timedelta(hours=2),       # Kill task if > 2 hours
}

# Task-level callbacks
def on_failure(context):
    """Called when task fails after all retries exhausted"""
    dag_id = context['dag'].dag_id
    task_id = context['task_instance'].task_id
    error = context['exception']
    # Send Slack/PagerDuty alert
    send_alert(f"FAILED: {dag_id}.{task_id} — {error}")

def on_success(context):
    """Called when task succeeds"""
    pass

def on_retry(context):
    """Called on each retry attempt"""
    attempt = context['task_instance'].try_number
    send_alert(f"RETRYING (attempt {attempt}): {context['task_instance'].task_id}")

task = PythonOperator(
    task_id='critical_task',
    python_callable=my_function,
    on_failure_callback=on_failure,
    on_success_callback=on_success,
    on_retry_callback=on_retry,
    sla=timedelta(hours=1),         # Alert if task takes > 1 hour
)
```

**DAG-level callbacks:**
```python
with DAG(
    dag_id='my_dag',
    on_failure_callback=dag_level_failure_handler,
    on_success_callback=dag_level_success_handler,
    ...
) as dag:
```

---

### 3.7 Idempotency — The Golden Rule 🟢

**An idempotent task produces the same result whether you run it once or 100 times.**

This is arguably the **single most important concept** in production data engineering.

```python
# NON-IDEMPOTENT (BAD) — running twice inserts duplicate rows
def load_data():
    hook.run("INSERT INTO daily_stats VALUES (100, '2026-04-13')")

# IDEMPOTENT (GOOD) — running twice produces same result
def load_data():
    hook.run("""
        DELETE FROM daily_stats WHERE stat_date = '2026-04-13';
        INSERT INTO daily_stats VALUES (100, '2026-04-13');
    """)

# EVEN BETTER — using MERGE/UPSERT
def load_data():
    hook.run("""
        MERGE INTO daily_stats AS target
        USING staging AS source
        ON target.stat_date = source.stat_date
        WHEN MATCHED THEN UPDATE SET value = source.value
        WHEN NOT MATCHED THEN INSERT VALUES (source.value, source.stat_date)
    """)
```

**Rules for idempotent tasks:**
1. Use `INSERT ... ON CONFLICT UPDATE` or `MERGE` instead of plain `INSERT`
2. Write to **date-partitioned** paths: `s3://bucket/data/dt=2026-04-13/` then overwrite
3. Never use `datetime.now()` — always use `{{ ds }}` (logical_date)
4. Tasks should be safe to re-run without manual cleanup

---

## PHASE 4: PRODUCTION PATTERNS (Week 6-8)
> Goal: Write DAGs the way companies actually run them.

---

### 4.1 Executors Deep Dive 🟢

| Executor | How It Works | When To Use | Limitations |
|---|---|---|---|
| **SequentialExecutor** | Runs one task at a time, in-process | Local testing ONLY | No parallelism |
| **LocalExecutor** | Spawns local processes for each task | Single-machine, small workloads | Limited by one machine's resources |
| **CeleryExecutor** | Sends tasks to a Celery worker cluster via message broker (Redis/RabbitMQ) | Medium-large production setups | Requires broker infra, homogeneous workers |
| **KubernetesExecutor** | Launches a new K8s Pod for each task | Large production, multi-tenant | Pod startup overhead (~30s), requires K8s cluster |
| **CeleryKubernetesExecutor** | Hybrid: default tasks on Celery, specific tasks on K8s | Best of both worlds | Complex setup |

**Interview question: "Which executor would you choose and why?"**
> "For a startup or small team, LocalExecutor with PostgreSQL is sufficient. For a mid-size company needing horizontal scaling, CeleryExecutor. For enterprise workloads needing per-task isolation, resource customization, and auto-scaling, KubernetesExecutor. Most MAANG-tier companies use KubernetesExecutor or custom executors."

---

### 4.2 Dynamic DAGs 🟡

**Method 1: Dynamic Task Mapping (Recommended — Airflow 2.3+)**
```python
@task
def get_files():
    """Returns a list — Airflow creates one task instance per element"""
    return ['file1.csv', 'file2.csv', 'file3.csv']

@task
def process_file(filename: str):
    print(f"Processing {filename}")

# .expand() creates dynamic parallel tasks at runtime
files = get_files()
process_file.expand(filename=files)
# This creates: process_file[0], process_file[1], process_file[2]
```

**Method 2: Generating multiple DAGs from config**
```python
# Useful when you have 50 tables to migrate with same pipeline structure
import yaml

config = yaml.safe_load(open('/opt/airflow/config/tables.yaml'))

for table in config['tables']:
    dag_id = f"migrate_{table['name']}"
    
    with DAG(dag_id=dag_id, schedule='@daily', ...) as dag:
        extract = PythonOperator(task_id='extract', ...)
        load = PythonOperator(task_id='load', ...)
        extract >> load
    
    globals()[dag_id] = dag  # Register DAG in global namespace
```

**Special point:** Dynamic task mapping is the modern, supported way. Avoid generating hundreds of DAG files from loops — it slows down the scheduler and makes debugging painful.

---

### 4.3 Pools and Priority 🟡

**Pools** limit concurrent tasks to prevent overwhelming external systems:

```python
# Create pool: Admin → Pools → Add Pool
# Name: snowflake_pool, Slots: 5

task = PythonOperator(
    task_id='query_snowflake',
    python_callable=run_query,
    pool='snowflake_pool',           # Max 5 concurrent Snowflake queries
    priority_weight=10,              # Higher = runs first when pool is congested
    weight_rule='downstream',        # Priority includes downstream task count
)
```

**Use case:** Your Snowflake account has 5 concurrent query limit. Create a pool with 5 slots — Airflow will queue additional tasks until a slot frees up.

---

### 4.4 DAG Design Patterns for Production 🟢

**Pattern 1: ETL with Validation**
```
extract → validate_source → transform → validate_transform → load → validate_load → notify
```

**Pattern 2: Fan-out / Fan-in**
```
              ┌→ process_table_A ─┐
start_task →  ├→ process_table_B ─┼→ merge_results → notify
              └→ process_table_C ─┘
```

**Pattern 3: Master-Child DAGs**
```
master_dag (scheduled daily):
    check_data_ready → trigger_child_dag_1
                     → trigger_child_dag_2
                     → wait_for_children → send_report
```

**Pattern 4: Conditional Processing**
```
check_if_new_data → (branch) → process_incremental
                             → skip_and_notify
```

**Anti-patterns to AVOID:**
```
BAD: One massive DAG with 200 tasks (hard to debug, slow to schedule)
BAD: Tasks that take 4+ hours (break into smaller tasks)
BAD: Tasks with side effects that aren't idempotent
BAD: Hard-coded credentials in DAG files
BAD: Using SubDAGs (deprecated — use TaskGroups instead)
```

---

### 4.5 TaskGroups (Replaced SubDAGs) 🟡

Visually group tasks in the UI without creating separate DAGs:

```python
from airflow.utils.task_group import TaskGroup

with DAG('grouped_dag', ...) as dag:

    with TaskGroup('extract_group') as extract:
        extract_users = PythonOperator(task_id='extract_users', ...)
        extract_orders = PythonOperator(task_id='extract_orders', ...)
        extract_products = PythonOperator(task_id='extract_products', ...)

    with TaskGroup('transform_group') as transform:
        clean_data = PythonOperator(task_id='clean_data', ...)
        join_tables = PythonOperator(task_id='join_tables', ...)

    load = PythonOperator(task_id='load_to_warehouse', ...)

    extract >> transform >> load
```

**Special point:** SubDAGs were deprecated in Airflow 2.0 and removed in Airflow 3.0. If an interviewer asks about SubDAGs, mention they should be replaced with TaskGroups.

---

### 4.6 Testing DAGs 🟡

```python
# tests/test_dags.py
import pytest
from airflow.models import DagBag

@pytest.fixture
def dagbag():
    return DagBag(dag_folder='dags/', include_examples=False)

def test_no_import_errors(dagbag):
    """Ensure all DAGs have no import errors"""
    assert len(dagbag.import_errors) == 0, f"Import errors: {dagbag.import_errors}"

def test_dag_loaded(dagbag):
    """Ensure specific DAG is loaded"""
    assert 'my_etl_dag' in dagbag.dags

def test_task_count(dagbag):
    """Ensure DAG has expected number of tasks"""
    dag = dagbag.get_dag('my_etl_dag')
    assert len(dag.tasks) == 5

def test_dependencies(dagbag):
    """Ensure task dependencies are correct"""
    dag = dagbag.get_dag('my_etl_dag')
    extract = dag.get_task('extract')
    transform = dag.get_task('transform')
    assert transform.task_id in [t.task_id for t in extract.downstream_list]

# Test individual task logic (unit test)
def test_transform_function():
    """Test the Python function used by PythonOperator"""
    from dags.my_etl import transform_data
    result = transform_data(raw_data={'values': [1, 2, 3]})
    assert result == {'values': [2, 4, 6]}
```

---

### 4.7 Logging and Monitoring 🟡

```python
import logging

logger = logging.getLogger(__name__)

@task()
def extract_data():
    logger.info("Starting extraction...")
    logger.info(f"Extracted {row_count} rows from {source}")
    logger.warning("Some records had null values — handled with defaults")
    logger.error("Connection timeout — will retry")  # Visible in task logs UI
```

**Where to find logs:**
- **Airflow UI:** Click on task instance → Log tab
- **File system:** `$AIRFLOW_HOME/logs/{dag_id}/{task_id}/{execution_date}/`
- **Remote logging (production):** Configure to send logs to S3, GCS, or Elasticsearch

**Production monitoring setup:**
```
# In airflow.cfg
[metrics]
statsd_on = True
statsd_host = localhost
statsd_port = 8125

# Airflow emits metrics like:
# - dag_processing.total_parse_time
# - scheduler.tasks.running
# - task_instance_duration
# Connect to Grafana/Prometheus for dashboards
```

---

## PHASE 5: ADVANCED CONCEPTS (Week 8-10)
> Goal: Master advanced features that separate senior from junior engineers.

---

### 5.1 Custom Operators 🟡

When built-in operators don't fit your needs:

```python
from airflow.models import BaseOperator

class DataQualityOperator(BaseOperator):
    """Custom operator to validate data quality after loading"""
    
    template_fields = ('sql', 'table_name')  # Fields that support Jinja templating
    
    def __init__(self, sql, table_name, conn_id, min_rows=1, **kwargs):
        super().__init__(**kwargs)
        self.sql = sql
        self.table_name = table_name
        self.conn_id = conn_id
        self.min_rows = min_rows
    
    def execute(self, context):
        hook = PostgresHook(postgres_conn_id=self.conn_id)
        records = hook.get_records(self.sql)
        
        if len(records) < 1 or records[0][0] < self.min_rows:
            raise ValueError(
                f"Data quality check failed: {self.table_name} "
                f"has {records[0][0]} rows, expected >= {self.min_rows}"
            )
        
        self.log.info(f"Data quality check passed: {self.table_name} has {records[0][0]} rows")
        return records[0][0]

# Usage in DAG
check = DataQualityOperator(
    task_id='check_users_table',
    sql="SELECT COUNT(*) FROM {{ params.schema }}.users WHERE load_date = '{{ ds }}'",
    table_name='users',
    conn_id='my_postgres',
    min_rows=1000,
    params={'schema': 'analytics'},
)
```

---

### 5.2 Custom Hooks 🔵

```python
from airflow.hooks.base import BaseHook
import requests

class CustomAPIHook(BaseHook):
    """Hook for interacting with an internal REST API"""
    
    conn_name_attr = 'api_conn_id'
    default_conn_name = 'custom_api_default'
    conn_type = 'http'
    hook_name = 'Custom API'
    
    def __init__(self, api_conn_id='custom_api_default'):
        super().__init__()
        self.api_conn_id = api_conn_id
        self.connection = self.get_connection(api_conn_id)
        self.base_url = f"https://{self.connection.host}"
        self.token = self.connection.password
    
    def get_data(self, endpoint, params=None):
        response = requests.get(
            f"{self.base_url}/{endpoint}",
            headers={'Authorization': f'Bearer {self.token}'},
            params=params,
        )
        response.raise_for_status()
        return response.json()
```

---

### 5.3 Datasets / Assets (Event-Driven Scheduling) 🟡

Airflow 2.4+ introduced Datasets (renamed to **Assets** in Airflow 3.0) — DAGs can be triggered when upstream DAGs update a dataset.

```python
from airflow.datasets import Dataset  # Airflow 2.x
# from airflow.sdk import Asset      # Airflow 3.x

# Define the dataset
users_dataset = Dataset('s3://my-bucket/processed/users/')

# PRODUCER DAG — declares it updates this dataset
@dag(schedule='@daily', ...)
def produce_user_data():
    @task(outlets=[users_dataset])  # Marks this dataset as updated after task succeeds
    def process_users():
        # ... process and write to s3://my-bucket/processed/users/
        pass

# CONSUMER DAG — triggered when users_dataset is updated
@dag(schedule=[users_dataset], ...)  # Triggered by dataset update, not a time schedule
def consume_user_data():
    @task()
    def read_users():
        # This runs automatically when produce_user_data updates the dataset
        pass
```

**Why this matters:** Moves from time-based scheduling ("run at 2 AM") to event-based scheduling ("run when data is ready"). This is the future of Airflow orchestration.

---

### 5.4 Airflow REST API 🟡

Trigger and monitor DAGs programmatically:

```python
import requests

AIRFLOW_URL = "http://localhost:8080/api/v1"
AUTH = ('admin', 'admin')

# Trigger a DAG run
response = requests.post(
    f"{AIRFLOW_URL}/dags/my_dag/dagRuns",
    json={
        "conf": {"table_name": "users", "date": "2026-04-13"},
        "logical_date": "2026-04-13T00:00:00Z",
    },
    auth=AUTH,
)

# Get DAG run status
response = requests.get(
    f"{AIRFLOW_URL}/dags/my_dag/dagRuns/manual__2026-04-13",
    auth=AUTH,
)
print(response.json()['state'])  # 'success', 'running', 'failed'

# List all DAGs
response = requests.get(f"{AIRFLOW_URL}/dags", auth=AUTH)
```

---

### 5.5 Secrets Backend 🟡

Production systems don't store credentials in Airflow's metadata DB — they use external secrets managers:

```python
# airflow.cfg
[secrets]
backend = airflow.providers.amazon.aws.secrets.secrets_manager.SecretsManagerBackend
backend_kwargs = {"connections_prefix": "airflow/connections", "variables_prefix": "airflow/variables"}
```

Supported backends: AWS Secrets Manager, HashiCorp Vault, GCP Secret Manager, Azure Key Vault.

---

### 5.6 Airflow on Kubernetes 🔵

```yaml
# values.yaml for Helm chart deployment
executor: KubernetesExecutor
images:
  airflow:
    repository: my-registry/airflow
    tag: 2.10.4
    
config:
  core:
    dags_folder: /opt/airflow/dags
    load_examples: 'False'
  kubernetes:
    namespace: airflow
    worker_container_repository: my-registry/airflow
    worker_container_tag: 2.10.4
    delete_worker_pods: 'True'
    
# Install
helm repo add apache-airflow https://airflow.apache.org
helm install airflow apache-airflow/airflow -f values.yaml -n airflow
```

**KubernetesPodOperator — run ANY container as a task:**
```python
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

spark_job = KubernetesPodOperator(
    task_id='run_spark_job',
    namespace='data-processing',
    image='my-spark-image:latest',
    cmds=['spark-submit'],
    arguments=['--master', 'k8s://...', 'my_job.py'],
    resources={'request_memory': '4Gi', 'request_cpu': '2'},
    is_delete_operator_pod=True,
    get_logs=True,
)
```

---

## PHASE 6: AIRFLOW 3.0 SPECIFICS (Week 10-11)
> Goal: Know the latest version — shows you're up-to-date. Bonus impact in interviews.

---

### 6.1 What's New in Airflow 3.0 🔵

Released **April 2025** — the first major release since 2.0 (2020).

| Feature | What Changed | Impact |
|---|---|---|
| **DAG Versioning** | Each DAG run is tied to the DAG version at start time | No more "DAG changed mid-run" bugs |
| **Assets (renamed from Datasets)** | `@asset` decorator, richer event-driven scheduling | Cleaner data-aware pipelines |
| **Task Execution Interface** | New API between scheduler and task execution | Better security, multi-cloud support |
| **React UI** | Complete rewrite from Flask-AppBuilder to React + FastAPI | Faster, modern, version-aware UI |
| **Scheduler-managed Backfills** | Backfills run through scheduler (not CLI) | Better control, visibility, and cancellation |
| **`airflow.sdk`** | Unified import namespace for all core constructs | `from airflow.sdk import dag, task` |
| **Logical date = None** | DAGs can run without a logical date | Supports ML inference, ad-hoc jobs |
| **Removed SubDAGs** | Fully removed (deprecated since 2.0) | Use TaskGroups instead |
| **Removed SLAs** | SLA feature removed | Use callbacks or external monitoring |
| **Edge Executor** | Run tasks on edge/remote devices | IoT, edge computing use cases |

**Import changes:**
```python
# Airflow 2.x
from airflow.decorators import dag, task
from airflow.datasets import Dataset

# Airflow 3.x
from airflow.sdk import dag, task, DAG, Asset
```

---

## PHASE 7: REAL-WORLD PRACTICE PROJECTS (Week 11-12)
> Goal: Build portfolio pieces using Airflow.

---

### 7.1 Practice Project Ideas (Progressive Difficulty)

**Project 1 (Beginner):** API → PostgreSQL Pipeline
```
Airflow DAG (scheduled @daily):
  1. PythonOperator → Call weather API → save JSON to local/S3
  2. PythonOperator → Parse JSON, clean data
  3. PostgresOperator → INSERT into PostgreSQL
  4. PythonOperator → Run data quality check
  5. BashOperator → Send email notification
```

**Project 2 (Intermediate):** Multi-source ELT with dbt
```
Airflow DAG:
  1. Extract from 3 sources (API, CSV, PostgreSQL) [parallel with TaskGroup]
  2. Land raw files in S3 (Bronze)
  3. Trigger dbt run (Silver + Gold transformations)
  4. Run Great Expectations validation
  5. Branching: if quality pass → notify success, else → alert + halt
```

**Project 3 (Advanced):** Dynamic Migration Framework
```
Config-driven Airflow DAG:
  1. Read YAML config listing 10 tables
  2. Dynamic task mapping → create extract/transform/load tasks for each table
  3. Per-table: Extract → S3 → PySpark transform → Snowflake load
  4. Reconciliation check (source vs target row counts)
  5. Generate and email migration report
```

---

## PHASE 8: INTERVIEW PREPARATION (Ongoing)
> Goal: Be ready for any Airflow question in a DE interview.

---

### 8.1 Top 25 Interview Questions (With Answer Frameworks)

**Architecture & Concepts:**
1. What is Apache Airflow and how does it work? (Architecture diagram)
2. What are the components of Airflow? (Scheduler, Webserver, Executor, Metadata DB)
3. What is a DAG? Why "acyclic"? (No circular dependencies)
4. Difference between Operator, Task, and Task Instance?
5. What is `execution_date` / `logical_date`? Why is it confusing? (End of interval, not start)
6. What are Sensors? When to use `poke` vs `reschedule` mode?
7. What is XCom? What are its limitations? (Size, stored in DB)
8. Difference between Variables and Connections?

**Executors & Deployment:**
9. Compare SequentialExecutor, LocalExecutor, CeleryExecutor, KubernetesExecutor.
10. When would you choose KubernetesExecutor over CeleryExecutor?
11. How do you deploy Airflow in production? (Docker/K8s/Managed services)

**DAG Design:**
12. How do you make tasks idempotent? (UPSERT, partitioned writes, avoid datetime.now())
13. What is catchup? When do you set it to False?
14. How do you handle task failures? (Retries, callbacks, alerting)
15. What are Trigger Rules? Give a use case for `all_done`.
16. Explain branching in Airflow. What's the gotcha with the join task?
17. How do you pass data between tasks? (XCom small data, S3 for large)

**Production & Advanced:**
18. How do you test DAGs? (DagBag import test, unit tests, integration tests)
19. What are Pools? Give a real-world use case.
20. Explain TaskGroups vs SubDAGs. Why were SubDAGs deprecated?
21. What are dynamic DAGs? How does dynamic task mapping work?
22. What is the TaskFlow API? How is it different from traditional operators?
23. How do you handle secrets/credentials in Airflow?
24. How do you monitor Airflow in production? (Metrics, logs, alerts)
25. What's new in Airflow 3.0? (DAG versioning, Assets, React UI, SDK imports)

---

### 8.2 Common Scenario-Based Questions 🟢

**Q: "Your DAG has been running fine for 3 months but suddenly tasks are stuck in 'queued' state. How do you debug?"**
```
Answer framework:
1. Check executor — are workers running? (CeleryExecutor: check workers, K8s: check pods)
2. Check pools — is the pool full? Are all slots occupied?
3. Check max_active_runs — is another DAG run already using all slots?
4. Check scheduler — is the scheduler running and healthy?
5. Check metadata DB — is it overloaded or out of connections?
6. Check resource limits — worker memory/CPU exhausted?
```

**Q: "How would you design an Airflow DAG to migrate 100 tables from Oracle to Snowflake?"**
```
Answer framework:
1. Config-driven: YAML file listing all 100 tables with source/target schemas
2. Dynamic task mapping: generate extract-transform-load tasks per table
3. Pools: limit concurrent Snowflake loads to avoid overloading
4. TaskGroups: group by extract/transform/load for visual clarity
5. Idempotency: use MERGE/UPSERT, partition by date
6. Validation: row count reconciliation after each table
7. Alerting: on_failure_callback for Slack/PagerDuty
8. Backfill support: parameterized by logical_date
```

**Q: "Your Airflow pipeline takes 6 hours. How do you optimize it?"**
```
Answer framework:
1. Identify bottleneck: check task durations in Gantt chart view
2. Parallelize: can independent tasks run concurrently?
3. Reduce data scanned: partition pruning, incremental loads
4. Move heavy processing: use Spark/EMR instead of PythonOperator
5. Sensor optimization: switch from poke to reschedule mode
6. Pool tuning: increase pool slots if external system can handle more
7. Executor: upgrade from Local to Celery/Kubernetes for parallelism
```

---

### 8.3 Quick Reference — Things That Catch People Off Guard 🟢

| Gotcha | Explanation |
|---|---|
| `start_date` in the past creates many runs | If `catchup=True` (default), Airflow creates DAG runs for EVERY missed interval since `start_date` |
| `datetime.now()` as `start_date` | NEVER do this — it changes every parse cycle and creates a moving target |
| DAG file runs every 30 seconds | The scheduler imports your DAG file periodically — keep top-level code FAST |
| `depends_on_past=True` deadlock | If first run fails, all future runs are blocked forever |
| XCom in CeleryExecutor | XCom data must be JSON-serializable when using Celery |
| Sensor deadlock | Poke-mode sensors can fill all worker slots, blocking actual tasks |
| Timezone confusion | Airflow uses UTC internally — always be explicit about timezones |
| Task deleted from DAG file | Historical task instances disappear from UI — you lose logs |
| `max_active_runs=1` with backfill | Backfills run one at a time — very slow for large backfills |

---

## SUMMARY: PRIORITY MATRIX

| Phase | Topics | Priority | Time |
|---|---|---|---|
| **Phase 1** | Architecture, Terminology, Setup | 🟢 Essential | Week 1-2 |
| **Phase 2** | DAGs, Operators, Sensors, Dependencies, XCom | 🟢 Essential | Week 2-4 |
| **Phase 3** | TaskFlow, Hooks, Connections, Branching, Jinja, Error Handling, Idempotency | 🟢 Essential | Week 4-6 |
| **Phase 4** | Executors, Dynamic DAGs, Pools, Design Patterns, TaskGroups, Testing, Logging | 🟡 Important | Week 6-8 |
| **Phase 5** | Custom Operators/Hooks, Datasets/Assets, REST API, Secrets, K8s | 🟡/🔵 Important-Bonus | Week 8-10 |
| **Phase 6** | Airflow 3.0 features | 🔵 Bonus | Week 10-11 |
| **Phase 7** | Practice Projects | 🟢 Essential | Week 11-12 |
| **Phase 8** | Interview Prep | 🟢 Essential | Ongoing |

---

## RESOURCES

| Resource | Type | Link/Note |
|---|---|---|
| **Official Docs** | Documentation | airflow.apache.org/docs |
| **Official Tutorial** | Hands-on | airflow.apache.org/docs/apache-airflow/stable/tutorial |
| **Astronomer Guides** | Best-in-class guides | docs.astronomer.io/learn |
| **Marc Lamberti (YouTube)** | Best Airflow instructor | YouTube channel + Udemy courses |
| **DataLemur Airflow** | Interview questions | datalemur.com |
| **Apache Airflow GitHub** | Source code + examples | github.com/apache/airflow |
| **Awesome Apache Airflow** | Curated resource list | github.com/jghoman/awesome-apache-airflow |

---

*Mastering Airflow is not about memorizing operators — it's about understanding WHY orchestration
matters, WHEN to use which pattern, and HOW to make pipelines reliable, idempotent, and observable.
That mindset is what separates a "DAG writer" from a "Data Engineer."*
