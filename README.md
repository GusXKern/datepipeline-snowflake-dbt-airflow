#### This followed the code along instructions in this [video by jayzern](https://www.youtube.com/watch?v=OLXkGB7krGo). I learned a ton setting it all up for myself, and am looking forward to using the same setup to try one of my own projects next! 

# Build an ELT Pipeline: dbt, Snowflake, and Airflow (Cosmos)

This repository contains a local end-to-end ELT (Extract, Load, Transform) data pipeline. In modern data engineering, we leverage cloud storage and compute to load raw data first, and then transform it using **dbt (Data Build Tool)** inside **Snowflake**, all orchestrated seamlessly on Windows/WSL using **Apache Airflow and Astronomer Cosmos**.

## Why This Tech Stack? (Tool Summary)

### 1. Snowflake (The Cloud Data Warehouse)
* **Role:** Storage and Compute Engine.
* **Why we use it:** In modern ELT architectures, Snowflake allows us to load raw data first without heavy upfront transformation. It cleanly separates **storage** from **compute**, meaning your data sits securely in the cloud while virtual warehouses spin up massive processing power *only* when executing queries or transformations, scaling down automatically when idle.

### 2. dbt / Data Build Tool (The Transformation Layer)
* **Role:** SQL Modularity and Data Modeling.
* **Why we use it:** Instead of writing messy, unmanaged SQL scripts directly in a database console, dbt brings software engineering best practices to data work. It lets you write modular, reusable SQL queries, automatically handles table dependencies (building views and tables in the correct order), runs automated data quality tests, and generates living documentation.

### 3. Apache Airflow & Astronomer Cosmos (The Orchestrator)
* **Role:** Workflow Automation and Pipeline Management.
* **Why we use it:** Airflow acts as the traffic controller for your data infrastructure, managing scheduled execution (e.g., running daily), handling error retries, and monitoring task health. **Astronomer Cosmos** bridges Airflow and dbt by automatically parsing your dbt project structure and translating every single model into a native, trackable Airflow task without manual boilerplate coding.

---

## Step 1: Setup Snowflake Environment

### What is the point of this step?
Before writing any transformations, you need a secure, scalable place to store and compute your data. This step sets up the core cloud data warehouse infrastructure, including roles for security, virtual warehouses for processing power, and databases for structured storage.

### What is Snowflake used for in an ETL process?
Snowflake acts as the **Cloud Data Warehouse**. In a modern ELT architecture, Snowflake handles both the **Load** and **Transform** phases. It separates storage from compute, allowing data teams to scale resources dynamically when executing heavy data pipelines.

```sql
-- create accounts
use role accountadmin;

create warehouseDBT_DB.DBT_SCHEMA dbt_wh with warehouse_size='x-small';
create database dbt_db;
create role dbt_role;


show grants on warehouse dbt_wh;

grant usage on warehouse dbt_wh to role dbt_role;
grant role dbt_role to user DRAINED9970;
grant all on database dbt_db to role dbt_role;


use role dbt_role;


create schema dbt_db.dbt_schema;
```

Step 2: Configure dbt Project Settings
--------------------------------------

### What is the point of this step?

This file (`dbt_project.yml`) acts as the command center for your dbt project. It tells dbt how to structure your folders, name your project, and determine how different layers of models should be materialized in Snowflake (e.g., as lightweight **views** or heavy storage **tables**).

### What is dbt used for in an ETL process?

dbt handles the **"T" (Transformation)** in ELT. Instead of writing messy, manual SQL scripts, dbt brings software engineering best practices---like modularity, version control, automated documentation, and testing---directly to your SQL queries.

``` yml
models:
  data_pipeline:
    staging:
      +materialized: view
      +snowflake_warehouse: dbt_wh  # Added '+' and fixed the space to an underscore
    marts:
      +materialized: table
      +snowflake_warehouse: dbt_wh  # Added '+' here too
```

Step 3: Create Source and Staging Files
---------------------------------------

### What is the point of this step?

Raw data from production systems is often messy, poorly named, or unstandardized. The **Staging** layer acts as the first line of defense. Here, we point to raw source tables, cast data types, rename columns into a clean convention, and establish baseline uniqueness tests.

### What is the point in an ETL process?

This establishes a clean abstraction boundary between raw external data and your downstream business logic. If a source system changes a column name upstream, you only have to fix it in your staging models rather than rewriting your entire data warehouse.

### Source Configuration (`models/staging/tpch_sources.yml`)

```yml
version: 2

sources:
  - name: tpch
    database: snowflake_sample_data
    schema: tpch_sf1
    tables:
      - name: orders
        columns:
          - name: o_orderkey
            tests:
              - unique
              - not_null
      - name: lineitem
        columns:
          - name: l_orderkey
            tests:
              - relationships:
                  to: source('tpch','orders')
                  field: o_orderkey
```
### Staging Orders (`models/staging/stg_tpch_orders.sql`)
```sql
select 
    o_orderkey as order_key,
    o_custkey as customer_key,
    o_orderstatus as status_code,
    o_totalprice as total_price,
    o_orderdate as order_date
FROM
    {{source('tpch','orders')}}
```

### Staging Line Items (models/staging/stg_tpch_line_items.sql)
```sql
select 
    {{
        dbt_utils.generate_surrogate_key([
            'l_orderkey',
            'l_linenumber'
        ])
    }} as order_item_key,
	l_orderkey as order_key,
	l_partkey as part_key,
	l_linenumber as line_number,
	l_quantity as quantity,
	l_extendedprice as extended_price,
	l_discount as discount_percentage,
	l_tax as tax_rate
FROM
    {{source('tpch','lineitem')}}
```

Step 4: Write Reusable Macros (DRY Principle)
---------------------------------------------

### What is the point of this step?

D.R.Y. stands for **Don't Repeat Yourself**. Instead of copy-pasting complex mathematical calculations across multiple SQL files, dbt lets you write Jinja **macros** (reusable functions) that can be called dynamically across your pipeline.

### What is the point in an ETL process?

Macros ensure business logic consistency. If a discount calculation formula changes tomorrow, you update it in one central macro file, and it automatically propagates across every model that uses it.

```sql
{% macro discounted_amount(extended_price, discount_percentage, scale=2) %}
    (-1 * {{extended_price}} * {{discount_percentage}})::decimal(16, {{ scale }})
{% endmacro %}
```

Step 5: Build Transformation Models (Intermediate & Marts)
----------------------------------------------------------

### What is the point of this step?

This is where raw data is turned into actual business value. **Intermediate models** handle complex multi-table joins, while **Data Marts (Fact and Summary tables)** aggregate metrics ready for business intelligence tools, dashboards, and stakeholders.

### What is the point in an ETL process?

This structures your warehouse into a clean analytical format (like a star schema), making queries fast, intuitive, and aligned with core business metrics.

### Intermediate Order Items (models/marts/int_order_items.sql)
```sql
SELECT 
    line_item.order_item_key,
    line_item.part_key,
    line_item.line_number,
    line_item.extended_price,
    orders.order_key,
    orders.customer_key,
    orders.order_date,
    {{ discounted_amount('line_item.extended_price', 'line_item.discount_percentage') }} as item_discount_amount
FROM
    {{ref('stg_tpch_orders')}} as orders
join
    {{ref('stg_tpch_line_items')}} as line_item
    on orders.order_key = line_item.order_key

order by 
    orders.order_date
```

### Order Items Summary (models/marts/int_order_items_summary.sql)
```sql
select 
    order_key,
    sum(extended_price) as gross_item_sales_amount,
    sum(item_discount_amount) as item_discount_amount
from
    {{ ref('int_order_items') }}
group by
    order_key
```

### Fact Orders (models/marts/fct_orders.sql)
```sql
select
    orders.*,
    order_item_summary.gross_item_sales_amount,
    order_item_summary.item_discount_amount
from
    {{ref('stg_tpch_orders')}} as orders
join
    {{ref('int_order_items_summary')}} as order_item_summary
        on orders.order_key = order_item_summary.order_key
order by order_date
```

Step 6: Add Tests
-----------------

### What is the point of this step?

Data quality is critical. This step introduces **generic tests** (like checking for nulls or uniqueness) and **singular custom tests** (asserting logical business rules, like ensuring dates aren't in the future).

### What is the point in an ETL process?

Automated data testing acts as an early warning system. If upstream data breaks or contains anomalies, tests catch it *before* bad data hits executive dashboards or financial reports.

### Generic Tests (`models/marts/generic_tests.yml`)
``` yml
models:
  - name: fct_orders
    columns:
      - name: order_key
        tests:
          - unique
          - not_null
          - relationships:
              config:
                severity: warn
              arguments:
                to: ref('stg_tpch_orders')
                field: order_key
      - name: status_code
        tests:
          - accepted_values:
              values: ['P','O','F']
```

### Singular Test: Discount Check [Making Sure Value is Positive] (tests/fct_orders_discount.sql)
``` sql
select *
FROM
    {{ref('fct_orders')}}
where
    item_discount_amount > 0
```

### Singular Test: Date Validity (tests/fct_orders_date_valid.sql)
```sql 
SELECT *
FROM
    {{ref('fct_orders')}}
where
    date(order_date) > CURRENT_DATE()
    or date(order_date) < date('1990-01-01')
```

Step 7: Orchestrate with Airflow & Cosmos
-----------------------------------------

### What is the point of this step?

Building transformations is great, but running them manually is inefficient. This final step configures **Apache Airflow** and **Astronomer Cosmos** to automatically parse your dbt project structure and turn every model into an individual, trackable task inside an automated DAG.

### What is the point in an ETL process?

Orchestration manages the **O** in modern data pipelines. It handles scheduling, retries on failure, dependency management (making sure models only run after their parent tables update), and monitoring alerts.

### Dockerfile
``` dockerfile
RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-snowflake && deactivate
```

### Requirements (requirements.txt)
``` plaintext
astronomer-cosmos
apache-airflow-providers-snowflake
```

### Airflow Connection Configuration (snowflake_conn)
```json
{
  "account": "-------",
  "warehouse": "dbt_wh",
  "database": "dbt_db",
  "role": "dbt_role",
  "insecure_mode": false
}
```
### Airflow DAG (dags/dbt_dag.py)
``` python
import os
from datetime import datetime

from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping


profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_conn", 
        profile_args={"database": "dbt_db", "schema": "dbt_schema"},
    )
)

dbt_snowflake_dag = DbtDag(
    project_config=ProjectConfig("/usr/local/airflow/dags/dbt/data_pipeline",),
    operator_args={"install_deps": True},
    profile_config=profile_config,
    execution_config=ExecutionConfig(dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt",),
    schedule="@daily",  # <--- Changed from schedule_interval to schedule
    start_date=datetime(2023, 9, 10),
    catchup=False,
    dag_id="dbt_dag",
)
```
