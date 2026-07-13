# Migration Design Document
**Job Name:** `d_alis_spaufruf_p0.sql`  
**Prescribed Migration Pattern:** UC4 + KSH + SQL_SIMPLE (Cloud Composer + Dataform + BigQuery)  
**Legacy Tooling:** Oracle SQL*Plus Wrapper / PL/SQL  
**Target Platform:** Google Cloud BigQuery & Cloud Composer (Airflow DAG)  

---

## 1. Executive Summary & Migration Strategy
The legacy file `d_alis_spaufruf_p0.sql` is an Oracle SQL*Plus wrapper script that initializes environment parameters, prepares output environments (`SET SERVEROUTPUT ON`), and executes a dynamic stored procedure passed via positional parameter (`&1`), followed by an explicit `COMMIT`.

In the target BigQuery environment:
* BigQuery does not use SQL*Plus-like session/formatting settings (`SET PAGESIZE`, `SET HEADING`, etc.).
* Explicit Oracle transaction control (`COMMIT`) is handled natively inside BigQuery stored procedures or automatically committed after successful execution of single transactions.
* The wrapper is replaced with a **Cloud Composer (Airflow) DAG** using the `BigQueryExecuteQueryOperator` to dynamically invoke target BigQuery Stored Procedures via `CALL`.
* Common initializations defined in the legacy referenced file `d_alis_init.sql` will either be mapped to global Airflow configurations, dataset-level settings, or shared initialization steps.

---

## 2. Unresolved Components

* **SOURCE: NOT FOUND** — `d_alis_init.sql` — Candidates: `vobs/dw_source/isdwh/allgemein/is/util/sql/d_alis_init.sql` (not provided in current context, must be verified and resolved manually by developer during building).
* **SOURCE: NOT FOUND** — Dynamic Stored Procedure (`&1`) — No explicit SQL file was bundled since the wrapper executes whichever stored procedure is passed at runtime. Individual child stored procedures must be migrated as separate BigQuery Stored Procedures (`CREATE OR REPLACE PROCEDURE`).

---

## 3. Lineage & Cross-File Dependencies

* **Upstream:**
  * Called from external UNIX wrappers/UC4 scheduling systems passing parameters:
    * `&1` (Parameter 1): Stored Procedure Name
    * `&2` (Parameter 2): Stored Procedure Arguments
  * Executed after initializing session parameters via `$DW_DIR_ROOT/allgemein/is/util/sql/d_alis_init.sql`.
* **Downstream:**
  * Runs the corresponding targeted database stored procedure.
  * Commits the transaction if successful, or exits with rollback on error.

---

## 4. Target File Plan

| Target File Path | Language | Source File | Description |
| :--- | :--- | :--- | :--- |
| `dags/d_al_is_spaufruf_p0.py` | Python (Airflow DAG) | `d_alis_spaufruf_p0.sql` | Airflow DAG using Google Cloud Operators to invoke BigQuery procedures. |
| `definitions/d_alis_init.sqlx` (or equivalent) | Dataform / SQL | `d_alis_init.sql` | Shared environment initialization script (if verified as containing DDL/DML). |

---

## 5. Verbatim MCP Tool Output (Transformation Logic)

Below is the generated translation of the Oracle SQL*Plus wrapper script to a modern Python Airflow operator utilizing BigQuery:

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator

# Default arguments for the Airflow DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_ his_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'd_al_is_spaufruf_p0',
    default_args=default_args,
    description='Execute BigQuery stored procedure simulating legacy d_al_is_spaufruf_p0 Oracle script',
    schedule_interval=None,
    catchup=False,
)

def process_and_join_data():
    """
    Python function encapsulating the SQL logic.
    Instead of executing PL/SQL command 'EXEC &1' from sqlplus, 
    we transition to Google BigQuery's CALL statement to invoke the stored procedure.
    """
    # Replace 'your_project_id.your_dataset.your_stored_procedure' with the actual procedure name 
    # and pass the required arguments in the signature.
    stored_procedure_query = """
    -- Calling the stored procedure translated from legacy Oracle EXEC &1;
    -- BigQuery automatically commits transactional statements within procedures.
    CALL `your_project_id.your_dataset.your_stored_procedure_name`(
        -- Add parameters here if required, mimicking the legacy P2 parameter
    );
    """

    # BigQueryExecuteQueryOperator executes the CALL statement.
    # write_disposition and create_disposition are configured but are optional 
    # since CALL statements typically handle target tables internally.
    return BigQueryExecuteQueryOperator(
        task_id='execute_stored_procedure',
        sql=stored_procedure_query,
        use_legacy_sql=False,
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_APPEND',
        dag=dag
    )

# Instantiate the task
execute_sp_task = process_and_join_data()

# Set task dependencies (Single task pipeline)
execute_sp_task
```

---

## 6. Environment-Specific Values & Configurations
To compile and execute this pipeline, the build agent/infrastructure must provide:
* **GCP Project ID**: Replace `your_project_id` with your GCP project identifier.
* **BigQuery Dataset**: Replace `your_dataset` with the destination target schema where migrated stored procedures reside.
* **BigQuery Connection ID**: The default `google_cloud_default` or a custom connection ID configured in Airflow.
* **Variables / Parameters**: In production, the Airflow DAG should be parameterized to dynamically receive the procedure name and arguments via DAG Run Configuration (`dag_run.conf`), mimicking Oracle's `&1` and `&2` behavior.

---

## 7. Risks & Manual Actions

1. **SOURCE: NOT FOUND — d_alis_init.sql — no candidate found in current batch (verify `$DW_DIR_ROOT/allgemein/is/util/sql/d_alis_init.sql` mapping)**  
   * *Manual Action:* Inspect legacy `d_alis_init.sql`. If it only contains Oracle session-level formatting settings, it can be entirely ignored in BigQuery. If it initializes global variables, they must be converted to Airflow variables or dataset parameters.
2. **Dynamic Stored Procedure Argument Resolution (`&1`, `&2`)**  
   * *Manual Action:* Identify all upstream callers (shell scripts, UC4 tasks) of `d_alis_spaufruf_p0.sql` to identify all potential target stored procedures that need to be compiled inside BigQuery. Ensure they are fully migrated.
3. **Transactional Commit/Rollback Control (`WHENEVER OSERROR EXIT FAILURE ROLLBACK`)**  
   * *Manual Action:* Ensure the target BigQuery stored procedures use BigQuery's transactional block structure (`BEGIN...EXCEPTION...ROLLBACK TRANSACTION...END`) if multi-statement transactional atomicity is required.