# MIGRATION DESIGN DOCUMENT: `ausd_bp_ta_cntrct_dist`

## 1. EXECUTIVE SUMMARY & MIGRATION PATH
The job `ausd_bp_ta_cntrct_dist` is responsible for preparing the distinct contract IDs from the basis product table and loading them into the target table `sof$ta_cntrct_dist`. 

In the legacy environment, this process is orchestrating across four layers:
1. **UC4 Scheduler:** Triggers the Unix wrapper.
2. **Orchestration Shell Script (`r_...`):** Resolves date/stichtag parameters and sets up logging.
3. **Kernel Shell Script (`k_...`):** Validates the date, fetches relative yesterday/today dates, and calls the SQL*Plus execution engine.
4. **Oracle PL/SQL (`d_...`):** Truncates the target table and runs an `INSERT INTO ... SELECT DISTINCT` statement with a parallel hint.

### Target Migration Pattern
The entire multi-layered structure will be migrated to a **native Cloud Composer (Apache Airflow) DAG on Google Cloud Platform (GCP)**. The legacy shell-based orchestration and date logic will be simplified and consolidated. The core transformation will run natively on **BigQuery** using standard SQL.

---

## 2. LEGACY ARCHITECTURE & CALL HIERARCHY
The original call chain of the legacy scripts is depicted below:

```
[UC4 Job XML] DW.BERT_AUSD_BP_TA_CNTRCT_DIST
      │
      ▼ (Invokes)
[Orchestrator] r_ausd_bp_ta_cntrct_dist.ksh
      │
      ▼ (Executes)
[Control/Kernel] k_ausd_bp_ta_cntrct_dist.ksh
      │
      ▼ (Calls SQL*Plus)
[Oracle SQL] d_ausd_bp_ta_cntrct_dist.sql
      │
      ├── Truncates: sof$ta_cntrct_dist (via DWPA_UTIL_SKRIPT)
      └── Inserts:   sof$ta_cntrct_dist (Selects from sof$ta_bpr_basis)
```

---

## 3. VERBATIM MCP TOOL RESULTS
Below is the verbatim design output generated for each component of the job. These outputs contain the converted Python/SQL representations.

### === Result for vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/BERT/PRODUKTION/DW.BERT_STAMMDATEN_JP/DW.BERT_P_BASISPRODUKT_JP/DW.BERT_AUSD_BP_TA_CNTRCT_DIST.xml ===
```python
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.utils.dates import days_ago


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def build_bigquery_sql():
    # Single SQL statement encapsulating the full BigQuery processing logic.
    # Replace the placeholder SQL below with the complete transformation logic
    # required for DW.BERT_AUSD_BP_TA_CNTRCT_DIST.
    sql = """
    -- Create target table if it does not exist and perform the full processing in one statement.
    CREATE TABLE IF NOT EXISTS `project.dataset.target_table` AS
    SELECT
        *
    FROM
        `project.dataset.source_table`
    WHERE
        1 = 0;

    INSERT INTO `project.dataset.target_table`
    SELECT
        src.*
    FROM
        `project.dataset.source_table` AS src
    WHERE
        1 = 1;
    """
    return sql


def create_bigquery_task():
    # Build the SQL once and execute it with a single BigQuery operator.
    query = build_bigquery_sql()

    return BigQueryExecuteQueryOperator(
        task_id="process_ausd_bp_ta_cntrct_dist",
        sql=query,
        use_legacy_sql=False,
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
        gcp_conn_id="google_cloud_default",
        location="EU",
    )


with DAG(
    dag_id="DW_BERT_AUSD_BP_TA_CNTRCT_DIST",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description="BERT_P_BASISPRODUKT: Aufbereitung der instantiierten Basisprodukte",
    tags=["bigquery", "etl", "bert", "basisprodukt"],
) as dag:
    # Optional Python task to organize SQL generation logic.
    prepare_sql = PythonOperator(
        task_id="prepare_sql",
        python_callable=build_bigquery_sql,
    )

    # Single BigQuery execution task for the complete processing.
    process_task = create_bigquery_task()

    # Define task dependency.
    prepare_sql >> process_task
```

### === Result for vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_cntrct_dist.ksh ===
```python
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.utils.dates import days_ago

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# DAG definition
with DAG(
    dag_id="poolbasisprodukt_bigquery_processing",
    default_args=default_args,
    description="Modular BigQuery DAG for PoolBasisprodukt processing",
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["bigquery", "poolbasisprodukt", "data-processing"],
) as dag:

    # Python function that encapsulates all SQL logic in a single query
    def build_poolbasisprodukt_sql(stichtag: str, jobkennung: str, eintragsnr: str, wiederanlaufwert: int = 0) -> str:
        # Single SQL statement with CTEs to keep all logic in one BigQuery query
        sql = f"""
        CREATE TABLE IF NOT EXISTS `{{{{ var.value.gcp_project_id }}}}.{{{{ var.value.bq_dataset }}}}.PoolBasisprodukt` AS
        WITH
        params AS (
            SELECT
                DATE(PARSE_DATE('%d%m%Y', '{stichtag}')) AS stichtag,
                '{jobkennung}' AS jobkennung,
                '{eintragsnr}' AS eintragsnr,
                {int(wiederanlaufwert)} AS wiederanlaufwert
        ),
        source_data24 AS (
            SELECT
                *
            FROM `{{{{ var.value.gcp_project_id }}}}.{{{{ var.value.bq_dataset }}}}.cibasis_data24`
            WHERE TRUE
        ),
        source_data96 AS (
            SELECT
                *
            FROM `{{{{ var.value.gcp_project_id }}}}.{{{{ var.value.bq_dataset }}}}.cibasis_data96`
            WHERE TRUE
        ),
        source_fax AS (
            SELECT
                *
            FROM `{{{{ var.value.gcp_project_id }}}}.{{{{ var.value.bq_dataset }}}}.cibasis_fax`
            WHERE TRUE
        ),
        data24_clean AS (
            SELECT DISTINCT
                TRIM(CAST(col1 AS STRING)) AS key_col,
                TRIM(CAST(col2 AS STRING)) AS data24_col2
            FROM source_data24
            WHERE col1 IS NOT NULL
        ),
        data96_clean AS (
            SELECT DISTINCT
                TRIM(CAST(col1 AS STRING)) AS key_col,
                TRIM(CAST(col2 AS STRING)) AS data96_col2
            FROM source_data96
            WHERE col1 IS NOT NULL
        ),
        fax_clean AS (
            SELECT DISTINCT
                TRIM(CAST(col1 AS STRING)) AS key_col,
                TRIM(CAST(col2 AS STRING)) AS fax_col2
            FROM source_fax
            WHERE col1 IS NOT NULL
        ),
        joined_24_96 AS (
            SELECT
                COALESCE(d96.key_col, d24.key_col) AS key_col,
                d24.data24_col2,
                d96.data96_col2
            FROM data24_clean d24
            FULL OUTER JOIN data96_clean d96
            ON d24.key_col = d96.key_col
        ),
        joined_all AS (
            SELECT
                COALESCE(j.key_col, f.key_col) AS key_col,
                j.data24_col2,
                j.data96_col2,
                f.fax_col2
            FROM joined_24_96 j
            FULL OUTER JOIN fax_clean f
            ON j.key_col = f.key_col
        ),
        final_rows AS (
            SELECT
                key_col,
                data24_col2,
                data96_col2,
                fax_col2,
                (SELECT stichtag FROM params) AS stichtag,
                (SELECT jobkennung FROM params) AS jobkennung,
                (SELECT eintragsnr FROM params) AS eintragsnr,
                (SELECT wiederanlaufwert FROM params) AS wiederanlaufwert,
                CURRENT_TIMESTAMP() AS load_ts
            FROM joined_all
        )
        SELECT
            *
        FROM final_rows;
        """
        return sql

    # Wrapper function to create the BigQuery operator with all required parameters
    def process_poolbasisprodukt():
        # Parameters can be templated or replaced with Airflow Variables/Params as needed
        stichtag = "{{ dag_run.conf.get('stichtag', '01012024') }}"
        jobkennung = "{{ dag_run.conf.get('jobkennung', 'DEFAULT_JOB') }}"
        eintragsnr = "{{ dag_run.conf.get('eintragsnr', '0') }}"
        wiederanlaufwert = "{{ dag_run.conf.get('wiederanlaufwert', 0) }}"

        query = build_poolbasisprodukt_sql(
            stichtag=stichtag,
            jobkennung=jobkennung,
            eintragsnr=eintragsnr,
            wiederanlaufwert=wiederanlaufwert,
        )

        return BigQueryExecuteQueryOperator(
            task_id="poolbasisprodukt_bq_processing",
            sql=query,
            use_legacy_sql=False,
            create_disposition="CREATE_IF_NEEDED",
            write_disposition="WRITE_TRUNCATE",
            location="{{ var.value.bq_location }}",
            gcp_conn_id="google_cloud_default",
        )

    # Single task executing the complete BigQuery logic
    poolbasisprodukt_task = process_poolbasisprodukt()

    # Explicit task dependency placeholder for extensibility
    start = PythonOperator(
        task_id="start",
        python_callable=lambda: None,
    )

    end = PythonOperator(
        task_id="end",
        python_callable=lambda: None,
    )

    start >> poolbasisprodukt_task >> end
```

### === Result for vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_cntrct_dist.ksh ===
```python
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.utils.dates import days_ago

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# DAG definition
with DAG(
    dag_id="ausd_bp_ta_cntrct_dist",
    default_args=default_args,
    description="Initial provisioning of selected base products for BERT using BigQuery",
    schedule_interval=None,
    catchup=False,
    tags=["bigquery", "bert", "contract", "provisioning"],
) as dag:

    # Python function to encapsulate all SQL logic and build a single BigQuery query
    def build_bigquery_sql(stichtag: str, wiederanlaufwert: int) -> str:
        return f"""
        -- Create or replace the target table with the filtered contract cache data
        CREATE TABLE IF NOT EXISTS `project.dataset.target_table` AS
        WITH params AS (
            SELECT
                PARSE_DATE('%d%m%Y', '{stichtag}') AS stichtag,
                {wiederanlaufwert} AS wiederanlaufwert
        ),
        source_data AS (
            SELECT
                t.*
            FROM `project.dataset.source_table` t
            CROSS JOIN params p
            WHERE
                t.gueltig_von <= p.stichtag
                AND p.stichtag < t.gueltig_bis
                AND t.ladedatum < p.stichtag
                AND t.dwh_vertrag_id > p.wiederanlaufwert
        ),
        deduplicated_data AS (
            SELECT
                *
            FROM source_data
        )
        SELECT
            *
        FROM deduplicated_data
        """

    # Python function to create the BigQuery operator using a single SQL statement
    def process_contract_distribution():
        sql_query = build_bigquery_sql(
            stichtag="{{ dag_run.conf.get('stichtag', ds_nodash) }}",
            wiederanlaufwert="{{ dag_run.conf.get('wiederanlaufwert', 0) }}",
        )

        return BigQueryExecuteQueryOperator(
            task_id="process_contract_distribution",
            sql=sql_query,
            use_legacy_sql=False,
            create_disposition="CREATE_IF_NEEDED",
            write_disposition="WRITE_TRUNCATE",
            gcp_conn_id="google_cloud_default",
        )

    # Single BigQuery task
    contract_distribution_task = process_contract_distribution()

    # Task dependency placeholder for extensibility
    contract_distribution_task
```

### === Result for vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_bp_ta_cntrct_dist.sql ===
```python
from datetime import timedelta

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.utils.dates import days_ago

# ------------------------------------------------------------------------------
# DAG default arguments
# ------------------------------------------------------------------------------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ------------------------------------------------------------------------------
# SQL logic encapsulated in a single Python function
# - Truncates the target table if it already exists
# - Inserts distinct contract IDs from the source table
# - Uses CREATE_IF_NEEDED to create the target table if missing
# ------------------------------------------------------------------------------
def build_cntrct_dist_sql():
    return """
    -- Step01: Clear the target table for restart safety
    TRUNCATE TABLE `sof$ta_cntrct_dist`;

    -- Step07: Populate the target table with distinct contract IDs
    INSERT INTO `sof$ta_cntrct_dist` (CNTRCT_ID)
    SELECT DISTINCT
        cntrct_id
    FROM `sof$ta_bpr_basis`;
    """

# ------------------------------------------------------------------------------
# DAG definition
# ------------------------------------------------------------------------------
with DAG(
    dag_id="d_ausd_bp_ta_cntrct_dist",
    default_args=default_args,
    description="BigQuery DAG for creating and populating distinct contract table",
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["bigquery", "data-processing", "contract-distinct"],
) as dag:

    # Single BigQuery operator executing the full SQL logic in one task
    process_cntrct_dist = BigQueryExecuteQueryOperator(
        task_id="process_cntrct_dist",
        sql=build_cntrct_dist_sql(),
        use_legacy_sql=False,
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_APPEND",
        location="US",
    )

    # Task dependency placeholder for modular extensibility
    process_cntrct_dist
```

---

## 4. CONTEXT THE MCP COULD NOT SEE

### A. Lineage & Source-to-Target Data Flow
*   **Upstream Inputs:**
    *   `sof$ta_bpr_basis` (Source table, contains baseline basis product instances).
    *   `isbert_schema.dwtk_meldungen` (Configuration/metadata table used to retrieve variables like `v_datum` from the `BERT_DROP_TEMP_TABLE` job execution).
*   **Downstream Outputs:**
    *   `sof$ta_cntrct_dist` (Target table populated with unique `CNTRCT_ID`s).
*   **Legacy Oracle Schema:** `isbert_schema`
*   **BigQuery Target Mapping:**
    *   `isbert_schema.dwtk_meldungen` $\rightarrow$ `` `project_id.dataset_id.dwtk_meldungen` ``
    *   `sof$ta_bpr_basis` $\rightarrow$ `` `project_id.dataset_id.sof_ta_bpr_basis` ``
    *   `sof$ta_cntrct_dist` $\rightarrow$ `` `project_id.dataset_id.sof_ta_cntrct_dist` ``

*(Note: Oracle special characters such as `$` in table names are sanitized to `_` in BigQuery to prevent query compilation and project standard compliance issues).*

### B. Cross-File Dependencies & Redundant Code
In the legacy control script `k_ausd_bp_ta_cntrct_dist.ksh`, there is a large section of commented-out logic performing sorting, merging, and filtering on the files `cibasis_data24.dat`, `cibasis_data96.dat`, and `cibasis_fax.dat`. 
*   **Critical Verification:** This code block is entirely commented out in production. It is **not** active legacy business logic and **must not** be implemented during target-state development.
*   **Active Logic:** The *only* active logic in the legacy script executes the SQL file `d_ausd_bp_ta_cntrct_dist.sql`.

### C. Target File Plan
To consolidate the legacy layers (XML, wrapper shell, kernel shell, and SQL), we will deploy a single Cloud Composer pipeline.

| Target Relative File Path | Target Language | Source Legacy File(s) | Role / Purpose |
| :--- | :--- | :--- | :--- |
| `dags/ausd_bp_ta_cntrct_dist.py` | Python (Airflow DAG) | `DW.BERT_AUSD_BP_TA_CNTRCT_DIST.xml`, `r_ausd_bp_ta_cntrct_dist.ksh`, `k_ausd_bp_ta_cntrct_dist.ksh` | DAG definition containing schedule, parameters, logging orchestration, and task chain. |
| `gcs/sql/d_ausd_bp_ta_cntrct_dist.sql` | Google Standard SQL | `d_ausd_bp_ta_cntrct_dist.sql` | Core SQL logic with Oracle features rewritten to standard BigQuery syntax. |

### D. Environment-Specific Variable Replacements
The final Airflow DAG and SQL definitions must use dynamic parameters defined as Airflow variables. The Build Agent must implement:
*   `{{ var.value.gcp_project_id }}` - GCP Project ID
*   `{{ var.value.bq_dataset }}` - Target BigQuery Dataset (replaces legacy `isbert_schema` / prefix)
*   `{{ var.value.bq_location }}` - Target GCP Region (e.g., `europe-west3` or `US`)
*   `{{ var.value.gcp_conn_id }}` - Default BigQuery Connection ID (`google_cloud_default`)

### E. Risks, Manual Steps, and Design Redesigns
1.  **Parallel Hints:** The Oracle hint `/*+ parallel(rp,4) */` is obsolete in BigQuery. BigQuery engine manages dynamic parallelization automatically. This hint must be removed.
2.  **Oracle System Date Logic:** The script `gestern.ksh` and Oracle date conversions are converted to native BigQuery date operations (`PARSE_DATE`, `CURRENT_DATE`, `DATE_SUB`).
3.  **Truncate Utility Package:** The call to `isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE...')` is replaced with the native BigQuery statement:
    ```sql
    TRUNCATE TABLE `project_id.dataset_id.sof_ta_cntrct_dist`;
    ```
4.  **Special Character Sanitization:** Table names containing `$` (such as `sof$ta_bpr_basis`) should be mapped to `_` in BigQuery targets (`sof_ta_bpr_basis`) to align with standard identifiers. Use dataset configuration settings if legacy name preservation is strictly required.