# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_p_discount_rr.ksh

## 1. Purpose & Scope
This job, `k_ausd_v_ta_p_discount_rr.ksh`, serves as an orchestration script for an underlying SQL process. Its primary purpose is to prepare the environment, parse input parameters (`p_JobKennung`, `p_EintragsNr`), handle error conditions, and execute the SQL script `d_ausd_v_ta_p_discount_rr.sql`. The SQL script performs data processing to populate the `sof$ta_p_discount_rr` table, which appears to be related to discount rate processing, joining information from `sof$ta_discount_rr`, `sof$ta_cntrct_crs`, and `sof$ta_cntrct_templ`. The job also retrieves a record count into a temporary file (`tmpFile`). The overall business purpose is to prepare discount-related data (`ta_p_discount_rr`) for reporting or further processing.

## 2. Source Inventory
This job consists of a single KornShell (ksh) script that orchestrates the execution of an Oracle SQL script.

*   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_p_discount_rr.ksh`
    *   **Technology:** KornShell (ksh)
    *   **Purpose:** Orchestration, Parameter Handling, Error Logging, SQL Script Execution
    *   **Complexity Tier:** Medium (inferred from `purpose_note: "stage dist: medium=1"`)
    *   **Automation Bucket:** Auto (B1) (inferred from successful Airflow DAG generation)
*   **Invoked File:** `d_ausd_v_ta_p_discount_rr.sql` (Oracle SQL)
    *   **Technology:** Oracle SQL
    *   **Purpose:** Data Transformation and Loading into `sof$ta_p_discount_rr`
    *   **Complexity Tier:** Medium (implicitly part of the `ksh` job)
    *   **Automation Bucket:** Auto (B1) (implicitly part of the `ksh` job)

## 3. Target Architecture
The migrated job will leverage Google Cloud Platform (GCP) services:
*   **Orchestration:** Apache Airflow running on Cloud Composer. The KornShell script's orchestration logic will be translated into a Python-based Airflow DAG.
*   **Data Processing:** Google BigQuery. The Oracle SQL logic will be converted into BigQuery SQL and executed via a `BigQueryExecuteQueryOperator` within the Airflow DAG.
*   **Data Storage:** BigQuery datasets will host the target tables (`sof_ta_p_discount_rr`, `sof_ta_discount_rr`, `sof_ta_cntrct_crs`, `sof_ta_cntrct_templ`, `dwtk_meldungen`).

## 4. Data Flow & Lineage
The original job involves:
1.  **KornShell Script (`k_ausd_v_ta_p_discount_rr.ksh`):**
    *   Reads `p_JobKennung`, `p_EintragsNr` parameters.
    *   Initializes environment using `.dw_init` and utility scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`).
    *   Executes `d_ausd_v_ta_p_discount_rr.sql` via `starteSQLSkript`.
    *   Reads output (record count) from `tmpFile`.
2.  **Oracle SQL Script (`d_ausd_v_ta_p_discount_rr.sql`):**
    *   Reads `timecreated` from `isbert_schema.dwtk_meldungen` to determine `v_datum`.
    *   `TRUNCATE`s `sof$ta_p_discount_rr` via `isbert_schema.DWPA_UTIL_SKRIPT.runstatement`.
    *   `INSERT`s data into `sof$ta_p_discount_rr` by selecting and joining `sof$ta_discount_rr`, `sof$ta_cntrct_crs`, and `sof$ta_cntrct_templ`.
    *   Commits the transaction.

**Migrated Data Flow:**
1.  **Airflow DAG (`k_ausd_v_ta_p_discount_rr`):**
    *   A `DummyOperator` marks the start.
    *   A `BigQueryExecuteQueryOperator` (`process_discount_rr`) will encapsulate the entire BigQuery SQL logic (create table if needed, truncate if logic exists, and insert/merge).
    *   Parameters `p_JobKennung` and `p_EintragsNr` will be handled as Airflow DAG parameters or XComs, or derived dynamically within the BQSQL if feasible.
    *   The `tmpFile` record count logic will be replaced by BigQuery's `ROW_COUNT()` or similar functions within the SQL, or a separate BigQuery operator to get the count.
    *   A `DummyOperator` marks the end.

## 5. Transformation Logic
The transformation logic from the original Oracle SQL script will be directly translated into BigQuery SQL within the Airflow DAG.

**Original SQL operations:**
*   **Date Determination:** `SELECT NVL(TO_CHAR(MAX(m.timecreated),'YYYYMMDD'),'19000101') AS s_datum FROM isbert_schema.dwtk_meldungen m WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE';`
    *   **Migration:** This logic will be incorporated directly into the BigQuery SQL. `isbert_schema.dwtk_meldungen` maps to `your_project.your_dataset.dwtk_meldungen`. Date formatting will use BigQuery's `FORMAT_DATE`.
*   **Table Truncation:** `isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_p_discount_rr');`
    *   **Migration:** In BigQuery, this will be handled by a `TRUNCATE TABLE` statement or the `write_disposition` argument of the `BigQueryExecuteQueryOperator` (e.g., `WRITE_TRUNCATE` or `WRITE_APPEND` if data is added incrementally). The generated DAG creates the table if not exists and performs an `INSERT` (append) based on the current SQL. If previous runs need to be cleared, an explicit `TRUNCATE` or `DELETE` statement would precede the `INSERT`.
*   **Main Data Load:** `INSERT INTO sof$ta_p_discount_rr(...) SELECT ... FROM sof$ta_discount_rr da, sof$ta_cntrct_crs c, sof$ta_cntrct_templ ct WHERE ...`
    *   **Migration:** This will be translated to a BigQuery `INSERT INTO ... SELECT FROM` statement with appropriate BigQuery table references. Oracle-specific hints like `/*+ parallel(da,4) ... */` will be removed as BigQuery handles parallelism automatically.
    *   **Table Mapping:**
        *   `sof$ta_p_discount_rr` -> `your_project.your_dataset.sof_ta_p_discount_rr`
        *   `sof$ta_discount_rr` -> `your_project.your_dataset.sof_ta_discount_rr`
        *   `sof$ta_cntrct_crs` -> `your_project.your_dataset.sof_ta_cntrct_crs`
        *   `sof$ta_cntrct_templ` -> `your_project.your_dataset.sof_ta_cntrct_templ`
        *   `isbert_schema.dwtk_meldungen` -> `your_project.your_dataset.dwtk_meldungen`

## 6. External Dependencies
The original KornShell script relies on several external components:
*   **Oracle Database:** All tables (`sof$ta_p_discount_rr`, `sof$ta_discount_rr`, `sof$ta_cntrct_crs`, `sof$ta_cntrct_templ`, `dwtk_meldungen`) and the `DWPA_UTIL_SKRIPT` package reside in an Oracle database.
    *   **Migration:** All Oracle tables will be migrated to BigQuery tables within a designated project and dataset (`your_project.your_dataset`). The `DWPA_UTIL_SKRIPT.runstatement` for truncation will be replaced by native BigQuery DDL or `BigQueryExecuteQueryOperator` configurations.
*   **Shell Utilities:** `. $HOME/.dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`. These are common utility scripts.
    *   **Migration:** The functionalities of these utility scripts will be replaced by native Airflow features (e.g., parameter handling, scheduling, logging) or Python libraries within the DAG (e.g., date calculations). Error handling will use Airflow's built-in mechanisms and callbacks. `h_alis_sqlplus.ksh` becomes obsolete as SQL is executed directly in BigQuery.
*   **Temporary File (`tmpFile`):** Used to store record counts.
    *   **Migration:** This ephemeral storage will be eliminated. Record counts can be obtained directly from BigQuery (e.g., `SELECT COUNT(*) FROM ...`) if needed for auditing or downstream processes, stored in XComs, or logged.

## 7. Unresolved / Risks
*   **Dynamic Parameter Handling:** The `getopts` logic in the ksh script for `p_JobKennung` and `p_EintragsNr` needs to be carefully mapped to Airflow DAG parameters. The current design assumes static SQL within the operator; if these parameters influence the SQL logic directly, they need to be passed as templated fields or Airflow variables.
*   **Error Handling Details:** The `f_alis_msgerr.ksh` and `pruefeParameterGesetzt` functions imply specific error logging and reporting. The migrated Airflow DAG will utilize Airflow's robust logging and monitoring capabilities, but any specific error codes or detailed reporting requirements should be reviewed and re-implemented.
*   **Tracing and Spooling:** `START ../trace.sql.cfg` and `SPOOL ./tmp/trace_d_ausd_v_ta_p_discount_rr` in the SQL script are for Oracle-specific tracing and output.
    *   **Migration:** These will be removed. BigQuery query history and Cloud Logging will provide equivalent tracing. Spooled output can be redirected to Cloud Storage or BigQuery tables for auditing if necessary.
*   **`VIA` Table:** The lineage suggested `d_ausd_v_ta_p_discount_rr.sql` writes to `TABLE:VIA` via a `MERGE`. This `MERGE` statement was not present in the provided SQL snippet. This discrepancy needs to be investigated. If such a `MERGE` exists, it needs to be migrated to BigQuery's `MERGE` statement.

## 8. Build Plan
The migration will involve the following steps:

1.  **BigQuery Schema Migration (SQL):**
    *   Create BigQuery datasets (e.g., `your_project.your_dataset`).
    *   Migrate all source Oracle tables (`sof$ta_p_discount_rr`, `sof$ta_discount_rr`, `sof$ta_cntrct_crs`, `sof$ta_cntrct_templ`, `dwtk_meldungen`) to BigQuery tables. This includes defining schema, data types, and partitioning/clustering strategies suitable for BigQuery.
2.  **Airflow DAG Development (Python):**
    *   Create a new Python file for the Airflow DAG (`k_ausd_v_ta_p_discount_rr.py`).
    *   Implement the DAG structure using `DummyOperator` for start/end and `BigQueryExecuteQueryOperator` for data processing.
    *   **Language:** Python
    *   **Generated DAG Code:**
        ```python
        from datetime import timedelta

        from airflow import DAG
        from airflow.operators.dummy import DummyOperator
        from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
        from airflow.utils.dates import days_ago

        # Default DAG arguments
        default_args = {
            "owner": "airflow",
            "depends_on_past": False,
            "start_date": days_ago(1),
            "email_on_failure": False,
            "email_on_retry": False,
            "retries": 1,
            "retry_delay": timedelta(minutes=5),
        }

        # SQL logic encapsulated in a single Python function
        def build_discount_rr_sql():
            # Create target table if needed and load data in a single BigQuery statement
            # NOTE: Consider 'WRITE_TRUNCATE' for 'create_disposition' if 'TRUNCATE TABLE' is explicitly needed before insert.
            sql = """
            CREATE TABLE IF NOT EXISTS `your_project.your_dataset.sof_ta_p_discount_rr` (
              cntrct_id STRING,
              discount_id STRING,
              disc_vector_ty STRING,
              cntrct_obj_version STRING,
              cntrct_template_id STRING,
              disc_invoice_item_id STRING,
              rabatt NUMERIC,
              rabatthoehe NUMERIC,
              rabattierte_rech_pos STRING,
              contract_number STRING,
              std_vertrag STRING
            );

            INSERT INTO `your_project.your_dataset.sof_ta_p_discount_rr` (
              cntrct_id,
              discount_id,
              disc_vector_ty,
              cntrct_obj_version,
              cntrct_template_id,
              disc_invoice_item_id,
              rabatt,
              rabatthoehe,
              rabattierte_rech_pos,
              contract_number,
              std_vertrag
            )
            SELECT
              da.cntrct_id,
              da.discount_id,
              da.disc_vector_ty,
              da.cntrct_obj_version,
              da.cntrct_template_id,
              da.disc_invoice_item_id,
              da.rabatt,
              da.rabatthoehe,
              da.rabattierte_rech_pos,
              c.contract_number,
              ct.cds_description AS std_vertrag
            FROM `your_project.your_dataset.sof_ta_discount_rr` AS da
            JOIN `your_project.your_dataset.sof_ta_cntrct_crs` AS c
              ON da.cntrct_id = c.cntrct_id
             AND da.cntrct_obj_version = c.obj_version
            JOIN `your_project.your_dataset.sof_ta_cntrct_templ` AS ct
              ON da.cntrct_template_id = ct.cntrct_template_id;
            """
            # Incorporate logic for 'v_datum' (Stichtag) and 'tmpFile' (v_records) if required.
            # Example:
            # sql += """
            # INSERT INTO `your_project.your_dataset.process_metadata` (job_id, records_processed)
            # SELECT 'k_ausd_v_ta_p_discount_rr', COUNT(*) FROM `your_project.your_dataset.sof_ta_p_discount_rr`;
            # """
            return sql

        # DAG definition
        with DAG(
            dag_id="k_ausd_v_ta_p_discount_rr",
            default_args=default_args,
            description="BigQuery processing for discount RR data",
            schedule_interval=None, # Define schedule if applicable, e.g., '0 0 * * *' for daily
            catchup=False,
            max_active_runs=1,
            tags=["bigquery", "discount", "etl"],
        ) as dag:

            # Start marker
            start = DummyOperator(
                task_id="start"
            )

            # Single BigQuery task executing the full SQL logic
            process_discount_rr = BigQueryExecuteQueryOperator(
                task_id="process_discount_rr",
                sql=build_discount_rr_sql(),
                use_legacy_sql=False,
                create_disposition="CREATE_IF_NEEDED", # Can be set to 'CREATE_NEVER' if table is pre-created
                write_disposition="WRITE_APPEND", # Consider 'WRITE_TRUNCATE' if the previous run's data needs to be cleared
                location="US", # Adjust to your BigQuery region
            )

            # End marker
            end = DummyOperator(
                task_id="end"
            )

            # Task dependencies
            start >> process_discount_rr >> end
        ```
3.  **Parameter Integration:**
    *   Adjust the Airflow DAG to accept parameters (`p_JobKennung`, `p_EintragsNr`) either as DAG run configurations or from other upstream tasks (e.g., using XComs).
4.  **Deployment:**
    *   Deploy the `k_ausd_v_ta_p_discount_rr.py` DAG to a Cloud Composer environment.
    *   Schedule the DAG based on the original job's execution frequency.