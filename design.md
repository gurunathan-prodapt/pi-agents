# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_templ.ksh

## 1. Purpose & Scope
This migration targets the ETL workflow initiated by `r_ausd_v_ta_cntrct_templ.ksh`. This job is responsible for reconciling contract data and populating the `ta_cntrct_templ` table in the legacy Oracle system. The `r_ausd_v_ta_cntrct_templ.ksh` script acts as a wrapper, handling environment setup, general parameter parsing, and error logging. It then invokes `k_ausd_v_ta_cntrct_templ.ksh`, which is an orchestrator that further sets up the environment, processes specific parameters for job management, and ultimately executes the core data transformation logic contained in `d_ausd_v_ta_cntrct_templ.sql`. The SQL script truncates and re-populates the target table `SOF$TA_CNTRCT_TEMPL` based on joined data from `CDS$TA_CNTRCT_TEMPLATE` and `CDS$TA_CARE_DESCRIPTION`, applying date-based filtering.

The scope of this migration is to re-implement this entire workflow on Google Cloud Platform, utilizing BigQuery for data storage and transformations, and Cloud Composer (Apache Airflow) for orchestration.

## 2. Source Inventory
The job consists of three primary components:

*   **`r_ausd_v_ta_cntrct_templ.ksh`**
    *   **Technology:** KornShell Script
    *   **Purpose:** Wrapper, environment setup, parameter parsing, error logging.
    *   **Complexity Tier:** Medium (inferred)
    *   **Automation Bucket:** Manual (B3) (inferred, due to its wrapper nature and absence from automation rate)

*   **`k_ausd_v_ta_cntrct_templ.ksh`**
    *   **Technology:** KornShell Script
    *   **Purpose:** Orchestrator, job parameter handling, SQL script execution.
    *   **Complexity Tier:** Medium (inferred)
    *   **Automation Bucket:** Semi-Auto (B2) (inferred, as it orchestrates SQL execution which is a common pattern)

*   **`d_ausd_v_ta_cntrct_templ.sql`**
    *   **Technology:** Oracle SQL*Plus
    *   **Purpose:** Truncates and populates `SOF$TA_CNTRCT_TEMPL` based on joined source data with date filtering.
    *   **Complexity Tier:** Medium (inferred, due to truncate/insert, joins, and date logic)
    *   **Automation Bucket:** Semi-Auto (B2) (inferred, as it's a standard ETL SQL pattern)

## 3. Target Architecture
The migrated solution will leverage Google Cloud Platform services:
*   **BigQuery:** All data storage (source, staging, target tables) and SQL transformations.
    *   **Target Table:** `project.dataset.sof_ta_cntrct_templ` (replaces legacy `SOF$TA_CNTRCT_TEMPL`)
    *   **Source Tables:**
        *   `project.dataset.dwtk_meldungen` (replaces `isbert_schema.dwtk_meldungen`)
        *   `project.dataset.cds_ta_cntrct_template` (replaces `cds$ta_cntrct_template@pcrs1`)
        *   `project.dataset.cds_ta_care_description` (replaces `cds$ta_care_description@pcrs1`)
*   **Cloud Composer (Apache Airflow):** For workflow orchestration, replacing the shell scripts (`r_*.ksh`, `k_*.ksh`). This will manage dependencies, parameter passing, and execution of BigQuery SQL tasks.
*   **Cloud Storage:** Potentially for staging data if direct BigQuery data transfer is not feasible for external systems.

## 4. Data Flow & Lineage
The original data flow is a chained execution:
`r_ausd_v_ta_cntrct_templ.ksh` (Wrapper)
  -> Calls `k_ausd_v_ta_cntrct_templ.ksh` (Orchestrator)
     -> Calls `d_ausd_v_ta_cntrct_templ.sql` (Data Transformation)
        -> Reads from `isbert_schema.dwtk_meldungen`, `cds$ta_cntrct_template`, `cds$ta_care_description`
        -> Writes to `SOF$TA_CNTRCT_TEMPL`

The migrated data flow will be orchestrated by an Airflow DAG:
**Airflow DAG (Python)**:
1.  **Extract `v_datum` Task:** A BigQuery PythonOperator or `BigQueryExecuteQueryOperator` to derive `v_datum` from `project.dataset.dwtk_meldungen`. This value will be passed as a templated parameter.
2.  **Truncate Target Table Task:** A `BigQueryExecuteQueryOperator` to `TRUNCATE TABLE project.dataset.sof_ta_cntrct_templ;`.
3.  **Insert Data Task:** A `BigQueryExecuteQueryOperator` to execute the transformed BigQuery SQL, using the `v_datum` from the prior step. This task will populate `project.dataset.sof_ta_cntrct_templ`.

## 5. Transformation Logic

### 5.1. `r_ausd_v_ta_cntrct_templ.ksh` and `k_ausd_v_ta_cntrct_templ.ksh` Orchestration Logic
The functionality of these KornShell scripts, including environment initialization (`. $HOME/.dw_init`), error handling (`f_alis_msgerr.ksh`), date utilities (`h_alis_date.ksh`), parameter parsing (`h_alis_parameter.ksh`), and SQL script execution via `starteSQLSkript` (which likely uses `h_alis_sqlplus.ksh`), will be re-implemented as a Python-based Apache Airflow DAG.

*   **Environment Setup:** GCP environment variables, service accounts, and Airflow connections will replace `.dw_init` and `BERT_DIR_ROOT`.
*   **Error Handling:** Airflow's built-in error handling, retry mechanisms, and alerting (e.g., Slack, PagerDuty) will replace `f_alis_msgerr.ksh` and `DWMSG_` functions.
*   **Parameter Passing:** Airflow's `params` and `xcom` mechanisms will manage parameters like `JobKennung` and `EintragsNr`.
*   **SQL Execution:** `BigQueryExecuteQueryOperator` will directly execute the BigQuery SQL.

### 5.2. `d_ausd_v_ta_cntrct_templ.sql` Data Transformation Logic (Oracle SQL to BigQuery SQL)

The Oracle SQL*Plus script's core transformation will be converted to BigQuery SQL.

**Original Logic Breakdown:**
1.  **Define `v_carmen`:** A DB-Link variable (`@pcrs1`).
2.  **Determine `v_datum` (processing date):**
    *   Selects `MAX(m.timecreated)` from `isbert_schema.dwtk_meldungen` where `m.job_kennung = 'BERT_DROP_TEMP_TABLE'`.
    *   Converts `timecreated` to `YYYYMMDD` format.
    *   Defaults to `19000101` if no records are found.
3.  **Truncate Target Table:** `TRUNCATE TABLE sof$ta_cntrct_templ` (executed via `isbert_schema.DWPA_UTIL_SKRIPT.runstatement`).
4.  **Insert Data into Target Table:**
    *   **Target:** `sof$ta_cntrct_templ`
    *   **Columns:** `CNTRCT_TEMPLATE_ID`, `CDS_DESCRIPTION_ID`, `CDS_DESCRIPTION`
    *   **Source Join:** `cds$ta_cntrct_template` (aliased as `ct`) joined with `cds$ta_care_description` (aliased as `cd`) on `ct.cds_description_id = cd.cds_description_id`.
    *   **Filtering:**
        *   `ct.insert_at <= TO_DATE('&v_datum','YYYYMMDD')`
        *   `(ct.modified_at IS NULL OR ct.modified_at > TO_DATE('&v_datum','YYYYMMDD'))`
        *   `ct.valid_from <= TO_DATE('&v_datum','YYYYMMDD')`
        *   `(ct.valid_to IS NULL OR ct.valid_to > TO_DATE('&v_datum','YYYYMMDD'))`
        *   `ct.is_production = 1`
        *   `cd.language = 1`
    *   **Commit:** Explicit `COMMIT`.

**Migrated BigQuery SQL:**

```sql
-- BigQuery SQL for d_ausd_v_ta_cntrct_templ.sql

-- 1. Declare processing date variable (v_datum)
DECLARE v_datum DATE DEFAULT (
  SELECT COALESCE(MAX(DATE(m.timecreated)), DATE '1900-01-01')
  FROM `project.dataset.dwtk_meldungen` m
  WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
);

-- 2. Truncate the target table
TRUNCATE TABLE `project.dataset.sof_ta_cntrct_templ`;

-- 3. Insert transformed data
INSERT INTO `project.dataset.sof_ta_cntrct_templ`
(
  cntrct_template_id,
  cds_description_id,
  cds_description
)
SELECT
  ct.cntrct_template_id,
  ct.cds_description_id,
  cd.cds_description
FROM `project.dataset.cds_ta_cntrct_template` ct
JOIN `project.dataset.cds_ta_care_description` cd
  ON ct.cds_description_id = cd.cds_description_id
WHERE ct.insert_at <= v_datum
  AND (ct.modified_at IS NULL OR ct.modified_at > v_datum)
  AND ct.valid_from <= v_datum
  AND (ct.valid_to IS NULL OR ct.valid_to > v_datum)
  AND ct.is_production = 1
  AND cd.language = 1;
```

**Conversion Notes:**
*   Oracle `NVL` is mapped to BigQuery `COALESCE`.
*   Oracle `TO_DATE` and `TO_CHAR` functions for date manipulation are replaced by BigQuery's native `DATE` casting and comparison.
*   Oracle SQL*Plus specific commands (`DEFINE`, `COLUMN`, `START`, `SPOOL`, `WHENEVER SQLERROR`, `PROMPT`, `COMMIT`) are handled by the Airflow orchestration layer or are not applicable in BigQuery SQL context.
*   The DB-Link `&v_carmen` is removed; BigQuery table references use the `project.dataset.table` format. Source tables `cds$ta_cntrct_template` and `cds$ta_care_description` are assumed to be ingested into BigQuery as `project.dataset.cds_ta_cntrct_template` and `project.dataset.cds_ta_care_description` respectively.

## 6. External Dependencies
The legacy job has the following external dependencies:
*   **Oracle Database:** This is the primary data source and target.
    *   **Replacement:** Source tables (`cds$ta_cntrct_template`, `cds$ta_care_description`, `isbert_schema.dwtk_meldungen`) will be migrated/replicated into BigQuery (e.g., via Datastream, Fivetran, or batch exports). The target table (`SOF$TA_CNTRCT_TEMPL`) will be a native BigQuery table.
*   **Oracle DB-Link `@pcrs1`:** Used to access source tables.
    *   **Replacement:** Direct BigQuery table references will be used once data is ingested.
*   **Shell Utilities (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`, `h_alis_sqlplus.ksh`):** These are common utility scripts.
    *   **Replacement:** Their functionalities will be absorbed by Cloud Composer's orchestration capabilities (e.g., error logging, parameter handling, scheduling) or standard Python libraries within the Airflow DAG.
*   **`$HOME/.dw_init` and `BERT_DIR_ROOT`:** Environment initialization.
    *   **Replacement:** GCP project/dataset configurations and Airflow environment variables.

## 7. Unresolved / Risks
*   **Missing `automation_rate` and `file_complexity` data:** For the main wrapper script `r_ausd_v_ta_cntrct_templ.ksh`, specific automation rate and complexity tier data was not available. Inferred values (B3, Medium) are used.
*   **`starteSQLSkript` function details:** The exact implementation of `starteSQLSkript` and its interaction with `h_alis_sqlplus.ksh` and the temporary file for record counts was not fully detailed. It's assumed to be a standard SQL*Plus execution wrapper. The migration proposes direct `BigQueryExecuteQueryOperator` calls which may require re-implementing any custom logic embedded in `starteSQLSkript` (e.g., job status updates, specific error handling) within the Airflow DAG.
*   **`isbert_schema.DWPA_UTIL_SKRIPT.runstatement`:** This Oracle procedure for truncating the table should be replaced by a direct BigQuery `TRUNCATE TABLE` statement. Ensure appropriate permissions are set for the service account running the BigQuery job.
*   **Data Latency:** The migration of Oracle source tables to BigQuery needs to consider the required data freshness and latency requirements.

## 8. Build Plan
The migration will involve the following steps:

1.  **Data Ingestion Pipeline:** Set up a robust data ingestion pipeline (e.g., Datastream, batch transfer jobs) to bring the Oracle source tables (`isbert_schema.dwtk_meldungen`, `cds$ta_cntrct_template`, `cds$ta_care_description`) into BigQuery. Ensure the destination tables in BigQuery are named `project.dataset.dwtk_meldungen`, `project.dataset.cds_ta_cntrct_template`, and `project.dataset.cds_ta_care_description` respectively, and their schemas are compatible.
2.  **Create Target BigQuery Table:** Define the `project.dataset.sof_ta_cntrct_templ` BigQuery table with the appropriate schema (`cntrct_template_id`, `cds_description_id`, `cds_description`).
3.  **Develop BigQuery SQL Transformation:**
    *   Create a BigQuery SQL script (`d_ausd_v_ta_cntrct_templ_bq.sql`) containing the migrated BigQuery SQL provided in Section 5.2.
4.  **Develop Cloud Composer (Airflow) DAG:**
    *   Create a Python DAG file (`r_ausd_v_ta_cntrct_templ_dag.py`) for Cloud Composer.
    *   **Task 1: `extract_v_datum_task`**: Use `BigQueryExecuteQueryOperator` to run the `DECLARE v_datum...` part of the SQL to determine the processing date. Store `v_datum` in XCom for downstream tasks.
    *   **Task 2: `truncate_target_table_task`**: Use `BigQueryExecuteQueryOperator` to execute `TRUNCATE TABLE project.dataset.sof_ta_cntrct_templ;`.
    *   **Task 3: `insert_transformed_data_task`**: Use `BigQueryExecuteQueryOperator` to execute the main `INSERT INTO ... SELECT ...` query from `d_ausd_v_ta_cntrct_templ_bq.sql`. Pass `v_datum` from XCom to this query as a Jinja template variable.
    *   Configure Airflow connections and variables for BigQuery project and dataset names.
    *   Implement Airflow logging and alerting for success/failure notifications.
5.  **Testing:** Thoroughly test the BigQuery SQL transformation and the Airflow DAG with sample data to ensure functional equivalence and performance.
6.  **Deployment:** Deploy the Airflow DAG to Cloud Composer.