# Migration Design — DW.BERT_AUSD_BP_TA_BCP_ICCID

## 1. Purpose & Scope
This document outlines the migration plan for the legacy ETL job `DW.BERT_AUSD_BP_TA_BCP_ICCID`. The job's primary purpose is to prepare selected 'Basisprodukte' (basic products) for BERT's demand scoring system. This involves orchestrating the execution of a KornShell script via a UC4 job, which in turn calls another KornShell script to prepare parameters and environment, finally executing an Oracle SQL*Plus script. The SQL script truncates a target table (`SOF$TA_BCP_ICCID`) and loads it with enriched data from `SOF$TA_BPR_BCP` and `SOF$TA_ICCID_VERTRAG`, as well as deriving a date variable from `DWTK_MELDUNGEN`. The scope of this migration is to re-platform this entire workflow to Google Cloud Platform, utilizing Cloud Composer (Airflow) for orchestration and BigQuery for data processing.

## 2. Source Inventory
The job `DW.BERT_AUSD_BP_TA_BCP_ICCID` is composed of four primary files:

*   **`vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/BERT/PRODUKTION/DW.BERT_STAMMDATEN_JP/DW.BERT_P_BASISPRODUKT_JP/DW.BERT_AUSD_BP_TA_BCP_ICCID.xml`**
    *   **Technology:** UC4/Automic (XML)
    *   **Summary:** UC4 job definition (UNIX job type) responsible for scheduling and orchestrating the execution of the `r_ausd_bp_ta_bcp_iccid.ksh` script.
    *   **Purpose:** Orchestration/Scheduler
    *   **Complexity Tier:** (Data not available)
    *   **Automation Bucket:** (Data not available)

*   **`vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bcp_iccid.ksh`**
    *   **Technology:** KornShell
    *   **Summary:** This ksh script acts as an orchestration layer. It handles parameter parsing (snapshot date, restart value), sets up the environment by sourcing other utility scripts, and calls `k_ausd_bp_ta_bcp_iccid.ksh`.
    *   **Purpose:** Orchestration/ETL
    *   **Complexity Tier:** (Data not available)
    *   **Automation Bucket:** (Data not available)

*   **`vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bcp_iccid.ksh`**
    *   **Technology:** KornShell
    *   **Summary:** A control script responsible for further parameter parsing, date validation, sourcing helper functions, and ultimately executing the core Oracle SQL script `d_ausd_bp_ta_bcp_iccid.sql` via a `starteSQLSkript` function.
    *   **Purpose:** Orchestration/ETL
    *   **Complexity Tier:** (Data not available)
    *   **Automation Bucket:** (Data not available)

*   **`vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_bp_ta_bcp_iccid.sql`**
    *   **Technology:** Oracle PL/SQL (SQL*Plus script)
    *   **Summary:** This SQL*Plus script performs the data transformation. It truncates the `SOF$TA_BCP_ICCID` table and then loads it with enriched data by joining `SOF$TA_BPR_BCP` and `SOF$TA_ICCID_VERTRAG`. It also derives a date variable (`v_datum`) from `DWTK_MELDUNGEN`.
    *   **Purpose:** ETL
    *   **Complexity Tier:** (Data not available)
    *   **Automation Bucket:** (Data not available)

*Note: `file_complexity` and `automation_rate` data were not available for any of the identified source files within the provided job scope.*

## 3. Target Architecture
The target architecture on Google Cloud Platform will consist of:

*   **Cloud Composer (Apache Airflow):** For workflow orchestration, scheduling, and execution of tasks.
*   **BigQuery:** As the primary data warehouse for all transformed data and source data tables (e.g., `DWTK_MELDUNGEN`, `SOF$TA_BPR_BCP`, `SOF$TA_ICCID_VERTRAG`, `SOF$TA_BCP_ICCID`).
*   **Cloud Storage:** For storing intermediate data files, Airflow DAGs, and potentially logs.
*   **Python:** For refactoring KornShell orchestration logic into Airflow tasks.

The overall flow will be:
UC4 Job (Scheduler) → Airflow DAG (Orchestration) → Python Operators (KornShell logic) → BigQuery (SQL ETL).

## 4. Data Flow & Lineage
The original job's data flow, inferred from file analysis and content, is as follows:

1.  **UC4 Job (`DW.BERT_AUSD_BP_TA_BCP_ICCID.xml`)** acts as the scheduler and entry point. It directly invokes `r_ausd_bp_ta_bcp_iccid.ksh`.
2.  **`r_ausd_bp_ta_bcp_iccid.ksh`** (KornShell Orchestrator):
    *   Receives parameters (Stichtag, Wiederanlaufwert).
    *   Sets up environment.
    *   Calls `k_ausd_bp_ta_bcp_iccid.ksh` with parsed parameters.
3.  **`k_ausd_bp_ta_bcp_iccid.ksh`** (KornShell Control Script):
    *   Performs further parameter validation and date checks.
    *   Sources common utility scripts (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`).
    *   Executes `d_ausd_bp_ta_bcp_iccid.sql` using a `starteSQLSkript` function, passing environment variables and parameters.
4.  **`d_ausd_bp_ta_bcp_iccid.sql`** (Oracle SQL*Plus Script):
    *   **Inputs:**
        *   `isbert_schema.dwtk_meldungen` (to determine `v_datum`)
        *   `sof$ta_bpr_bcp`
        *   `sof$ta_iccid_vertrag`
    *   **Transformation:** Truncates `sof$ta_bcp_iccid`, then `INSERT INTO ... SELECT DISTINCT` from `sof$ta_bpr_bcp` joined with `sof$ta_iccid_vertrag`.
    *   **Output:** `sof$ta_bcp_iccid` (target table).

**Target Data Flow (BigQuery & Airflow):**

1.  **Airflow DAG (`dw_bert_ausd_bp_ta_bcp_iccid`):** This DAG, scheduled in Cloud Composer, will be the new entry point.
2.  **`r_ausd_bp_ta_bcp_iccid_task` (Python Operator):** This task will encapsulate the logic from `r_ausd_bp_ta_bcp_iccid.ksh` and `k_ausd_bp_ta_bcp_iccid.ksh`. It will handle:
    *   Parameter parsing (Stichtag, Wiederanlaufwert) for the BigQuery SQL.
    *   Environment setup (e.g., retrieving project IDs, dataset names).
    *   Constructing the BigQuery SQL query with dynamic parameters.
    *   Executing the BigQuery SQL query.
3.  **BigQuery ETL:** The translated SQL logic from `d_ausd_bp_ta_bcp_iccid.sql` will be executed directly in BigQuery.
    *   **Input Tables (BigQuery):**
        *   `isbert_schema.dwtk_meldungen` (e.g., `project.dataset.dwtk_meldungen`)
        *   `sof_ta_bpr_bcp` (e.g., `project.dataset.sof_ta_bpr_bcp`)
        *   `sof_ta_iccid_vertrag` (e.g., `project.dataset.sof_ta_iccid_vertrag`)
    *   **Output Table (BigQuery):**
        *   `sof_ta_bcp_iccid` (e.g., `project.dataset.sof_ta_bcp_iccid`)

## 5. Transformation Logic

*   **Orchestration Logic (`DW.BERT_AUSD_BP_TA_BCP_ICCID.xml`, `r_ausd_bp_ta_bcp_iccid.ksh`, `k_ausd_bp_ta_bcp_iccid.ksh`):**
    *   The UC4 job's scheduling will be replaced by Airflow's scheduling mechanism.
    *   The parameter parsing, date validation, and script invocation logic within `r_ausd_bp_ta_bcp_iccid.ksh` and `k_ausd_bp_ta_bcp_iccid.ksh` will be refactored into a single Python function or a series of Python functions called by an Airflow `PythonOperator`.
    *   Shell environment sourcing (`. $HOME/.dw_init`) will be replaced by Airflow's environment management or direct BigQuery connection/client setup.
    *   The `starteSQLSkript` function (implicitly called by ksh) will be replaced by a BigQuery hook/operator in Airflow that directly executes the generated BQ SQL.

*   **SQL Transformation Logic (`d_ausd_bp_ta_bcp_iccid.sql`):**
    *   **Variable Definition:**
        *   `DEFINE v_carmen = "@pcrs1"` will be handled as a Python variable or Airflow variable if dynamic.
        *   `v_datum` derivation from `isbert_schema.dwtk_meldungen` will be translated to BigQuery SQL using `COALESCE(FORMAT_TIMESTAMP('%Y%m%d', MAX(m.timecreated)), '19000101')`.
    *   **Truncation:** `isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_bcp_iccid REUSE STORAGE');` will be replaced by a BigQuery `TRUNCATE TABLE `sof_ta_bcp_iccid`` statement.
    *   **Data Load/Enrichment:**
        ```sql
        INSERT INTO sof$ta_bcp_iccid
        (CNTRCT_ID, BPR_ID, CNTRCT_ID_REF, TN_ICCID, TN_IMSI_HLR)
        SELECT /*+ full(bp) parallel(bp,4) full(ic) parallel(ic,4) */
                  distinct
                  bp.cntrct_id,
                  bp.bpr_id,
                  bp.cntrct_id_ref,
                  ic.tn_iccid,
                  ic.tn_imsi_hlr
        FROM      sof$ta_bpr_bcp bp,
                  sof$ta_iccid_vertrag ic
        WHERE     bp.cntrct_id_ref = ic.cntrct_id;
        ```
        This will be translated to standard BigQuery SQL:
        ```sql
        INSERT INTO `project.dataset.sof_ta_bcp_iccid`
          (CNTRCT_ID, BPR_ID, CNTRCT_ID_REF, TN_ICCID, TN_IMSI_HLR)
        SELECT DISTINCT
          bp.cntrct_id,
          bp.bpr_id,
          bp.cntrct_id_ref,
          ic.tn_iccid,
          ic.tn_imsi_hlr
        FROM `project.dataset.sof_ta_bpr_bcp` AS bp
        JOIN `project.dataset.sof_ta_iccid_vertrag` AS ic
          ON bp.cntrct_id_ref = ic.cntrct_id;
        ```
        Oracle hints (`/*+ full(bp) parallel(bp,4) ... */`) will be removed as they are not applicable in BigQuery. Implicit comma join will be converted to an explicit `JOIN` clause.
    *   **SQL*Plus Commands:** `prompt`, `start`, `spool`, `WHENEVER SQLERROR`, `set timing on`, `COMMIT`, `exit success` will be removed or managed by Airflow's logging and error handling mechanisms.

## 6. External Dependencies
The `lineage_assembled_jobs` record indicates no external systems were explicitly identified for this job within the lineage data. However, based on the `file_analysis` and content:

*   **Oracle Database:** The `d_ausd_bp_ta_bcp_iccid.sql` script clearly interacts with an Oracle database (e.g., `isbert_schema.dwtk_meldungen`, `sof$ta_bpr_bcp`, `sof$ta_iccid_vertrag`, `sof$ta_bcp_iccid`).
    *   **Replacement Strategy:** All Oracle tables referenced will be migrated to BigQuery. Initial data loading will involve a one-time migration, followed by setting up continuous data replication (e.g., using Datastream or a custom CDC solution) to keep the BigQuery tables synchronized with the source Oracle system until the Oracle system can be deprecated.

*   **Local File System (for KornShell scripts):** The ksh scripts read/write temporary files (`tmpFile`, logs) and source other scripts from the local file system (`$HOME/.dw_init`, `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh` etc.).
    *   **Replacement Strategy:** Temporary file handling will be refactored to use Cloud Storage for any necessary intermediate storage. Utility functions will be translated into Python modules and imported within the Airflow DAG's Python tasks. Logging will leverage Airflow's native logging capabilities, integrated with Cloud Logging.

## 7. Unresolved / Risks

*   **Missing Complexity and Automation Rate Data:** The `file_complexity` and `automation_rate` tables returned no rows for any of the source files. This prevents a detailed assessment of migration effort and automation potential, introducing a risk of underestimating the work required. A manual assessment or deeper analysis would be needed for these metrics.
*   **KornShell Script Complexity:** While the scripts appear to be primarily orchestrators, they contain shell-specific constructs, parameter parsing, and calls to helper functions. Refactoring this into robust and maintainable Python code for Airflow will require careful translation and testing to ensure identical behavior, especially regarding error handling and parameter validation. The `starteSQLSkript` function's exact implementation is not provided, implying an assumption about its basic SQL execution capability.
*   **`gestern.ksh` script:** This script is sourced by `k_ausd_bp_ta_bcp_iccid.ksh` to set `p_datum_heute` and `p_datum_gestern`. Its logic needs to be explicitly translated into Python date calculations within the Airflow task.
*   **UC4 External Dependencies (`DW.HOLE_PFAD`, `DW.BERT_LESE_LOG`):** The UC4 XML includes these objects. Their exact purpose and content are unknown from the provided data. It is assumed they are UC4-specific inclusions for path definitions or logging and can be replaced by Airflow/GCP standard practices.
*   **Oracle `DWPA_UTIL_SKRIPT.runstatement`:** The SQL script uses this stored procedure for truncating the table. This procedure's specific implementation is not known, but the direct `TRUNCATE TABLE` equivalent in BigQuery is used in the proposed solution. Verification of exact behavior is needed.
*   **Legacy Data Types:** While the SQL migration tool attempted to infer data types, a definitive mapping requires access to the Oracle source schema definitions to ensure accurate BigQuery table schema creation.

## 8. Build Plan

The build plan focuses on sequential conversion and integration into the target GCP environment.

1.  **Migrate Oracle Tables to BigQuery:**
    *   **Action:** Create target BigQuery tables for `dwtk_meldungen`, `sof_ta_bpr_bcp`, `sof_ta_iccid_vertrag`, and `sof_ta_bcp_iccid`.
    *   **Tool/Language:** BigQuery DDL (SQL).
    *   **Notes:** This requires obtaining the exact schema from the source Oracle database.
    *   **Action:** Perform initial data load for historical data from Oracle to BigQuery.
    *   **Tool/Language:** Data Migration Service, custom ETL, or BigQuery Data Transfer Service.
    *   **Action:** Implement continuous data synchronization for active Oracle tables to BigQuery.
    *   **Tool/Language:** Datastream, custom CDC.

2.  **Translate SQL Transformation:**
    *   **Action:** Convert `d_ausd_bp_ta_bcp_iccid.sql` to BigQuery Standard SQL.
    *   **Tool/Language:** Manual translation based on `hql_sql_to_bqsql_design` output, or `bqsql_build` tool if available.
    *   **Output:** `d_ausd_bp_ta_bcp_iccid.bqsql` (BigQuery SQL file).
    *   **Notes:** This SQL will be directly executed by an Airflow BigQuery Operator.

3.  **Refactor KornShell Orchestration to Python:**
    *   **Action:** Consolidate and translate the logic from `r_ausd_bp_ta_bcp_iccid.ksh` and `k_ausd_bp_ta_bcp_iccid.ksh` into a Python module or function. This will include parameter parsing, environment setup, date calculations (replacing `gestern.ksh` logic), and calling the BigQuery SQL.
    *   **Tool/Language:** Python.
    *   **Output:** `bert_ausd_bp_ta_bcp_iccid_tasks.py` (Python module for Airflow).
    *   **Notes:** This Python code will form the core task logic within the Airflow DAG.

4.  **Develop Airflow DAG:**
    *   **Action:** Create an Airflow DAG file (`dw_bert_ausd_bp_ta_bcp_iccid_dag.py`) in Cloud Composer.
    *   **Tool/Language:** Python (using Airflow DAG and operators).
    *   **Output:** `dw_bert_ausd_bp_ta_bcp_iccid_dag.py`.
    *   **Components:**
        *   A `PythonOperator` task to execute the Python logic derived from the KornShell scripts, which will dynamically generate and execute the BigQuery SQL.
        *   Alternatively, a `BigQueryOperator` task to directly execute the static `d_ausd_bp_ta_bcp_iccid.bqsql` if parameterization is handled externally. Given the dynamic nature of parameters in the ksh scripts, a PythonOperator wrapping BigQuery execution is more likely.
    *   **Notes:** The DAG will implement the scheduling and dependencies as outlined in the UC4-to-Airflow design.

5.  **Testing and Validation:**
    *   **Action:** Unit and integration testing of the Python tasks and BigQuery SQL.
    *   **Action:** End-to-end testing of the Airflow DAG.
    *   **Tool/Language:** Python unit testing frameworks, Airflow testing utilities.

6.  **Deployment:**
    *   **Action:** Deploy the Airflow DAG and Python modules to Cloud Composer.
    *   **Action:** Schedule the DAG in Cloud Composer.
    *   **Tool/Language:** Cloud Composer environment, `gcloud` CLI.