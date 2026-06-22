# Migration Design — DW.BERT_AUSD_BP_TA_APN_VERTRAG

## 1. Purpose & Scope
This job, `DW.BERT_AUSD_BP_TA_APN_VERTRAG`, is responsible for the preparation of instantiated base products (APN and contract reference data) for the BERT process. It involves extracting and aggregating contract cache data from the legacy DWH, handling date parameters, and orchestrating a core SQL script to process and load the data. The primary goal of this migration is to re-platform the existing UC4-orchestrated KornShell and Oracle PL/SQL solution to Google Cloud Platform, utilizing Airflow for orchestration and BigQuery for data processing.

## 2. Source Inventory

| File Path | Technology | Tier | Automation Bucket | Summary |
|---|---|---|---|---|
| `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/BERT/PRODUKTION/DW.BERT_STAMMDATEN_JP/DW.BERT_P_BASISPRODUKT_JP/DW.BERT_AUSD_BP_TA_APN_VERTRAG.xml` | UC4/Automic | medium | semi_auto | This UC4 JOBS_UNIX object defines a job named DW.BERT_AUSD_BP_TA_APN_VERTRAG, responsible for the preparation of instantiated base products by executing a KornShell script. |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_apn_vertrag.ksh` | KornShell | medium | semi_auto | This ksh script prepares selected basic products for BERT by extracting contract cache data from DWH, handling date parameters, and orchestrating a core script (`k_ausd_bp_ta_apn_vertrag.ksh`). It handles environment setup, parameter parsing, and error reporting. |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_apn_vertrag.ksh` | KornShell | medium | semi_auto | This KornShell script acts as a control script, parsing parameters, setting up the environment, performing validation checks, and orchestrating the execution of a SQL script (`d_ausd_bp_ta_apn_vertrag.sql`) to process data. It is invoked by `r_ausd_bp_ta_apn_vertrag.ksh`. |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_bp_ta_apn_vertrag.sql` | Oracle PL/SQL | complex | manual | This Oracle PL/SQL script processes APN (Access Point Name) and contract reference data from a source table (`sof$ta_bpr_apn`), aggregates them, and inserts the results into a target table (`sof$ta_apn_vertrag`). It uses a cursor-based loop for row-by-row processing and references `isbert_schema.dwtk_meldungen` for metadata. |

## 3. Target Architecture
The migrated solution will leverage Google Cloud Platform (GCP) services:
*   **Orchestration:** Airflow (managed by Cloud Composer) will replace UC4 for scheduling and managing the end-to-end workflow.
*   **Data Processing:** BigQuery will be the primary data warehouse, handling all SQL-based transformations. The Oracle PL/SQL script will be converted to BigQuery SQL, specifically as a BigQuery Stored Procedure. The shell script logic will be re-implemented as a wrapper BigQuery Stored Procedure.
*   **Data Storage:** All source tables (`isbert_schema.dwtk_meldungen`, `sof$ta_bpr_apn`) and target tables (`sof$ta_apn_vertrag`) will reside in BigQuery datasets.
*   **Logging & Monitoring:** Airflow's native logging and monitoring capabilities, integrated with Cloud Logging and Cloud Monitoring, will replace the custom shell-based logging mechanisms (`f_alis_msgerr.ksh`, `dwtk_meldungen`).

## 4. Data Flow & Lineage
The original execution flow is:
`UC4 Job (DW.BERT_AUSD_BP_TA_APN_VERTRAG.xml)`
  `-> KornShell Script (r_ausd_bp_ta_apn_vertrag.ksh)`
    `-> KornShell Script (k_ausd_bp_ta_apn_vertrag.ksh)`
      `-> Oracle PL/SQL Script (d_ausd_bp_ta_apn_vertrag.sql)`

The Oracle PL/SQL script `d_ausd_bp_ta_apn_vertrag.sql` reads from `sof$ta_bpr_apn` and `isbert_schema.dwtk_meldungen`, aggregates data, and writes to `sof$ta_apn_vertrag`.

The target data flow will be:
`Airflow DAG (dw_bert_ausd_bp_ta_apn_vertrag)`
  `-> BigQuery Stored Procedure (e.g., project.dataset.sp_r_k_ausd_bp_ta_apn_vertrag)` (combining `r_` and `k_` ksh logic)
    `-> BigQuery Stored Procedure (project.dataset.sp_d_ausd_bp_ta_apn_vertrag)` (core SQL logic)

The `sp_d_ausd_bp_ta_apn_vertrag` procedure will:
*   Read from `isbert_schema.dwtk_meldungen` (BigQuery table)
*   Read from `sof$ta_bpr_apn` (BigQuery table)
*   Write to `sof$ta_apn_vertrag` (BigQuery table)

## 5. Transformation Logic

### 5.1. `DW.BERT_AUSD_BP_TA_APN_VERTRAG.xml` (UC4 Job)
*   **Original:** A UC4 `JOBS_UNIX` object that executes the KornShell script `r_ausd_bp_ta_apn_vertrag.ksh`. It includes variable settings (`DWH_JOB_KENNUNG`) and sourcing of common include files (`DW.HOLE_PFAD`, `.dw_init`, `DW.BERT_LESE_LOG`).
*   **Target (Airflow DAG):** The UC4 job will be transformed into an Airflow DAG named `dw_bert_ausd_bp_ta_apn_vertrag`. This DAG will schedule a single task that executes a BigQuery Stored Procedure, encapsulating the logic from `r_ausd_bp_ta_apn_vertrag.ksh` and `k_ausd_bp_ta_apn_vertrag.ksh`.

### 5.2. `r_ausd_bp_ta_apn_vertrag.ksh` and `k_ausd_bp_ta_apn_vertrag.ksh` (KornShell Scripts)
*   **Original:** These scripts handle parameter parsing (`getopts`), environment setup (`. $HOME/.dw_init`), date computations (`gestern.ksh`), parameter validation, error handling (`f_alis_msgerr.ksh`), and orchestration of the `d_ausd_bp_ta_apn_vertrag.sql` via `starteSQLSkript`. `r_` calls `k_`, which then calls the SQL.
*   **Target (BigQuery Stored Procedure):** The combined orchestration logic of both KornShell scripts will be re-implemented as a single BigQuery Stored Procedure, e.g., `project.dataset.sp_r_k_ausd_bp_ta_apn_vertrag`. This procedure will:
    *   Accept parameters equivalent to the shell script's command-line arguments.
    *   Perform parameter validation and error handling using BigQuery's `IF`, `DECLARE`, `SIGNAL`, and logging to a BigQuery error log table.
    *   Derive 'today' and 'yesterday' dates using BigQuery date functions (`CURRENT_DATE()`, `DATE_SUB`).
    *   Execute the core data transformation BigQuery Stored Procedure (`project.dataset.sp_d_ausd_bp_ta_apn_vertrag`) using `EXECUTE IMMEDIATE`.
    *   Record job metadata (e.g., record counts) in a BigQuery job tracking table.
    *   Remove commented-out file processing (sed/sort/join) as it is not active.

### 5.3. `d_ausd_bp_ta_apn_vertrag.sql` (Oracle PL/SQL)
*   **Original:** This script is a procedural Oracle PL/SQL block. It selects the maximum `timecreated` from `isbert_schema.dwtk_meldungen`, truncates `sof$ta_apn_vertrag`, and then iterates through `sof$ta_bpr_apn` to aggregate and insert data into `sof$ta_apn_vertrag`. String concatenation is done procedurally within a loop.
*   **Target (BigQuery Stored Procedure):** This script will be converted into a BigQuery Stored Procedure named `project.dataset.sp_d_ausd_bp_ta_apn_vertrag`.
    *   The `DECLARE` and `SET` statements will be used for variables.
    *   The `TRUNCATE TABLE` statement for `sof$ta_apn_vertrag` remains.
    *   The procedural `FOR` loop for aggregation will be replaced by a single `INSERT INTO ... SELECT` statement utilizing BigQuery's `STRING_AGG` function for efficient string concatenation.
    *   Oracle-specific functions like `TO_CHAR` and `NVL` will be replaced with `FORMAT_TIMESTAMP` and `COALESCE` respectively.
    *   The `isbert_schema.DWPA_UTIL_SKRIPT.runstatement` and `dbms_output.put_line` calls will be removed or replaced with BigQuery logging mechanisms.

## 6. External Dependencies
*   **Oracle Database:** The current source and target tables (`isbert_schema.dwtk_meldungen`, `sof$ta_bpr_apn`, `sof$ta_apn_vertrag`) reside in an Oracle database.
    *   **Replacement:** These tables will be migrated to BigQuery. Initial data loading will be performed using a one-time migration, and ongoing data synchronization (if required) will be established using appropriate GCP data integration tools (e.g., Datastream, Fivetran, or custom CDC).
*   **`DWHDWH2P` (OS Host):** The UC4 job executes on this host.
    *   **Replacement:** The job execution will be orchestrated by Airflow on Cloud Composer, running on Google Kubernetes Engine, and the BigQuery procedures will execute within BigQuery's serverless environment.
*   **`DW.UNIX.ISBERT` (Login):** UC4 uses this login.
    *   **Replacement:** Airflow tasks will run under a specified GCP Service Account with appropriate IAM roles to access BigQuery and other GCP resources.
*   **Helper Shell Scripts (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`, `gestern.ksh`):** These are shell scripts providing common utilities.
    *   **Replacement:** The functionalities provided by these scripts (environment setup, parameter parsing, date utilities, error handling, SQL execution wrapper) will be absorbed into the Airflow DAG's Python code and the BigQuery Stored Procedures. Common utilities can be re-implemented as Python functions or BigQuery UDFs/routines if they are generic and reusable.
*   **`DW.HOLE_PFAD`, `DW.BERT_LESE_LOG` (UC4 Includes):** These are specific to the UC4 environment.
    *   **Replacement:** These will be replaced by Airflow DAG structure for modularity and BigQuery's native logging capabilities.

## 7. Unresolved / Risks
*   **Parameter `p_wiederanlaufWert`:** The shell script `k_ausd_bp_ta_apn_vertrag.ksh` initializes this parameter but its usage in the original code is not apparent from the provided script. It's assumed to be part of a restart/recovery mechanism. This needs to be clarified and implemented correctly in the BigQuery Stored Procedure, possibly by managing restart logic within the orchestrating Airflow DAG or the BigQuery procedure itself.
*   **`isbert_schema.DWPA_UTIL_SKRIPT.runstatement`:** This is a procedural utility call in the Oracle PL/SQL. Its exact functionality needs to be understood and re-implemented in BigQuery SQL if it has critical side effects or logic beyond a simple `TRUNCATE`.
*   **Unresolved References:** Several `UNRESOLVED` nodes were identified in the `lineage_unresolved` query (e.g., `CALENDAR:DW.KALENDER`, `COMMAND:SQLPLUS`, various environment variables and file paths). While many of these are implicit in the shell environment or database-resident objects, their migration requires careful consideration to ensure all dependencies are met in the new environment.
*   **Error Logging (`DWMSG_MeldeFehler`):** The exact implementation of `f_alis_msgerr.ksh` and its interaction with `DWMSG_MeldeFehler` needs to be fully understood to replicate the error reporting and alerting in GCP (e.g., to Cloud Logging/Monitoring, email notifications).
*   **Performance of `STRING_AGG`:** While `STRING_AGG` replaces the procedural loop for string concatenation, the performance on very large datasets should be monitored and optimized if necessary.

## 8. Build Plan

1.  **Migrate Source and Target Tables to BigQuery:**
    *   Create BigQuery datasets: `project.isbert_schema`, `project.sof`.
    *   Create BigQuery tables: `project.isbert_schema.dwtk_meldungen`, `project.sof.ta_bpr_apn`, `project.sof.ta_apn_vertrag`.
    *   Populate initial data into these BigQuery tables.
    *   (Language: DDL for BigQuery, Data Migration Tools)

2.  **Develop BigQuery Stored Procedure for Core SQL Logic (`sp_d_ausd_bp_ta_apn_vertrag`):**
    *   Translate `d_ausd_bp_ta_apn_vertrag.sql` to BigQuery SQL, implementing `STRING_AGG` for concatenation and BigQuery-native functions.
    *   Ensure proper handling of date formats and string lengths.
    *   (Language: BigQuery SQL)

3.  **Develop BigQuery Stored Procedure for Orchestration Logic (`sp_r_k_ausd_bp_ta_apn_vertrag`):**
    *   Combine the parameter handling, validation, date logic, and error reporting from `r_ausd_bp_ta_apn_vertrag.ksh` and `k_ausd_bp_ta_apn_vertrag.ksh` into a single BigQuery Stored Procedure.
    *   Integrate the call to `sp_d_ausd_bp_ta_apn_vertrag`.
    *   Implement logging to a BigQuery error table and a job tracking table.
    *   (Language: BigQuery SQL)

4.  **Develop Airflow DAG (`dw_bert_ausd_bp_ta_apn_vertrag`):**
    *   Create an Airflow DAG to schedule and manage the execution.
    *   The DAG will have a single `BigQueryOperator` (or similar) task that calls the orchestrating BigQuery Stored Procedure (`sp_r_k_ausd_bp_ta_apn_vertrag`).
    *   Define necessary DAG properties (schedule, retries, etc.).
    *   Configure GCP service account and project details as parameters.
    *   (Language: Python for Airflow DAG)

5.  **Re-implement Helper Utilities (if generic and reusable):**
    *   Any highly reusable logic from `.dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `gestern.ksh` can be refactored into Python modules (for Airflow) or BigQuery UDFs/routines.
    *   (Language: Python, BigQuery SQL)

6.  **Setup Logging, Monitoring, and Alerting:**
    *   Configure Cloud Logging and Cloud Monitoring for the Airflow DAG and BigQuery job executions.
    *   Establish alerting mechanisms for job failures.
    *   (Language: GCP Configuration/Terraform)