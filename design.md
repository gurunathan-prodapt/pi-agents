# Migration Design — DW.BERT_AUSD_BP_TA_ICCID_VERTRAG

## 1. Purpose & Scope
The purpose of the `DW.BERT_AUSD_BP_TA_ICCID_VERTRAG` job is to prepare and make available aggregated ICCID (SIM card ID) data for various contract IDs. This job initially truncates a target table (`sof$ta_iccid_vertrag`), then populates it by aggregating and pivoting ICCID information from a source table (`sof$ta_iccid_einzeln`). The transformation involves grouping by contract ID and transforming multiple ICCID-related attributes (ICCID, IMSI_MCC, IMSI_MNC, IMSI_HLR, IMSI_SI, STATUS, VALID_TO, E_ID, CARD_TYPE_NAME) from rows into columns (e.g., TN_ICCID, TC_ICCID, MS1_ICCID up to MS10_ICCID). The job also includes parameter parsing, date validation, and robust error handling.

## 2. Source Inventory
This job consists of a multi-layered orchestration and data transformation logic spread across several files:

*   **`DW.BERT_AUSD_BP_TA_ICCID_VERTRAG.xml`**
    *   **Technology:** UC4/Automic Job Definition (UNIX type)
    *   **Description:** Top-level orchestrator. Executes a KornShell script.
    *   **Tier:** Medium
    *   **Automation Bucket:** Semi-auto
*   **`r_ausd_bp_ta_iccid_vertrag.ksh`**
    *   **Technology:** KornShell Script
    *   **Description:** Orchestration script. Parses parameters (`Stichtag`, `Wiederanlaufwert`) and executes `k_ausd_bp_ta_iccid_vertrag.ksh`. Includes comprehensive error handling and logging.
    *   **Relative Path:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_iccid_vertrag.ksh`
    *   **Tier:** Medium
    *   **Automation Bucket:** Semi-auto
*   **`k_ausd_bp_ta_iccid_vertrag.ksh`**
    *   **Technology:** KornShell Script
    *   **Description:** Control script. Parses parameters, performs date validation, and executes the core Oracle SQL script (`d_ausd_bp_ta_iccid_vertrag.sql`) via a wrapper. Contains commented-out `sed`, `sort`, `join` commands for shell-based data reformatting.
    *   **Relative Path:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_iccid_vertrag.ksh`
    *   **Tier:** Medium
    *   **Automation Bucket:** Semi-auto
*   **`d_ausd_bp_ta_iccid_vertrag.sql`**
    *   **Technology:** Oracle SQL (SQL*Plus script)
    *   **Description:** Core data transformation logic. Truncates `sof$ta_iccid_vertrag` and populates it from `sof$ta_iccid_einzeln` through aggregation (MAX) and pivoting based on `cntrct_id`, creating multiple columns for ICCID data.
    *   **Relative Path:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_bp_ta_iccid_vertrag.sql`
    *   **Tier:** Complex (`loc-escalated`)
    *   **Automation Bucket:** Retire (Suggests a need for significant redesign or re-evaluation for direct migration)

## 3. Target Architecture
The entire job will be migrated to Google Cloud Platform, leveraging the following components:

*   **Google Cloud Composer (Apache Airflow):** To replace the UC4 and KornShell orchestration logic. A single Airflow DAG will manage the end-to-end workflow, including parameter passing, task dependencies, and error handling.
*   **Google BigQuery:** To host the transformed data. Source and target Oracle tables will be migrated to BigQuery tables. The Oracle SQL transformation will be rewritten as BigQuery SQL.
*   **Dataform (optional, for advanced SQL management):** Can be used to manage the BigQuery SQL transformation code, enabling version control, testing, and dependency management for the data pipeline.

## 4. Data Flow & Lineage

The migration will follow a sequence of operations within a single Airflow DAG:

1.  **Airflow DAG Start:** The DAG initiates, accepting parameters `p_stichtag` (key date) and `p_wiederanlaufWert` (restart value), equivalent to the legacy UC4 and `r_ausd_bp_ta_iccid_vertrag.ksh` inputs.
2.  **Date Validation & Variable Preparation:** Airflow tasks will replicate the date validation and environment variable setup performed by the `r_` and `k_` KornShell scripts. This includes sourcing `.dw_init` and utility scripts, re-implementing their logic in Python where necessary.
3.  **Target Table Truncation:** A BigQuery operator in Airflow will execute a `TRUNCATE TABLE` DDL statement on the target BigQuery table (equivalent to `sof$ta_iccid_vertrag`). This replaces the Oracle stored procedure call (`isbert_schema.DWPA_UTIL_SKRIPT.runstatement`).
4.  **Data Transformation (BigQuery SQL):** A BigQuery operator will execute the translated SQL logic from `d_ausd_bp_ta_iccid_vertrag.sql`. This SQL will:
    *   Read from the BigQuery equivalent of `sof$ta_iccid_einzeln`.
    *   Read from the BigQuery equivalent of `isbert_schema.dwtk_meldungen` (if required for dynamic variable substitution logic, this part will be handled within the Airflow task as Python logic to dynamically build the query).
    *   Perform the aggregation (MAX) and pivoting based on `cntrct_id` to populate the target BigQuery table (equivalent to `sof$ta_iccid_vertrag`).
5.  **Logging & Status Update:** Airflow's native logging capabilities will replace the shell script's custom logging mechanisms (`DWMSG_MeldeFehler`, `DWMSG_SetzeStatusOK`, etc.). Job status updates to `PoolBasisprodukt` (if still required) will be implemented as BigQuery `INSERT` or `UPDATE` statements.
6.  **End of DAG:** The DAG completes upon successful execution of all tasks.

**Lineage:**
*   **Source Tables (Oracle):** `sof$ta_iccid_einzeln`, `isbert_schema.dwtk_meldungen`
*   **Target Table (Oracle):** `sof$ta_iccid_vertrag`
*   **Intermediate Components:**
    *   UC4 Job (`DW.BERT_AUSD_BP_TA_ICCID_VERTRAG.xml`)
    *   KornShell Scripts (`r_ausd_bp_ta_iccid_vertrag.ksh`, `k_ausd_bp_ta_iccid_vertrag.ksh`)
    *   Utility Scripts (e.g., `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`, `h_alis_sqlplus.ksh`, `gestern.ksh`)
*   **Target Tables (BigQuery):** `gcp_project.dataset.sof_ta_iccid_einzeln`, `gcp_project.dataset.dwtk_meldungen`, `gcp_project.dataset.sof_ta_iccid_vertrag`
*   **Target Orchestration:** Airflow DAG (e.g., `dags/bert_ausd_bp_ta_iccid_vertrag.py`)

## 5. Transformation Logic

**UC4 Job & KornShell Orchestration (`DW.BERT_AUSD_BP_TA_ICCID_VERTRAG.xml`, `r_ausd_bp_ta_iccid_vertrag.ksh`, `k_ausd_bp_ta_iccid_vertrag.ksh`):**
*   **Legacy:** Layered shell script execution with parameter passing, environment sourcing, and custom error handling via `trap` commands and custom logging functions.
*   **Target (Airflow DAG):**
    *   A Python-based Airflow DAG will encapsulate the entire workflow.
    *   Parameters (`Stichtag`, `Wiederanlaufwert`) will be passed as Airflow DAG parameters or XComs.
    *   Environment setup (`. $HOME/.dw_init`) will be replicated using Python environment variables or Airflow Variables.
    *   Date validation (`DWDate_Datum_Check`) will be implemented as a Python function within an Airflow task.
    *   The error handling (`trap` commands, `DWMSG_MeldeFehler`) will be replaced by Airflow's built-in error handling, retry mechanisms, and robust logging.
    *   The commented-out `sed`, `sort`, `join` operations in `k_ausd_bp_ta_iccid_vertrag.ksh` suggest shell-based data manipulation. If these were ever active, they would need re-evaluation. Assuming they are inactive, they will not be migrated. If they become active, they would be re-implemented using PySpark on Dataproc or Dataflow.

**Oracle SQL Transformation (`d_ausd_bp_ta_iccid_vertrag.sql`):**
*   **Legacy (Oracle SQL):**
    *   `DEFINE` and `COLUMN new_value` for dynamic SQL*Plus variable substitution, deriving `v_datum` from `isbert_schema.dwtk_meldungen`.
    *   `isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_iccid_vertrag REUSE STORAGE');` for truncating the target.
    *   `INSERT INTO sof$ta_iccid_vertrag (...) SELECT ... FROM sof$ta_iccid_einzeln rp GROUP BY cntrct_id;` with `MAX()` aggregates to pivot ICCID attributes.
    *   Oracle hints `/*+ full(rp) parallel(rp,4) */`.
*   **Target (BigQuery SQL):**
    *   **Variable Substitution:** The logic to derive `v_datum` from `dwtk_meldungen` will be handled in Python within the Airflow DAG. The derived date will then be passed as a variable to the BigQuery SQL query using Jinja templating in the BigQuery operator.
    *   **Truncate:** Replaced by a `TRUNCATE TABLE \`gcp_project.dataset.sof_ta_iccid_vertrag\`` DDL statement executed by a BigQuery operator.
    *   **Data Load & Transformation:** The `INSERT...SELECT` will be translated to BigQuery SQL. The `MAX()` aggregation and pivoting will be re-implemented using BigQuery's `GROUP BY` and either conditional `MAX()` statements or the `PIVOT` clause for clarity and maintainability.
    *   **Performance:** Oracle hints will be removed. BigQuery's columnar storage and automatic parallelism will handle performance. Partitioning and clustering strategies will be applied to BigQuery tables for optimal query performance.

## 6. External Dependencies

*   **Oracle Database (source):** `sof$ta_iccid_einzeln`, `sof$ta_iccid_vertrag`, `isbert_schema.dwtk_meldungen`
    *   **Replacement:** These will be migrated to equivalent tables in BigQuery (e.g., `gcp_project.dataset.sof_ta_iccid_einzeln`, `gcp_project.dataset.sof_ta_iccid_vertrag`, `gcp_project.dataset.dwtk_meldungen`). Data will be ingested into BigQuery using appropriate data migration tools (e.g., Cloud Data Fusion, database migration service, or custom scripts).
*   **Oracle Stored Procedure:** `isbert_schema.DWPA_UTIL_SKRIPT.runstatement`
    *   **Replacement:** The `TRUNCATE TABLE` functionality will be replaced by a direct BigQuery DDL statement executed via an Airflow BigQuery operator. Any other complex logic within this stored procedure would need to be reimplemented in BigQuery SQL or Python.
*   **UNIX Host (`DWHDWH2P`), Login (`DW.UNIX.ISBERT`), Queue (`CLIENT_QUEUE`):**
    *   **Replacement:** These are specific to the legacy UC4 and UNIX environment. The Airflow DAG running on Cloud Composer will manage execution, credentials, and resource allocation natively within GCP.
*   **Shared Shell Utilities:** (e.g., `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`, `h_alis_sqlplus.ksh`, `gestern.ksh`, `.dw_init`)
    *   **Replacement:** The functionalities of these scripts (logging, date manipulation, parameter parsing, SQL execution wrappers) will be re-implemented in Python as Airflow utility functions, custom operators, or Python modules within the Airflow environment.

## 7. Unresolved / Risks

*   **"Retire" Migration Bucket for SQL:** The SQL script is categorized as "Retire," despite having "REPLATFORM" and "REFACTOR" transformation hints. This suggests the complexity (`loc-escalated`) might warrant a deeper architectural review or a complete redesign if a direct BigQuery translation is overly burdensome or inefficient. The current design assumes direct translation, but this "Retire" flag is a risk that needs careful consideration and potentially a more in-depth assessment during implementation.
*   **Commented-out Shell Scripting:** The `k_ausd_bp_ta_iccid_vertrag.ksh` contains significant commented-out `sed`, `sort`, `join` commands. It's assumed these are inactive. If they are, or become, active logic, their functionality would need to be re-implemented in a BigQuery-compatible way, potentially using Dataflow or PySpark for complex text processing if BigQuery is not suitable.
*   **Schema Evolution:** The numerous `MSx_ICCID` fields (up to MS10) and `_E_ID`, `_CARD_TYPE_NAME` fields indicate a potential for schema evolution with additional MultiSIM slave cards. The BigQuery design should account for this flexibility, possibly using `ARRAY<STRUCT>` or a more dynamic approach if the number of `MSx` fields is not fixed.
*   **`v_carmen = "@pcrs1"`:** The `DEFINE v_carmen = "@pcrs1"` in the SQL script suggests a database link or connection string. This external connection needs to be identified and handled during migration. If `pcrs1` points to another Oracle database, data replication to BigQuery will be required.
*   **`dwtk_meldungen` usage:** The `SELECT NVL(TO_CHAR(MAX(m.timecreated),'YYYYMMDD'),'19000101') AS s_datum FROM isbert_schema.dwtk_meldungen m WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE';` indicates a dependency on a "messages" or "metadata" table. This table (`dwtk_meldungen`) needs to be migrated to BigQuery, and its data population mechanism understood.

## 8. Build Plan

The migration will be executed in a phased approach, focusing on modularity and testability.

1.  **BigQuery Schema Definition:**
    *   Define target BigQuery schemas for `sof_ta_iccid_einzeln`, `sof_ta_iccid_vertrag`, `dwtk_meldungen`, and `PoolBasisprodukt` based on the Oracle source tables.
    *   Consider partitioning and clustering keys for performance optimization.
    *   **Language:** SQL (BigQuery DDL)

2.  **Data Ingestion:**
    *   Establish a data pipeline to ingest data from the source Oracle tables (`sof$ta_iccid_einzeln`, `isbert_schema.dwtk_meldungen`) into their respective BigQuery staging tables.
    *   **Tool:** Cloud Data Fusion, Database Migration Service, or custom Python scripts.
    *   **Language:** Python/SQL

3.  **Core SQL Transformation (BigQuery):**
    *   Translate `d_ausd_bp_ta_iccid_vertrag.sql` into BigQuery SQL, handling the `TRUNCATE` and `INSERT...SELECT` with `GROUP BY` and `MAX()` for pivoting.
    *   Address the dynamic date variable substitution (`v_datum`) by accepting a date parameter.
    *   **Language:** SQL (BigQuery DML/DDL)

4.  **Airflow DAG Development:**
    *   Create a new Airflow DAG (e.g., `bert_ausd_bp_ta_iccid_vertrag_dag.py`) in Cloud Composer.
    *   **Task 1: Parameter Parsing & Validation:** PythonOperator to parse and validate input parameters (`p_stichtag`, `p_wiederanlaufWert`), replicating `r_` script's logic.
    *   **Task 2: Dynamic Variable Preparation:** PythonOperator to query `dwtk_meldungen` in BigQuery and determine the `s_datum` value, replicating the SQL*Plus `DEFINE` logic.
    *   **Task 3: Truncate Target Table:** BigQueryOperator to execute the `TRUNCATE TABLE` DDL on `gcp_project.dataset.sof_ta_iccid_vertrag`.
    *   **Task 4: Execute Main Transformation:** BigQueryOperator to execute the BigQuery SQL transformation (from Step 3), passing dynamically prepared parameters.
    *   **Task 5: Logging & Status Update:** PythonOperator or BigQueryOperator to update job status in `gcp_project.dataset.PoolBasisprodukt` (if required) and log job metrics.
    *   Replicate `r_` and `k_` script utility functions (error handling, date functions) as Python functions/modules integrated into the Airflow tasks.
    *   **Language:** Python (Airflow DAG)

5.  **Testing & Validation:**
    *   Develop unit and integration tests for the BigQuery SQL transformation.
    *   Test the Airflow DAG end-to-end with various parameters and edge cases.
    *   Validate data correctness by comparing outputs with the legacy system.
    *   **Language:** SQL, Python

6.  **Deployment & Scheduling:**
    *   Deploy the Airflow DAG to Cloud Composer.
    *   Configure scheduling similar to the legacy UC4 job.
    *   **Tool:** Cloud Composer UI/CLI

7.  **Decommissioning (if "Retire" bucket is fully embraced):**
    *   If the "Retire" bucket for the SQL script means complete redesign or decommissioning, a separate, more detailed analysis and design phase would be initiated for the transformation logic. This would focus on defining new business requirements and rebuilding the transformation from scratch in BigQuery. This build plan assumes a direct migration for the SQL as the initial step, acknowledging the "Retire" flag as a potential future redesign trigger.