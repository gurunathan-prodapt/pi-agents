# Migration Design — BERT_V_TA_DISC_ZUSGF

## 1. Purpose & Scope

The primary purpose of the `BERT_V_TA_DISC_ZUSGF` job is to consolidate and concatenate discount descriptions associated with contract data, ultimately populating or updating a target table named `SOF$TA_DISC_ZUSGF`. This aggregated discount information is crucial for downstream reporting and analytical processes. The job involves a complex data transformation logic, particularly around dynamically concatenating discount descriptions while adhering to a maximum length constraint per contract entry.

## 2. Source Inventory

| File Relative Path                                                                                             | Technology           | Summary                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 | Inferred Tier | Inferred Automation Bucket |
| :------------------------------------------------------------------------------------------------------------- | :------------------- | :-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :------------ | :------------------------- |
| `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/BERT/PRODUKTION/DW.BERT_STAMMDATEN_JP/DW.BERT_P_VERTRAG_JP/DW.BERT_AUSD_V_TA_DISC_ZUSGF.xml` | UC4/Automic          | This XML file defines a UC4 UNIX job that serves as the top-level orchestrator. It executes a KornShell wrapper script, sets job-specific variables like `DWH_JOB_KENNUNG`, and incorporates common UC4 objects for path handling and logging. This is the entry point for the entire business process.                                                                                                                                                                                                                                   | Medium        | Semi-Auto                  |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_disc_zusgf.ksh`                           | KornShell (wrapper)  | This is a wrapper KornShell script that is invoked by the UC4 job. Its responsibilities include setting up the execution environment, implementing a robust error handling framework, parsing command-line parameters, and managing common logging utilities. It subsequently calls the more specific control script `k_ausd_v_ta_disc_zusgf.ksh` to perform the core logic.                                                                                                                                                            | Medium        | Semi-Auto                  |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_disc_zusgf.ksh`                           | KornShell (control)  | This KornShell control script is responsible for the granular management of the job execution. It parses specific job-related parameters (e.g., job identifier, entry number), sets the target table name (`ta_disc_zusgf`), and prepares the environment for SQL*Plus execution. Crucially, it invokes the Oracle PL/SQL script `d_ausd_v_ta_disc_zusgf.sql` via SQL*Plus to perform the actual data transformation and loading.                                                                                                    | Medium        | Semi-Auto                  |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_disc_zusgf.sql`                           | Oracle PL/SQL (SQL*Plus script) | This Oracle SQL*Plus script contains the core data transformation logic. It defines custom Oracle object types (`SOF$TY_O_DISCOUNT`, `SOF$TY_T_DISCOUNT`) and a PL/SQL package (`sof$sp_discount_functions`) which houses a pipelined table function (`concat_discounts`). This function is designed to concatenate discount information from `sof$ta_discount`, grouping by contract IDs, and then populates the `SOF$TA_DISC_ZUSGF` table. It also reads `v_datum` from `isbert_schema.dwtk_meldungen`.                                                                         | Complex       | Redesign                   |

## 3. Target Architecture

The migration will target Google Cloud's BigQuery for data warehousing and transformations, with orchestration managed by Cloud Composer (Apache Airflow).

*   **Orchestration (UC4 replacement)**: The UC4 job `DW.BERT_AUSD_V_TA_DISC_ZUSGF` will be replaced by an Apache Airflow DAG hosted on Google Cloud Composer. This DAG will manage the end-to-end workflow, including task dependencies, scheduling, and error handling.
*   **Scripting (KornShell replacement)**: The KornShell scripts (`r_ausd_v_ta_disc_zusgf.ksh`, `k_ausd_v_ta_disc_zusgf.ksh`) will be re-implemented in Python. Common utility functions for parameter handling, logging, and environment setup will be refactored into reusable Python modules or directly integrated into Airflow tasks.
*   **Data Transformation (Oracle PL/SQL replacement)**:
    *   **Source and Target Tables**: The Oracle tables `isbert_schema.dwtk_meldungen`, `sof$ta_discount`, and `SOF$TA_DISC_ZUSGF` will be migrated to corresponding tables in BigQuery (e.g., `project_id.dataset_id.dwtk_meldungen`, `project_id.dataset_id.sof_ta_discount`, `project_id.dataset_id.sof_ta_disc_zusgf`).
    *   **PL/SQL Logic (`concat_discounts`)**: The complex logic of the pipelined table function in `d_ausd_v_ta_disc_zusgf.sql` will be re-engineered. Given its procedural nature (loops, `PIPE ROW`, dynamic string concatenation with length checks), it is best implemented using Python with a data processing library like Pandas or PySpark (depending on data volume) that can interact directly with BigQuery. This Python component will perform the grouping, concatenation, and loading into the target BigQuery table.
*   **Logging & Monitoring**: Standard Airflow logging will be used for job execution details, with metrics potentially pushed to Cloud Monitoring and logs to Cloud Logging.

## 4. Data Flow & Lineage

**Current Data Flow (Legacy System):**

1.  **UC4 Job (`DW.BERT_AUSD_V_TA_DISC_ZUSGF.xml`)**
    *   **INVOKES**: `r_ausd_v_ta_disc_zusgf.ksh`

2.  **KornShell Wrapper (`r_ausd_v_ta_disc_zusgf.ksh`)**
    *   **INVOKES**: `k_ausd_v_ta_disc_zusgf.ksh` (passing job parameters and log file details).
    *   **DEPENDS ON**: Shared utilities for environment setup, error handling, parameter parsing, and date functions.

3.  **KornShell Control (`k_ausd_v_ta_disc_zusgf.ksh`)**
    *   **INVOKES**: `d_ausd_v_ta_disc_zusgf.sql` via SQL*Plus.
    *   **DEPENDS ON**: Shared utilities for SQL*Plus interaction, error handling, and parameter parsing.

4.  **Oracle PL/SQL Script (`d_ausd_v_ta_disc_zusgf.sql`)**
    *   **READS FROM**:
        *   `isbert_schema.dwtk_meldungen` (Oracle table, for `v_datum`).
        *   `sof$ta_discount` (Oracle table, input for discount details).
    *   **TRANSFORMS**: Utilizes a custom PL/SQL package `sof$sp_discount_functions` and its pipelined table function `concat_discounts` to aggregate and concatenate discount descriptions.
    *   **WRITES TO**: `SOF$TA_DISC_ZUSGF` (Oracle table).

**Target Data Flow (Google Cloud Composer / BigQuery):**

1.  **Airflow DAG (`bert_v_ta_disc_zusgf_dag.py`)**
    *   **Task: `get_sysdate_equivalent_task` (PythonOperator / BigQueryOperator)**:
        *   **READS FROM**: `project_id.dataset_id.dwtk_meldungen` (BigQuery table).
        *   **OUTPUTS**: A processing date (equivalent to `v_datum`) via XCom.
    *   **Task: `transform_and_load_discount_data_task` (PythonOperator, possibly backed by Dataproc/PySpark for scale)**:
        *   **INPUTS**: Processing date from XCom (optional, if logic is self-contained).
        *   **READS FROM**: `project_id.dataset_id.sof_ta_discount` (BigQuery table).
        *   **TRANSFORMS**: Python code implements the aggregation and concatenation logic mirroring `concat_discounts`.
        *   **WRITES TO**: `project_id.dataset_id.sof_ta_disc_zusgf` (BigQuery table).

## 5. Transformation Logic

The core transformation is handled by the Oracle PL/SQL pipelined table function `sof$sp_discount_functions.concat_discounts` in `d_ausd_v_ta_disc_zusgf.sql`. This function processes discount records (`cntrct_id`, `cntrct_obj_version`, `rabatt`) and concatenates `rabatt` values for the same contract within a 500-character limit, producing new rows if the limit is exceeded.

**Legacy Logic Overview:**
The PL/SQL function uses a cursor to iterate through sorted discount records. It identifies changes in `cntrct_id` or `cntrct_obj_version` as group breaks, piping out the accumulated `rabatt_alle` for the previous group. Within a group, it appends `rabatt` values to `rabatt_alle` as long as the total length does not exceed 500 characters.

**Target Transformation Logic (Python / Pandas or PySpark):**

The re-implementation will be in a Python script invoked by an Airflow `PythonOperator`.

1.  **Data Ingestion**: Read data from `project_id.dataset_id.sof_ta_discount` and `project_id.dataset_id.dwtk_meldungen` into a Pandas DataFrame or PySpark DataFrame.
2.  **Date Extraction**: Replicate the `v_datum` logic by querying `dwtk_meldungen`.
3.  **Discount Concatenation**:
    *   Sort the DataFrame by `cntrct_id`, `cntrct_obj_version`, and `rabatt` to ensure consistent ordering for concatenation, mimicking the `ORDER p_discounts BY` clause.
    *   Implement a custom aggregation function (e.g., using `groupby().apply()`) that iterates through the `rabatt` values for each `(cntrct_id, cntrct_obj_version)` group.
    *   Within this function:
        *   Initialize an empty string for `rabatt_alle`.
        *   For each `rabatt` item in the group, append it to `rabatt_alle`, separated by `', '`.
        *   Crucially, check the `LENGTH()` of `rabatt_alle` after each append. If it exceeds 500 characters, store the current `rabatt_alle` as a completed entry for that contract, reset `rabatt_alle` to the current `rabatt` item, and continue. This replicates the `PIPE ROW` behavior for partial concatenations.
        *   Collect the final concatenated strings (potentially multiple per contract if the 500-char limit was hit multiple times) into a new DataFrame.
4.  **Target Load**: Truncate and load the resulting DataFrame into `project_id.dataset_id.sof_ta_disc_zusgf` in BigQuery. Ensure the `disc_vector_ty` column, which seems to be a direct pass-through, is handled correctly.

## 6. External Dependencies

*   **Oracle Database (`isbert_schema`, `sof$ta_discount`, `SOF$TA_DISC_ZUSGF`)**:
    *   **Replacement**: All relevant Oracle source and target tables will be migrated to BigQuery. A continuous data replication strategy (e.g., Google Cloud Datastream, Fivetran, or custom ETL) must be established to keep the BigQuery tables in sync with the operational Oracle system until Oracle is fully deprecated. The `v_carmen` DB-link, if solely defined in this script and not actively used for data retrieval within the main query, will not require direct replacement in BigQuery. If it is used elsewhere, those dependencies must be addressed.
*   **UC4/Automic Orchestration**:
    *   **Replacement**: The job scheduling and orchestration will be handled by Apache Airflow running on Google Cloud Composer.
*   **KornShell (Shared Utilities)**:
    *   **Replacement**: The common functions provided by `.dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, and `h_alis_sqlplus.ksh` will be replaced by native Python functionalities, Airflow's built-in features (e.g., `logging`, XComs for parameter passing), and specific BigQuery operators. Custom business logic within these scripts will be refactored into Python modules.

## 7. Unresolved / Risks

*   **Complex PL/SQL Pipelined Table Function**: The primary risk is accurately re-implementing the Oracle `PIPELINED` table function `concat_discounts` with its `ORDER BY`, `PARALLEL_ENABLE`, and internal looping/`PIPE ROW` logic in BigQuery SQL or Python. Special attention is required for the 500-character length constraint and ensuring functional equivalence, especially how multiple concatenated segments are handled for a single contract if the limit is repeatedly hit. This component is classified as `redesign` due to its complexity.
*   **Absence of `file_complexity` and `automation_rate` data**: The lack of pre-computed tier and automation bucket information for the source files means that the estimated effort and migration approach rely on manual analysis. This introduces a risk of underestimating the actual complexity and effort.
*   **Full Scope of `v_carmen` DB-Link**: While `v_carmen` is defined in the SQL script, its actual usage within the entire data ecosystem is unclear from the provided context. If `v_carmen` points to other critical Oracle sources that feed into `sof$ta_discount` or `dwtk_meldungen`, those upstream dependencies must also be identified and migrated or integrated.
*   **Refactoring Shared KornShell Utilities**: The extent of unique, non-generic logic within the numerous included `.ksh` helper scripts is not fully known. Refactoring these might uncover hidden complexities or interdependencies that could impact the Python re-implementation.
*   **Performance Considerations**: For very large datasets, the Python/Pandas implementation of the `concat_discounts` logic might face performance bottlenecks. In such cases, migration to PySpark running on Dataproc might be a more scalable alternative.
*   **Testing Coverage**: Comprehensive data validation and reconciliation between the legacy Oracle system and the new BigQuery solution will be critical to ensure data integrity and prevent discrepancies.

## 8. Build Plan

The migration will follow a phased approach, focusing on infrastructure setup, data migration, and then logic re-implementation.

1.  **Cloud Environment Setup (Weeks 1-2)**:
    *   Provision Google Cloud Project and configure BigQuery datasets (`project_id.dataset_id`).
    *   Set up Google Cloud Composer environment for Airflow DAGs.
    *   Configure IAM roles and service accounts for BigQuery access and Composer operations.
    *   *Deliverables*: Configured GCP project, empty BigQuery datasets, running Cloud Composer environment.
    *   *Language*: GCP CLI/Console, Terraform (IaC).

2.  **Oracle Table Migration to BigQuery (Weeks 2-4)**:
    *   Create DDL for `dwtk_meldungen`, `sof_ta_discount`, and `sof_ta_disc_zusgf` tables in BigQuery, mapping Oracle data types to BigQuery data types.
    *   Perform initial historical data load from Oracle to BigQuery for these tables.
    *   Establish a data replication mechanism (e.g., Datastream for CDC) to keep BigQuery tables in sync with Oracle.
    *   *Deliverables*: BigQuery DDLs, populated BigQuery tables, active data replication pipeline.
    *   *Language*: SQL, Data Migration Tools (e.g., Datastream configuration).

3.  **Transformation Logic Re-implementation (Weeks 4-8)**:
    *   Develop `transform_discount_data.py`: A Python script containing the re-engineered `concat_discounts` logic. This script will read from `sof_ta_discount` and `dwtk_meldungen` in BigQuery, perform the transformation, and write to `sof_ta_disc_zusgf` in BigQuery.
    *   Develop unit tests for the `transform_discount_data.py` script to ensure functional equivalence with the Oracle PL/SQL.
    *   *Deliverables*: `transform_discount_data.py` script, unit tests.
    *   *Language*: Python (Pandas/PySpark).

4.  **Airflow DAG Development (Weeks 6-9)**:
    *   Create `bert_v_ta_disc_zusgf_dag.py`: An Airflow DAG definition that orchestrates the data flow.
        *   A `PythonOperator` (`get_sysdate_equivalent_task`) to extract `v_datum` from `dwtk_meldungen`.
        *   A `PythonOperator` (`transform_and_load_discount_data_task`) to execute `transform_discount_data.py`, potentially using XCom to pass `v_datum` as a parameter.
    *   Integrate Airflow's native logging and error handling.
    *   *Deliverables*: `bert_v_ta_disc_zusgf_dag.py` deployed to Cloud Composer.
    *   *Language*: Python (Airflow).

5.  **Refactor Common Utilities (As needed, parallel to 3 & 4)**:
    *   Analyze and convert necessary functionalities from KornShell helper scripts (`.dw_init`, `f_alis_msgerr.ksh`, etc.) into reusable Python modules.
    *   *Deliverables*: Python utility modules.
    *   *Language*: Python.

6.  **Testing and Validation (Weeks 9-12)**:
    *   Execute the Airflow DAG in a staging environment.
    *   Perform comprehensive data reconciliation between the output of the legacy Oracle job and the new BigQuery job.
    *   Conduct performance testing and optimization.
    *   *Deliverables*: Test reports, validated BigQuery data, performance benchmarks.
    *   *Language*: SQL (for data validation queries), Python (for test automation).

7.  **Production Deployment (Week 12+)**:
    *   Deploy the validated Airflow DAG and Python scripts to the production Cloud Composer environment.
    *   Decommission the legacy UC4 job and related KornShell/Oracle components.
    *   *Deliverables*: Live BigQuery solution, retired legacy components.