# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_inv_def.ksh

## 1. Purpose & Scope
This job, identified by `run_id: 5af228f1-3847-4cc6-9310-ed82ed19407c`, is responsible for the data synchronization process of contract data into the `SOF$TA_INV_DEF` table. The primary script `r_ausd_v_ta_inv_def.ksh` acts as a wrapper, orchestrating the environment setup, parameter parsing, error handling, and invocation of a core data preparation script. This core script, `k_ausd_v_ta_inv_def.ksh`, in turn executes an Oracle SQL*Plus script, `d_ausd_v_ta_inv_def.sql`, which performs the actual data manipulation. The scope of this migration is to re-platform this entire workflow from its current KornShell and Oracle SQL*Plus environment to Google Cloud Platform, utilizing BigQuery for data storage and transformation, Cloud Composer (Apache Airflow) for orchestration, and Dataform for SQL pipeline management.

## 2. Source Inventory

| File Path                                                                   | Technology       | Role                   | Migration Hint                                   |
| :-------------------------------------------------------------------------- | :--------------- | :--------------------- | :----------------------------------------------- |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_inv_def.ksh` | KornShell        | Main Orchestrator      | Replatform to Cloud Composer (Airflow)           |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_inv_def.ksh` | KornShell        | Sub-Orchestrator/Wrapper | Replatform to Cloud Composer (Airflow)           |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_inv_def.sql` | Oracle SQL*Plus  | ETL Script             | Refactor to BigQuery SQL and Dataform, orchestrated by Cloud Composer |

*Note: `file_complexity` and `automation_rate` details for these files were not found in the database.*

## 3. Target Architecture
The migrated solution will reside on Google Cloud Platform and leverage the following services:

*   **Google Cloud Composer (Apache Airflow)**: Will serve as the primary orchestration engine. The existing KornShell scripts (`r_ausd_v_ta_inv_def.ksh`, `k_ausd_v_ta_inv_def.ksh`) will be converted into Python-based Airflow DAGs. These DAGs will manage the execution flow, error handling, logging, and parameter passing.
*   **Google BigQuery**: Will be the target data warehouse for `SOF$TA_INV_DEF` and all source tables involved in the transformation. Data will be stored in appropriate datasets and tables.
*   **Google Dataform**: Will manage the SQL transformations (`d_ausd_v_ta_inv_def.sql`) for creating and updating `SOF$TA_INV_DEF`. Dataform will provide version control, testing, and deployment capabilities for the SQL assets, which will be triggered by Cloud Composer.
*   **Data Ingestion Service (e.g., Cloud Data Fusion, Cloud Storage Transfer Service, or custom ingestion)**: A dedicated service will be required to ingest data from the external Carmen database into BigQuery staging tables, addressing the current DB link dependency.

## 4. Data Flow & Lineage

The legacy workflow consists of a three-tiered execution model:

1.  **`r_ausd_v_ta_inv_def.ksh` (Main Orchestrator)**:
    *   This script initializes the environment, sources common utility scripts (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`), and handles command-line arguments.
    *   It sets up robust error trapping and logging mechanisms.
    *   Crucially, it invokes `k_ausd_v_ta_inv_def.ksh` with relevant parameters (`-j $JobKennung -f ${DW_EintragsNr}`).

2.  **`k_ausd_v_ta_inv_def.ksh` (Sub-Orchestrator)**:
    *   This script is called by the main orchestrator. It further processes parameters and likely sources additional utility scripts (`h_alis_sqlplus.ksh`).
    *   It defines the name of the SQL script to be executed (`Name_SQLskript="${BERT_DIR_ROOT}/aufbereitung/sql/d_ausd_v_ta_inv_def.sql"`).
    *   It executes the Oracle SQL*Plus script `d_ausd_v_ta_inv_def.sql`, presumably through a `starteSQLSkript` function (likely a wrapper for `sqlplus` execution).

3.  **`d_ausd_v_ta_inv_def.sql` (ETL Script)**:
    *   This Oracle SQL*Plus script performs the core data transformation.
    *   **Input Sources**:
        *   `DWTK_MELDUNGEN` (for `v_datum` parameter extraction)
        *   `CDS$TA_INV_DEFINITION` (via DB link to Carmen)
        *   `CDS$TA_INV_CONT_CONFIG` (via DB link to Carmen)
        *   `CDS$TA_CARE_DESCRIPTION` (via DB link to Carmen)
    *   **Transformation**:
        *   Truncates the target table `SOF$TA_INV_DEF` using `isbert_schema.DWPA_UTIL_SKRIPT.runstatement()`.
        *   Inserts data into `SOF$TA_INV_DEF` by joining the `CDS$` tables, applying filters based on `insert_at`, `modified_at`, `valid_from`, `valid_to`, and `is_production` columns.
        *   Uses Oracle hints (e.g., `/*+ full(id) parallel(id,4) */`).
        *   Dynamic SQL substitution through `DEFINE` variables and `&v_carmen`, `&v_datum`.
    *   **Output Target**: `SOF$TA_INV_DEF`

**Migrated Data Flow (GCP):**

*   A **Cloud Composer DAG** will represent the overall workflow.
*   The DAG will have a task to ingest data from the Carmen database into BigQuery staging tables (e.g., `stg_carmen.cds_ta_inv_definition`, `stg_carmen.cds_ta_inv_cont_config`, `stg_carmen.cds_ta_care_description`).
*   Another task will trigger a **Dataform job** to execute the transformed SQL.
*   The Dataform job will define models for `dwtk_meldungen` and the staging Carmen tables.
*   A core Dataform model will represent the transformation logic, writing to a BigQuery table, e.g., `dwh.sof_ta_inv_def`.
*   Airflow tasks will manage logging and error handling, replacing the existing KornShell logic.

## 5. Transformation Logic

The core transformation logic resides in `d_ausd_v_ta_inv_def.sql`. This will be re-engineered in BigQuery SQL within Dataform.

*   **Truncate and Load**: The `TRUNCATE TABLE sof$ta_inv_def` followed by `INSERT INTO` will be directly translated. In Dataform, this can be achieved using a `type: table` definition for `dwh.sof_ta_inv_def` with an `ON REPLACE` or `TRUNCATE` strategy if it's a full refresh. If it's an incremental load, the strategy needs to be adapted. Based on the source SQL, it appears to be a full refresh for `SOF$TA_INV_DEF`.
*   **Source Tables**: `DWTK_MELDUNGEN`, `CDS$TA_INV_DEFINITION`, `CDS$TA_INV_CONT_CONFIG`, `CDS$TA_CARE_DESCRIPTION`. These tables must first be migrated or ingested into BigQuery.
*   **Joins**: The `LEFT OUTER JOIN` (represented by `(+)` in Oracle) between `CDS$TA_INV_DEFINITION`, `CDS$TA_INV_CONT_CONFIG`, and `CDS$TA_CARE_DESCRIPTION` will be converted to standard BigQuery SQL `LEFT JOIN` syntax.
*   **Filters**: All `WHERE` clause filters based on `insert_at`, `modified_at`, `valid_from`, `valid_to`, and `is_production` will be directly translated to BigQuery SQL.
*   **Dynamic SQL**: The `DEFINE v_carmen` and `DEFINE v_datum` variables and their substitution (`&v_carmen`, `&v_datum`) will need to be handled.
    *   `v_datum` is derived from `DWTK_MELDUNGEN`. This lookup can be performed in a preceding Airflow task or within Dataform using a subquery/pre-operation.
    *   `v_carmen` represents the DB link. Once data is in BigQuery, this dynamic element is no longer needed; the tables will be directly referenced.
*   **Oracle Hints**: Oracle-specific hints like `/*+ full(id) parallel(id,4) */` are not applicable in BigQuery and will be removed. BigQuery's query optimizer handles parallelization automatically.
*   **Function Conversion**: `NVL` will be converted to `COALESCE`. `TO_CHAR(MAX(m.timecreated), 'YYYYMMDD')` and `TO_DATE('&v_datum','YYYYMMDD')` will be mapped to BigQuery's date and string formatting functions (e.g., `FORMAT_DATE('%Y%m%d', MAX(m.timecreated))`, `PARSE_DATE('%Y%m%d', @v_datum)`).

## 6. External Dependencies

*   **Carmen Database (via DB link)**:
    *   **Current State**: The `d_ausd_v_ta_inv_def.sql` script accesses tables `CDS$TA_INV_DEFINITION`, `CDS$TA_INV_CONT_CONFIG`, and `CDS$TA_CARE_DESCRIPTION` in an external Carmen database using an Oracle DB link.
    *   **Migration Approach**: This is a critical external dependency that requires a dedicated ingestion strategy. The recommended approach is to establish a data pipeline to regularly extract these tables from the Carmen database and load them into BigQuery staging tables. This could involve:
        *   **Cloud Data Fusion**: For managed ETL pipelines from on-premise Oracle to BigQuery.
        *   **Cloud Storage Transfer Service**: If the data can be periodically exported to Cloud Storage.
        *   **Custom Dataflow/Python scripts**: For more complex extraction logic or if real-time/near real-time sync is required.
    *   The ingestion process must ensure that the BigQuery staging tables are up-to-date *before* the Dataform transformation for `dwh.sof_ta_inv_def` is executed.

## 7. Unresolved / Risks

*   **Missing Complexity/Automation Data**: The `file_complexity` and `automation_rate` for the source files could not be retrieved. This means the estimated effort and automation bucket are unknown, which might impact project planning. A manual assessment might be needed.
*   **`DWPA_UTIL_SKRIPT.runstatement`**: The specific implementation of this Oracle procedure, especially how it handles the `TRUNCATE TABLE` call and other potential dynamic statements, needs to be fully understood to ensure accurate translation to BigQuery SQL or Airflow operations.
*   **KornShell Utilities**: The KornShell scripts source several utility files (e.g., `.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`, `h_alis_sqlplus.ksh`). The functionalities within these utilities (e.g., logging, error reporting, parameter validation) need to be identified and reimplemented using Cloud Composer's logging, error handling, and parameter mechanisms, or by converting relevant logic into Python functions.
*   **Parameter Passing**: The `getopts` parameter parsing in the KornShell scripts needs to be translated to Airflow DAG parameters or environment variables within the Airflow context.

## 8. Build Plan

1.  **Data Ingestion Pipeline Development**:
    *   **Task**: Design and implement a data ingestion pipeline (e.g., using Cloud Data Fusion or custom scripts) to extract `DWTK_MELDUNGEN`, `CDS$TA_INV_DEFINITION`, `CDS$TA_INV_CONT_CONFIG`, `CDS$TA_CARE_DESCRIPTION` from the Carmen database and load them into dedicated BigQuery staging tables (e.g., `project_id.stg_carmen.dwtk_meldungen`, `project_id.stg_carmen.cds_ta_inv_definition`, etc.).
    *   **Language/Tool**: Python (for Dataflow/custom scripts), Cloud Data Fusion, or equivalent.

2.  **Dataform Project Setup**:
    *   **Task**: Initialize a Dataform repository.
    *   **Language/Tool**: Dataform.

3.  **BigQuery Staging Table Definitions (Dataform)**:
    *   **Task**: Define the BigQuery staging tables for the ingested Carmen data in Dataform to ensure they are cataloged and version-controlled. These will likely be `type: incremental` or `type: table` depending on ingestion strategy.
    *   **Language/Tool**: Dataform (SQLX).

4.  **BigQuery `dwh.sof_ta_inv_def` Model Development (Dataform)**:
    *   **Task**: Translate `d_ausd_v_ta_inv_def.sql` into a Dataform SQLX model for `project_id.dwh.sof_ta_inv_def`. This will involve:
        *   Converting Oracle SQL syntax to BigQuery SQL.
        *   Replacing Oracle-specific functions (e.g., `NVL`, `TO_DATE`, `TO_CHAR`).
        *   Handling the `v_datum` logic (potentially as a Dataform assertion or a pre-operation).
        *   Converting `(+)` joins to explicit `LEFT JOIN`s.
        *   Implementing the `TRUNCATE` logic (e.g., `ON REPLACE` or a separate `pre_hook`).
    *   **Language/Tool**: Dataform (SQLX).

5.  **Cloud Composer DAG Development**:
    *   **Task**: Create an Airflow DAG (`r_ausd_v_ta_inv_def_dag.py`) to orchestrate the entire workflow.
    *   **Language/Tool**: Python (for Airflow DAG).
    *   **Components**:
        *   Task for triggering the Carmen data ingestion (if not scheduled externally).
        *   Task for executing the Dataform job (using `DataformRunOperator`).
        *   Tasks for logging, error handling, and notifications (replacing KornShell logic).
        *   Define parameters that correspond to the legacy KornShell script's arguments.

6.  **Utility Script Reimplementation**:
    *   **Task**: Analyze the functionalities of `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`, `h_alis_sqlplus.ksh` and reimplement necessary parts as Python functions or Airflow operators/hooks.
    *   **Language/Tool**: Python.

7.  **Testing**:
    *   **Task**: Develop unit and integration tests for the Dataform models and the Airflow DAG.
    *   **Language/Tool**: Python (for Airflow tests), Dataform (assertions, data tests).

8.  **Deployment**:
    *   **Task**: Deploy the Dataform project and the Airflow DAG to their respective GCP environments.
    *   **Language/Tool**: Dataform CLI/API, Airflow UI/CLI.