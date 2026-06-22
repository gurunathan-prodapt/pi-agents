# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_rechempf.ksh

## 1. Purpose & Scope

This job, identified as `5af228f1`, is responsible for the initial provisioning of the contract cache for "Forderungsscoring" (FOS). Its primary purpose is to generate a snapshot-based extraction (`Stichtags-Abzug`) of contract cache data from the Data Warehouse (DWH) and make it available for the FOS system. The job involves reading execution parameters, determining a processing date, managing logging and error handling, and orchestrating a core script that performs the actual data processing.

The scope of this migration design covers the `r_ausd_rechempf.ksh` KornShell script, which acts as an orchestrator, and its direct invocation of `k_ausd_rechempf.ksh`.

## 2. Source Inventory

The job consists of a single primary source file:

*   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_rechempf.ksh`
    *   **Technology:** KornShell
    *   **Category:** shell
    *   **Complexity Tier:** medium
    *   **Migration Bucket:** semi_auto
    *   **Purpose:** Orchestration script. It parses command-line arguments, sets up logging, error handling, and then invokes a core processing script (`k_ausd_rechempf.ksh`) with the determined parameters.

## 3. Target Architecture

The target platform is Google BigQuery. Given the orchestration nature of the source KornShell script, the proposed target architecture will involve:

*   **Orchestration:** An Airflow DAG (Directed Acyclic Graph) will replace the `r_ausd_rechempf.ksh` script. This DAG will handle parameter parsing, environment setup, logging, error handling, and the invocation of the core data processing logic.
*   **Data Processing:** The logic encapsulated within the currently invoked `k_ausd_rechempf.ksh` (which is assumed to contain the SQL-like data extraction and preparation) will be migrated to BigQuery SQL scripts. These SQL scripts will be executed as tasks within the Airflow DAG.
*   **Data Storage:** All source data relevant to the "Vertrags-Cache" and target FOS tables will reside in BigQuery datasets.
*   **Logging & Monitoring:** Airflow's native logging and monitoring capabilities will replace the custom KornShell logging functions (`DWMSG_*`).

## 4. Data Flow & Lineage

The current data flow is as follows:

1.  **`r_ausd_rechempf.ksh` (Orchestrator)**:
    *   Reads command-line parameters: `-s` (Stichtag - processing date), `-l` (Wiederanlaufwert - restart value).
    *   Sourced utility scripts (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`) are loaded for environment setup, error handling, parameter parsing, and date functions.
    *   Determines the `p_stichtag` (processing date), defaulting to the system date (`v_sysdate`) if not provided.
    *   Initializes `p_wiederanlaufWert` to 0 if not provided.
    *   Sets up logging (`JobKennung`, `LogDatei`, `DW_EintragsNr`) and error traps.
    *   **Invokes `k_ausd_rechempf.ksh`** with the following parameters:
        *   `-j $JobKennung`
        *   `-s $p_stichtag`
        *   `-f ${DW_EintragsNr}`
        *   `-l ${p_wiederanlaufWert}`
        *   The output of `k_ausd_rechempf.ksh` is redirected to `$LogDatei`.
2.  **`k_ausd_rechempf.ksh` (Core Logic)**:
    *   This script, explicitly invoked by `r_ausd_rechempf.ksh`, is responsible for the actual "Stichtags-Abzug der Vertrags-Cache im DWH und stellt sie Forderungsscoring zur Verfuegung." This implies it reads data from DWH tables (`Gueltig_von`, `Gueltig_bis`, `LADEDATUM` are mentioned as selection criteria in `r_ausd_rechempf.ksh`'s usage text, likely applied by `k_ausd_rechempf.ksh`) and writes to FOS-related tables.
    *   The restart logic (`p_wiederanlaufWert`) suggests an incremental load or selective processing based on `DWH_VERTRAG_ID`.

**Migrated Data Flow:**

1.  **Airflow DAG (`r_ausd_rechempf_dag`)**:
    *   **Task 1 (Parameter Parsing & Setup)**: PythonOperator to parse DAG parameters (e.g., `stichtag`, `restart_value`), equivalent to shell script's `getopts` and date logic.
    *   **Task 2 (Core Data Processing)**: BigQueryOperator executing BigQuery SQL scripts (derived from `k_ausd_rechempf.ksh`'s logic). This task will read from BigQuery tables representing the DWH "Vertrags-Cache" (e.g., historical `TA_C_VERTRAG` data) and apply filtering logic (`Gueltig_von <= Stichtag < Gueltig_bis AND LADEDATUM < Stichtag`). It will then write the extracted data to BigQuery tables designated for FOS (e.g., `FOS_CONTRACT_CACHE`). The restart logic will be incorporated into the SQL or a preceding Python task.
    *   **Task 3 (Status Reporting)**: PythonOperator to update Airflow's metadata or trigger external notifications based on job success/failure, replacing `DWMSG_SetzeStatusOK`.

## 5. Transformation Logic

The `r_ausd_rechempf.ksh` script itself contains no direct data transformation logic. Its function is purely orchestrational. The transformation logic resides in the invoked `k_ausd_rechempf.ksh`.

Based on the comments in `r_ausd_rechempf.ksh`, the core transformation performed by `k_ausd_rechempf.ksh` is:
*   **Extract `Vertrags-Cache`**: Select records from a DWH contract table (potentially `TA_C_VERTRAG` or a similar entity).
*   **Date Filtering**: Apply filtering based on `Stichtag` (processing date) to identify valid records: `Gueltig_von <= Stichtag < Gueltig_bis AND LADEDATUM < Stichtag`.
*   **Incremental/Restart Logic**: If `p_wiederanlaufWert` is provided, select only contracts with `DWH_VERTRAG_ID > p_wiederanlaufWert` and potentially delete existing entries in the target table for `DWH_VERTRAG_ID >= p_wiederanlaufWert` before inserting new data.
*   **Load to FOS**: Write the filtered and potentially transformed contract data to a target table used by the FOS system.

The detailed column-level transformations would need to be extracted from the `k_ausd_rechempf.ksh` script once it is analyzed.

## 6. External Dependencies

### Current Dependencies:

*   **Sourced Utility Scripts (Local Filesystem)**:
    *   `$HOME/.dw_init`: Environment initialization.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`: Error messaging and handling.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`: Parameter parsing helper.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`: Date handling helper.
*   **Invoked Core Script (Local Filesystem)**:
    *   `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_rechempf.ksh`: Contains the primary data extraction and processing logic.
*   **Inferred Database Interactions**:
    *   Source DWH tables (e.g., tables related to `Vertrags-Cache`, with columns like `Gueltig_von`, `Gueltig_bis`, `LADEDATUM`, `DWH_VERTRAG_ID`).
    *   Target FOS tables (e.g., `FOS-Tabelle`).

There are no external systems explicitly identified in the `lineage_assembled_jobs` record (e.g., Oracle, SFTP, S3).

### Target Replacements:

*   **Sourced Utility Scripts**: These shell-specific utilities will be replaced by Python functions or Airflow features within the DAG.
    *   `dw_init`: Replaced by Airflow environment configuration.
    *   `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`: Replaced by standard Python libraries, custom Python functions, or Airflow's built-in parameter handling and logging mechanisms.
*   **Invoked Core Script (`k_ausd_rechempf.ksh`)**: The data processing logic will be rewritten as BigQuery SQL and orchestrated by a `BigQueryOperator` in Airflow.
*   **Database Interactions**: All inferred DWH and FOS tables will be migrated to BigQuery.

## 7. Unresolved / Risks

*   **`k_ausd_rechempf.ksh` Analysis**: The most significant unresolved item is the detailed logic within `k_ausd_rechempf.ksh`. This script needs to be thoroughly analyzed to extract the precise SQL transformations, table names, and column mappings for migration to BigQuery SQL. Its category, complexity, and migration bucket are currently unknown.
*   **Utility Script Migration**: The custom shell utility scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`) represent a migration effort to reimplement their functionality in Python or leverage Airflow equivalents.
*   **Restart Logic (`p_wiederanlaufWert`)**: The specific implementation of the restart logic, including the deletion of existing records (`die Eintraege bzgl. Werten >= diesem Wert werden geloescht`) needs careful migration to BigQuery SQL to ensure data integrity and idempotency.
*   **Date Logic**: The date determination logic `MIN(sysdate,maxladedatum) ... fuer die Synchronisation ist dieses Vorgehen notwendig` in the original script (`#AL?? FOSHoleLadedatum "DWH\$TA_C_VERTRAG" v_ladedatum`) implies a dependency on the maximum load date of the source table, which might need specific handling in BigQuery, possibly by querying a metadata table or the source table itself. The current script defaults to `p_stichtag=$v_sysdate` if not set.

## 8. Build Plan

The migration of this job will involve the following steps:

1.  **Analyze `k_ausd_rechempf.ksh`**:
    *   Read and reverse-engineer the `k_ausd_rechempf.ksh` script to understand its exact data extraction, transformation, and loading (ETL) logic, including all source tables, target tables, and column mappings.
    *   Identify any embedded SQL, shell commands interacting with databases, or calls to other scripts.
2.  **Design BigQuery SQL for `k_ausd_rechempf.ksh`**:
    *   Translate the ETL logic from `k_ausd_rechempf.ksh` into optimized BigQuery SQL scripts.
    *   Incorporate the `Stichtag` and `Wiederanlaufwert` parameters as variables within the BigQuery SQL, possibly using `CREATE TEMP FUNCTION` or direct parameterization.
    *   Design the deletion/insertion logic for the restart mechanism (`DWH_VERTRAG_ID > restart_value`).
3.  **Develop Airflow DAG (`r_ausd_rechempf_dag.py`)**:
    *   Create a new Airflow DAG in Python.
    *   **Task 1 (`parse_params_and_setup`)**: PythonOperator to parse input parameters (e.g., `stichtag`, `restart_value`), handle default values, and set up Airflow variables or XComs for downstream tasks.
    *   **Task 2 (`execute_bq_load`)**: BigQueryOperator to execute the BigQuery SQL script(s) designed in step 2. Pass the parsed parameters to the BigQuery job.
    *   **Task 3 (`log_status`)**: PythonOperator to log job completion status, replacing the `DWMSG_SetzeStatusOK` functionality.
    *   Implement error handling and retry mechanisms within the DAG tasks.
4.  **Migrate Utility Functionality**:
    *   Reimplement necessary functionalities from `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, and `h_alis_date.ksh` using Python modules or Airflow's built-in features.
5.  **Testing**:
    *   Develop unit and integration tests for the BigQuery SQL scripts and the Airflow DAG.
    *   Perform data validation to ensure the migrated solution produces identical or functionally equivalent results to the legacy job.
6.  **Deployment**:
    *   Deploy the BigQuery SQL scripts and the Airflow DAG to the Google Cloud environment.