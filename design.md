# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_apn.ksh

## 1. Purpose & Scope
This document outlines the migration design for the KornShell script `r_ausd_bp_ta_bpr_apn.ksh`. The job's primary purpose is the initial provisioning of selected base products for BERT (Basisprodukte fï¿½r BERT). Specifically, it generates a cutoff-date extraction of contract cache data from the Data Warehouse (DWH) and makes it available for "Forderungsscoring" (FOS). It also handles the deletion of already provisioned tables under certain conditions. The script acts as a wrapper, orchestrating parameter handling, environment setup, logging, and ultimately invoking a core business logic script. The migration target platform is Google BigQuery. This job was assembled from 1 component, indicating a medium complexity for its stage distribution.

## 2. Source Inventory
The assembled job consists of a single KornShell script.

*   **File Name**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_apn.ksh`
*   **Technology**: Shell (KornShell)
*   **Category**: shell
*   **Tool**: KornShell
*   **Complexity Tier**: (Not available in `file_complexity` table, assumed to be medium based on `lineage_assembled_jobs` complexity distribution)
*   **Migration Flags**: (Not available in `file_complexity` table)
*   **Automation Bucket**: semi_auto

**Indirectly Referenced Scripts (not part of this assembled job's `component_files` but invoked by the seed script):**
*   Utility scripts: `$HOME/.dw_init`, `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`, `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`, `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`
*   Core business logic script: `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_bp_ta_bpr_apn.ksh`

## 3. Target Architecture
The migrated solution in BigQuery will consist of:
*   **BigQuery Stored Procedures**: To encapsulate the parameter handling, date derivation, validation, logging, and orchestration logic.
*   **BigQuery Tables**:
    *   A `job_control` table to store job metadata, status, `Stichtag`, `Wiederanlaufwert`, and logging information (replacing file-based logs).
    *   Potentially additional tables for specific configuration or error logging.
*   **Orchestration Layer**: An external orchestrator (e.g., Cloud Composer/Airflow, Cloud Workflows, or Dataform) will be required to schedule the BigQuery Stored Procedure and manage its execution, especially if external dependencies remain or the invoked `k_ausd_bp_ta_bpr_apn.ksh` is also migrated to a separate BigQuery component.

## 4. Data Flow & Lineage
The original script `r_ausd_bp_ta_bpr_apn.ksh` serves as an orchestration wrapper.
1.  **Input Parameters**: The script accepts optional command-line parameters `-s` (Stichtag DDMMYYYY) and `-l` (Wiederanlaufwert).
2.  **Environment & Utility Sourcing**: It sources several local utility KornShell scripts (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`) for environment setup, error handling, parameter parsing, and date functions.
3.  **Parameter Processing**: It parses command-line arguments, sets default values for `Wiederanlaufwert` (0) and `Stichtag` (system date if not provided), and performs parameter validation.
4.  **Logging Setup**: Initializes job-specific logging, generates a log filename, and creates initial log entries.
5.  **Core Script Invocation**: The script's primary function is to invoke another KornShell script, `k_ausd_bp_ta_bpr_apn.ksh`, passing validated parameters (`JobKennung`, `Stichtag`, `DW_EintragsNr`, `Wiederanlaufwert`). This invocation is critical for the actual business logic.
6.  **Status Reporting**: Upon completion, it updates the job status as successful or logs errors if `trap` conditions are met.
7.  **External Systems**: No direct external systems were identified in `lineage_assembled_jobs` for this run.
8.  **Unresolved Targets**: No unresolved targets were identified.

In BigQuery, this flow will be mapped to a Stored Procedure (`ausd_bp_ta_bpr_apn_wrapper`) which will:
*   Accept `p_stichtag` and `p_wiederanlaufWert` as `IN` parameters.
*   Perform internal variable initialization and date derivation.
*   Validate inputs and raise errors if invalid.
*   Insert records into a `job_control` table for logging start and metadata.
*   Call a downstream BigQuery Stored Procedure (e.g., `k_ausd_bp_ta_bpr_apn`) that will contain the migrated business logic from the original `k_ausd_bp_ta_bpr_apn.ksh`.
*   Update the `job_control` table with final status and error information upon completion or exception.

## 5. Transformation Logic
The script's logic primarily involves orchestration and parameter management.

*   **Parameter Parsing**: The `getopts` mechanism in KornShell for `-s` and `-l` will be replaced by `IN` parameters in a BigQuery Stored Procedure.
*   **Defaulting Logic**:
    *   `p_wiederanlaufWert` defaults to `0` if not provided. This will be an `INT64` default value in the stored procedure.
    *   `p_stichtag` defaults to the current system date if not provided. This will be handled by `CURRENT_DATE()` in BigQuery.
*   **Date Derivation**: The `DWDate_Gib_Zeitraum` helper function will be replaced by BigQuery's native date functions like `CURRENT_DATE()`, `PARSE_DATE()`, and `FORMAT_DATE()`.
*   **Validation**: The `pruefeParameterGesetzt` helper and `if [ ! $ErrNr -eq 0 ]` blocks will be replaced by `ASSERT` statements or explicit `IF ... THEN SIGNAL`-style error handling within the BigQuery Stored Procedure.
*   **Error Handling**: The `set -e` and `trap` mechanisms will be replaced by BigQuery's `BEGIN ... EXCEPTION WHEN ERROR THEN ... END` blocks to catch and manage errors. Errors will be logged to the `job_control` table.
*   **Logging**: All `print` statements and references to the `DWMSG_*` logging framework will be replaced by `INSERT` or `UPDATE` statements to the BigQuery `job_control` table for persistent and queryable logs.
*   **Invocation of Core Logic**: The execution of `${Name_Kernskript}` (`k_ausd_bp_ta_bpr_apn.ksh`) will be replaced by a `CALL` statement to a corresponding BigQuery Stored Procedure (e.g., `CALL project.dataset.k_ausd_bp_ta_bpr_apn(...)`).

## 6. External Dependencies
The original script has the following dependencies:
*   **Environment Sourcing**: `. $HOME/.dw_init` is used for environment initialization. In BigQuery, this would be replaced by BigQuery dataset constants, session parameters, or configurations managed by the orchestration layer.
*   **Utility Scripts**:
    *   `f_alis_msgerr.ksh`: Error handling and messaging. Replaced by BigQuery's exception handling and `job_control` logging.
    *   `h_alis_parameter.ksh`: Parameter parsing. Replaced by BigQuery Stored Procedure parameters.
    *   `h_alis_date.ksh`: Date manipulation. Replaced by BigQuery's native date functions.
*   **Core Business Logic Script**: `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_bp_ta_bpr_apn.ksh`. This is the most significant dependency. Its functionality is not included in this document's scope, but its migration to a BigQuery Stored Procedure or a set of SQL statements will be a prerequisite for this wrapper's complete migration. The wrapper will `CALL` this migrated BigQuery component.
*   **External Systems**: The `lineage_assembled_jobs` record indicates no direct external systems for this specific job.

## 7. Unresolved / Risks
*   **Core Business Logic (`k_ausd_bp_ta_bpr_apn.ksh`)**: The actual data transformation and provisioning logic resides in `k_ausd_bp_ta_bpr_apn.ksh`, which is invoked by this wrapper script but is not part of this assembled job's component files. The migration of this core script is critical and represents the main "functionality gap" that needs to be addressed for the end-to-end process. Its absence means the current design only covers the wrapper.
*   **Shell Traps**: The `trap` commands for signal handling (INT, STOP, CONT, ERR) are a shell-specific construct and do not have direct BigQuery SQL equivalents beyond the `EXCEPTION` block for errors. Complex signal handling logic might require Python-based orchestration.
*   **Environment Variables**: The sourcing of `.dw_init` and the use of `${BERT_DIR_ROOT}` implies an environment setup that needs to be mapped to BigQuery project/dataset structure or configurable parameters in the orchestration layer.
*   **Proprietary Logging Framework**: The `DWMSG_*` functions are part of a custom logging framework. While the conceptual outcome (logging job status) is replicable in BigQuery tables, the exact implementation and detailed log messages might need careful mapping.

## 8. Build Plan
The migration build plan involves creating BigQuery objects and an orchestration mechanism.

1.  **Define `job_control` Table (DDL)**:
    ```sql
    CREATE TABLE IF NOT EXISTS `project.dataset.job_control` (
      job_nr INT64,
      job_kennung STRING,
      script_name STRING,
      log_file STRING,
      sysdate DATE,
      stichtag DATE,
      restart_value INT64,
      status STRING,
      error_message STRING,
      created_at TIMESTAMP,
      finished_at TIMESTAMP
    );
    ```

2.  **Migrate `k_ausd_bp_ta_bpr_apn.ksh` (Prerequisite)**:
    *   Analyze `k_ausd_bp_ta_bpr_apn.ksh` to identify its data sources (tables it reads), transformations, and target tables (tables it writes to).
    *   Design and implement `project.dataset.k_ausd_bp_ta_bpr_apn` as a BigQuery Stored Procedure, potentially involving BigQuery SQL queries for data manipulation. This is an independent task but essential for the full workflow.

3.  **Create BigQuery Stored Procedure for Wrapper (`ausd_bp_ta_bpr_apn_wrapper`)**:
    *   **Language**: BigQuery SQL
    *   **Code**:
        ```sql
        CREATE OR REPLACE PROCEDURE `project.dataset.ausd_bp_ta_bpr_apn_wrapper`(
          IN p_stichtag STRING,
          IN p_wiederanlaufWert INT64
        )
        BEGIN
          DECLARE v_sysdate DATE DEFAULT CURRENT_DATE();
          DECLARE v_stichtag DATE;
          DECLARE v_wiederanlaufWert INT64 DEFAULT 0;
          DECLARE v_job_kennung STRING DEFAULT 'ausd_bp_ta_bpr_apn';
          DECLARE v_job_nr INT64;
          DECLARE v_logdatei STRING;
          DECLARE v_errnr INT64 DEFAULT 0;
          DECLARE v_errarg STRING DEFAULT '';
          DECLARE v_status STRING DEFAULT 'INIT';

          BEGIN
            -- Initialize restart value
            IF p_wiederanlaufWert IS NULL THEN
              SET v_wiederanlaufWert = 0;
            ELSE
              SET v_wiederanlaufWert = p_wiederanlaufWert;
            END IF;

            -- Determine stichtag
            IF p_stichtag IS NULL OR TRIM(p_stichtag) = '' THEN
              SET v_stichtag = v_sysdate;
            ELSE
              SET v_stichtag = PARSE_DATE('%d%m%Y', p_stichtag);
            END IF;

            -- Validate required parameter
            IF v_stichtag IS NULL THEN
              SET v_errnr = 193;
              SET v_errarg = 'Stichtag';
              RAISE USING MESSAGE = 'Required parameter Stichtag missing or invalid';
            END IF;

            -- Create job metadata and log initial status
            SET v_job_nr = (
              SELECT IFNULL(MAX(job_nr), 0) + 1
              FROM `project.dataset.job_control`
            );

            SET v_logdatei = CONCAT('job_', CAST(v_job_nr AS STRING), '_', v_job_kennung, '.log');

            INSERT INTO `project.dataset.job_control`
            (
              job_nr,
              job_kennung,
              script_name,
              log_file,
              sysdate,
              stichtag,
              restart_value,
              status,
              created_at
            )
            VALUES
            (
              v_job_nr,
              v_job_kennung,
              'ausd_bp_ta_bpr_apn_wrapper',
              v_logdatei,
              v_sysdate,
              v_stichtag,
              v_wiederanlaufWert,
              'STARTED',
              CURRENT_TIMESTAMP()
            );

            -- Call the core business logic procedure (placeholder)
            CALL `project.dataset.k_ausd_bp_ta_bpr_apn`( -- This procedure must be implemented
              v_job_kennung,
              FORMAT_DATE('%d%m%Y', v_stichtag),
              v_job_nr,
              v_wiederanlaufWert
            );

            SET v_status = 'OK';

            UPDATE `project.dataset.job_control`
            SET status = v_status,
                finished_at = CURRENT_TIMESTAMP()
            WHERE job_nr = v_job_nr;

          EXCEPTION WHEN ERROR THEN
            UPDATE `project.dataset.job_control`
            SET status = 'ERROR',
                error_message = @@error.message,
                finished_at = CURRENT_TIMESTAMP()
            WHERE job_nr = v_job_nr;

            RAISE USING MESSAGE = CONCAT('AppError: ', @@error.message);
          END;
        END;
        ```

4.  **Develop Orchestration (e.g., Cloud Composer/Airflow DAG)**:
    *   Create an Airflow DAG that calls the `ausd_bp_ta_bpr_apn_wrapper` BigQuery Stored Procedure, passing the necessary `stichtag` and `wiederanlaufWert` parameters.
    *   The DAG should handle scheduling and potentially upstream/downstream dependencies.