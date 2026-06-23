# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_disc_zusgf.ksh

## 1. Purpose & Scope
This migration targets a KornShell wrapper script named `r_ausd_v_ta_disc_zusgf.ksh`. The primary purpose of this script is to orchestrate the data reconciliation process for the `ta_disc_zusgf` table. It handles environment setup, command-line parameter parsing, logging, and error handling before invoking a core processing script, `k_ausd_v_ta_disc_zusgf.ksh`. The script itself does not contain business data manipulation but serves as a control-flow mechanism for a larger ETL job. The overall job was assembled from 1 component and is categorized as medium complexity (stage dist: medium=1).

## 2. Source Inventory
- **File**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_disc_zusgf.ksh`
- **Technology**: KornShell Script
- **Summary**: This is a wrapper KornShell script responsible for orchestrating the data reconciliation process for the `ta_disc_zusgf` table. It handles parameter parsing, environment setup, logging, and error handling before calling a core processing script (`k_ausd_v_ta_disc_zusgf.ksh`).
- **Complexity Tier**: (Not available - no data in `file_complexity` table for this file)
- **Automation Bucket**: B0 (Retire) - *Note: Despite being marked for retirement, this document provides a migration design as requested.*

## 3. Target Architecture
The migration target is Google BigQuery. The current KornShell script's orchestration and control-flow logic will be translated into a BigQuery Stored Procedure. Shared utility scripts and functions will be implemented as separate BigQuery Stored Procedures or User-Defined Functions (UDFs). Logging will be managed through a dedicated BigQuery audit/log table instead of filesystem-based logs. The core business logic, currently residing in `k_ausd_v_ta_disc_zusgf.ksh`, will need to be migrated into its own BigQuery Stored Procedure.

**Components:**
- **`Vertragsdatenabgleich_wrapper_sp`**: A BigQuery Stored Procedure encapsulating the wrapper script's logic.
- **`k_ausd_v_ta_disc_zusgf_sp`**: A BigQuery Stored Procedure for the core processing logic (migration of `k_ausd_v_ta_disc_zusgf.ksh`).
- **`job_log_table`**: A BigQuery table for capturing job execution logs and status.
- **`DWMSG_ErmittleNr_sp`, `DWMSG_Logdateiname_sp`, `DWMSG_ErzeugeEintrag_sp`, `DWMSG_SetzeStichtagInfo_sp`, `DWMSG_Fehlerbehandlung_sp`, `DWMSG_SetzeStatusOK_sp`**: BigQuery Stored Procedures for error handling and logging utilities.
- **Configuration tables**: Optional tables to store job identifiers, log settings, and runtime parameters.
- **Orchestration**: If parameter-driven orchestration across multiple jobs is required, Cloud Workflows or Cloud Composer will be used to invoke the BigQuery Stored Procedures.

## 4. Data Flow & Lineage
The original data flow and lineage involve the `r_ausd_v_ta_disc_zusgf.ksh` script invoking `k_ausd_v_ta_disc_zusgf.ksh`, which in turn interacts with the `ta_disc_zusgf` table.

**Migration Data Flow:**
1. **External Orchestrator (Optional: Cloud Workflows/Composer)**: Initiates the `Vertragsdatenabgleich_wrapper_sp`.
2. **`Vertragsdatenabgleich_wrapper_sp`**:
    - Initializes internal variables and sets up job metadata.
    - Calls utility stored procedures (`DWMSG_ErmittleNr_sp`, `DWMSG_Logdateiname_sp`, `DWMSG_ErzeugeEintrag_sp`, `DWMSG_SetzeStichtagInfo_sp`).
    - Logs job execution status to `job_log_table`.
    - Invokes `k_ausd_v_ta_disc_zusgf_sp`, passing necessary job parameters.
    - On successful completion, logs a success message and calls `DWMSG_SetzeStatusOK_sp`.
    - On error, calls `DWMSG_Fehlerbehandlung_sp` and logs the error to `job_log_table`.
3. **`k_ausd_v_ta_disc_zusgf_sp`**: Contains the migrated core reconciliation logic. This stored procedure will read from and write to the `ta_disc_zusgf` table (or its BigQuery equivalent). The specific read/write operations will be determined during the migration of this core script.
4. **`ta_disc_zusgf` (BigQuery)**: The target table for the data reconciliation process.

## 5. Transformation Logic
The transformation logic primarily involves translating KornShell control flow, variable handling, and external script invocations into BigQuery SQL scripting and stored procedures.

**Key Transformations:**
- **Environment Variables (`$HOME`, `$BERT_DIR_ROOT`, etc.)**: Replaced by BigQuery Stored Procedure parameters, `DECLARE` variables, or values retrieved from configuration tables.
- **Parameter Parsing (`getopts`)**: Replaced by direct parameters passed to the `Vertragsdatenabgleich_wrapper_sp`. Input validation logic will be implemented using BigQuery's `IF` statements.
- **Date Formatting (`date +%d%m%Y`)**: Replaced by BigQuery's `FORMAT_DATE(format_string, date_expression)`.
- **Output (`print`, `tee`)**: Replaced by `INSERT` statements into the `job_log_table` for logging purposes.
- **Error Handling (`trap INT ERR`)**: Replaced by BigQuery's `BEGIN ... EXCEPTION WHEN ERROR THEN ... END` block for structured error handling. Custom error logging will involve calls to `DWMSG_Fehlerbehandlung_sp` and logging to `job_log_table`.
- **Script Inclusion (`. $HOME/.dw_init`)**: Replaced by `CALL` statements to helper BigQuery Stored Procedures or UDFs that implement the functionality of the sourced scripts.
- **External Shell Execution (`${Name_Kernskript} ...`)**: Replaced by a `CALL` statement to the `k_ausd_v_ta_disc_zusgf_sp`.

**Pseudocode for BigQuery Stored Procedure (`Vertragsdatenabgleich_wrapper_sp`):**

```sql
-- BigQuery Script: Vertragsdatenabgleich wrapper

CREATE OR REPLACE PROCEDURE `project_id.dataset_id.Vertragsdatenabgleich_wrapper_sp`(
  -- Parameters will replace shell script arguments and some environment variables
  p_param_s STRING,
  p_param_l STRING,
  p_display_help BOOL
)
BEGIN
  DECLARE ProgName STRING DEFAULT 'Vertragsdatenabgleich';
  DECLARE ProgVersion STRING DEFAULT 'V1.0.0';
  DECLARE ErrNr INT64 DEFAULT 0;
  DECLARE ErrArg STRING DEFAULT '';
  DECLARE DW_EintragsNr INT64; -- Assigned by utility SP
  DECLARE JobKennung STRING DEFAULT 'BERT_V_TA_DISC_ZUSGF';
  DECLARE v_sysdate STRING DEFAULT FORMAT_DATE('%d%m%Y', CURRENT_DATE());
  DECLARE LogDatei STRING; -- Assigned by utility SP
  DECLARE Name_Kernskript_SP STRING DEFAULT 'k_ausd_v_ta_disc_zusgf_sp';

  IF p_display_help THEN
    SELECT
      CONCAT('Programm: ', ProgName) AS line1,
      CONCAT('Version:  ', ProgVersion) AS line2,
      'Aufruf:   Parameter' AS line3,
      '-h     zeigt diese Seite an' AS line4,
      'Beschreibung:' AS line5,
      'Rahmenskript fuer den Abgleich der Vertragsdaten: Tabelle ta_disc_zusgf.' AS line6;
    RETURN;
  END IF;

  -- Simulate sourced initialization via helper procedures/config tables
  -- CALL `project_id.dataset_id.init_dw_environment_sp`();
  -- CALL `project_id.dataset_id.init_error_concept_sp`();

  -- Determine job entry number
  CALL `project_id.dataset_id.DWMSG_ErmittleNr_sp`(DW_EintragsNr);

  -- Determine log file name
  CALL `project_id.dataset_id.DWMSG_Logdateiname_sp`(LogDatei, JobKennung, DW_EintragsNr);

  -- Create log entry
  CALL `project_id.dataset_id.DWMSG_ErzeugeEintrag_sp`(DW_EintragsNr, JobKennung, 'Vertragsdatenabgleich_wrapper_sp', LogDatei);

  -- Set reference date info
  CALL `project_id.dataset_id.DWMSG_SetzeStichtagInfo_sp`(DW_EintragsNr, v_sysdate, 'DDMMYYYY');

  BEGIN
    -- Log job banner
    INSERT INTO `project_id.dataset_id.job_log_table`(job_nr, job_kennung, log_file, message, created_at)
    VALUES
      (DW_EintragsNr, JobKennung, LogDatei, ' ----------------- Job -----------------------', CURRENT_TIMESTAMP()),
      (DW_EintragsNr, JobKennung, LogDatei, CONCAT(' Job-Nr    : \'', CAST(DW_EintragsNr AS STRING), '\''), CURRENT_TIMESTAMP()),
      (DW_EintragsNr, JobKennung, LogDatei, CONCAT(' JobKennung: \'', JobKennung, '\''), CURRENT_TIMESTAMP()),
      (DW_EintragsNr, JobKennung, LogDatei, CONCAT(' Logdatei  : \'', LogDatei, '\''), CURRENT_TIMESTAMP()),
      (DW_EintragsNr, JobKennung, LogDatei, ' ---------------------------------------------', CURRENT_TIMESTAMP());

    -- Replace shell execution with stored procedure call for the core script
    CALL `project_id.dataset_id.k_ausd_v_ta_disc_zusgf_sp`(DW_EintragsNr, JobKennung);

    -- Success handling
    INSERT INTO `project_id.dataset_id.job_log_table`(job_nr, job_kennung, log_file, message, created_at)
    VALUES (DW_EintragsNr, JobKennung, LogDatei, 'Die Abarbeitung wurde ohne erkennbare Fehler beendet', CURRENT_TIMESTAMP());

    CALL `project_id.dataset_id.DWMSG_SetzeStatusOK_sp`(DW_EintragsNr);

  EXCEPTION WHEN ERROR THEN
    -- Equivalent to trap ERR / INT handling
    CALL `project_id.dataset_id.DWMSG_Fehlerbehandlung_sp`(DW_EintragsNr);

    INSERT INTO `project_id.dataset_id.job_log_table`(job_nr, job_kennung, log_file, message, created_at)
    VALUES (DW_EintragsNr, JobKennung, LogDatei, 'AppError: Abbruch', CURRENT_TIMESTAMP());

    RAISE USING MESSAGE = 'Abbruch';
  END;
END;
```

## 6. External Dependencies
The original script directly references several external files and implicitly depends on the operating system for shell functionalities.

- **`$HOME/.dw_init`**: A shell initialization file. In BigQuery, this functionality would be replaced by initial setup within the stored procedure or by calling a dedicated BigQuery helper stored procedure.
- **`${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`**: A utility script for error messaging. This will be migrated to a BigQuery Stored Procedure, e.g., `DWMSG_MeldeFehler_sp`.
- **`${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`**: A utility script for parameter handling. This functionality will be integrated into the main wrapper stored procedure's parameter handling or a dedicated BigQuery helper stored procedure.
- **`${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`**: A utility script for date handling. This will be replaced by BigQuery's built-in date functions or a BigQuery UDF.
- **`${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_v_ta_disc_zusgf.ksh`**: The core processing script. This is the most critical dependency and will be migrated to its own BigQuery Stored Procedure, `k_ausd_v_ta_disc_zusgf_sp`.
- **`ta_disc_zusgf` (Table)**: This is a critical table referenced by the job. Its BigQuery equivalent will be `project_id.dataset_id.ta_disc_zusgf`.

There are no external systems (e.g., Oracle, SFTP, S3) directly referenced in the `lineage_assembled_jobs` metadata for this specific job (`external_systems` is empty). The dependencies are primarily within the file system and other shell scripts.

## 7. Unresolved / Risks
- **`file_complexity` data missing**: The complexity tier and migration flags for `r_ausd_v_ta_disc_zusgf.ksh` were not available, which could impact effort estimation.
- **Migration Bucket `retire` (B0)**: The job is marked for retirement. If the intention is truly to retire, then a full migration design may not be necessary. However, this document provides one based on the prompt's request. A clear decision on retirement vs. migration is crucial.
- **Core Script (`k_ausd_v_ta_disc_zusgf.ksh`) Logic**: The detailed logic of `k_ausd_v_ta_disc_zusgf.ksh` is not fully analyzed here. Its migration complexity and specific data transformations are a downstream dependency. If this core script involves complex file I/O, external system interactions, or highly specialized shell features, it might require a more elaborate solution involving Cloud Functions/Run or Dataflow in addition to BigQuery SQL.
- **Shell-specific Features**: Direct equivalents for shell `source`, `trap`, and `tee` do not exist in pure BigQuery SQL and require architectural changes as described in Section 5.
- **Implicit Dependencies**: There might be other implicit dependencies (e.g., specific environment variables, cron schedules) not explicitly captured in the provided metadata that would need to be identified during a deeper analysis.

## 8. Build Plan
The build plan focuses on implementing the BigQuery components and ensuring the correct invocation and data flow.

1. **Create `job_log_table`**: Define and create the BigQuery logging table schema.
   ```sql
   CREATE TABLE `project_id.dataset_id.job_log_table` (
     job_nr INT64,
     job_kennung STRING,
     log_file STRING,
     message STRING,
     created_at TIMESTAMP
   );
   ```
2. **Develop BigQuery Utility Stored Procedures**: Implement `DWMSG_ErmittleNr_sp`, `DWMSG_Logdateiname_sp`, `DWMSG_ErzeugeEintrag_sp`, `DWMSG_SetzeStichtagInfo_sp`, `DWMSG_Fehlerbehandlung_sp`, and `DWMSG_SetzeStatusOK_sp`. These will handle logging and metadata management within BigQuery.
3. **Migrate `ta_disc_zusgf` Table**: Create the BigQuery schema for `ta_disc_zusgf` and migrate existing data.
4. **Migrate Core Script Logic (`k_ausd_v_ta_disc_zusgf.ksh`)**: Develop the `k_ausd_v_ta_disc_zusgf_sp` BigQuery Stored Procedure, which will contain the actual data reconciliation logic, including data reads and writes to `ta_disc_zusgf`. This is a critical independent task.
5. **Develop `Vertragsdatenabgleich_wrapper_sp`**: Implement the main wrapper stored procedure in BigQuery SQL, incorporating parameter handling, error management, and calling the utility and core stored procedures as detailed in Section 5.
6. **Implement Orchestration (if required)**: If this job is part of a larger workflow, configure Cloud Workflows or Cloud Composer to trigger `Vertragsdatenabgleich_wrapper_sp` with appropriate parameters.
7. **Testing**: Thoroughly test each BigQuery stored procedure and the overall workflow for functionality, error handling, and performance.
8. **Deployment**: Deploy all BigQuery objects and orchestration components to the production environment.