# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_iccid_vertrag.ksh

## 1. Purpose & Scope
This document outlines the migration design for the KornShell script `r_ausd_bp_ta_iccid_vertrag.ksh`. This script acts as an orchestrator, parsing command-line arguments for a 'Stichtag' (key date) and a 'Wiederanlaufwert' (restart value). It then executes a core data preparation script (`k_ausd_bp_ta_iccid_vertrag.ksh`) with these parameters, incorporating robust error handling and logging. The primary business purpose of this job is the initial provisioning of selected base products (e.g., FAX, Data24) for the BERT system, specifically by extracting a snapshot of contract cache data from the Data Warehouse (DWH) and making it available for demand scoring (FOS). The job was assembled from a single component, and its processing stage distribution is classified as medium.

## 2. Source Inventory
The job consists of a single source file:
*   **File Name**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_iccid_vertrag.ksh`
*   **Technology**: KornShell (ksh)
*   **Category**: shell
*   **Purpose**: Orchestration script, wrapper for core data provisioning logic.
*   **Complexity Tier**: Not available (file_complexity query returned no rows).
*   **Automation Bucket**: semi_auto
*   **Summary**: This KornShell script is an orchestrator responsible for parameter parsing, environment setup, error handling, logging, and invoking a core data preparation script. It does not contain direct data transformation logic.

## 3. Target Architecture
The target platform for this migration is Google Cloud Platform, primarily utilizing BigQuery for data processing and potentially Cloud Composer (Airflow) for orchestration.

The `r_ausd_bp_ta_iccid_vertrag.ksh` script, being an orchestrator, will be replatformed as a **BigQuery Stored Procedure**. This stored procedure will handle:
*   Parameter parsing for `Stichtag` and `Wiederanlaufwert`.
*   Defaulting of parameter values.
*   Validation of input parameters.
*   Job audit logging (start, success, error) to a dedicated `job_audit` table in BigQuery.
*   Invocation of a downstream BigQuery Stored Procedure (e.g., `k_ausd_bp_ta_iccid_vertrag`) that will encapsulate the logic of the original core data preparation script.

External shell environment sourcing and OS-level trap handling will be replaced by BigQuery's built-in scripting capabilities (`ASSERT`, `IF`, `BEGIN...EXCEPTION`, `CALL`) and a dedicated audit/logging table.

## 4. Data Flow & Lineage
The original script's data flow is as follows:
1.  **Inputs**:
    *   Command-line arguments: `-s DDMMYYYY` (Stichtag) and `-l` (Wiederanlaufwert).
    *   Environment variables and settings sourced from `$HOME/.dw_init`.
    *   Shared shell utility scripts: `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`.
2.  **Processing**:
    *   Parses command-line arguments.
    *   Initializes `Wiederanlaufwert` to `0` if not provided.
    *   Determines `Stichtag` (defaults to system date if not provided).
    *   Validates parameters.
    *   Sets up job-specific logging and error handling using internal `DWMSG_*` functions.
    *   Invokes the core script `k_ausd_bp_ta_iccid_vertrag.ksh` with derived parameters and redirects its output to a log file.
3.  **Outputs**:
    *   Log file entries for job start, parameters, status, and messages.
    *   Job status updates via `DWMSG_SetzeStatusOK`.
    *   Execution of the core data provisioning logic handled by `k_ausd_bp_ta_iccid_vertrag.ksh`.

No explicit lineage edges (INVOKES, READS, WRITES, DEPENDS_ON) were found for this file in the `lineage_edges` table. This indicates that its dependencies on the called scripts (`k_ausd_bp_ta_iccid_vertrag.ksh` and other utilities) and any data read/written by those scripts are not automatically captured in the available lineage graph. This implies a need for manual analysis of the called scripts for a complete lineage picture.

The migrated data flow will be orchestrated by the BigQuery Stored Procedure, which will:
*   Receive `p_stichtag` and `p_wiederanlaufWert` as input parameters.
*   Perform internal validation and defaulting.
*   Record audit entries in `project.dataset.job_audit`.
*   `CALL` the `project.dataset.k_ausd_bp_ta_iccid_vertrag` BigQuery Stored Procedure (the migrated core logic).
*   Handle exceptions and update audit logs accordingly.

## 5. Transformation Logic
The transformation logic for this orchestrator script involves translating shell scripting patterns to BigQuery SQL scripting constructs.

**Key Preserved Logic**:
*   **Parameter Handling**: The script's ability to accept `p_stichtag` (cutoff date) and `p_wiederanlaufWert` (restart value) as input and to apply default values if not provided will be directly translated into the BigQuery Stored Procedure's input parameters and conditional logic (`IFNULL`).
*   **Parameter Validation**: The script's `pruefeParameterGesetzt` logic will be implemented using BigQuery `ASSERT` statements to ensure parameters are present and in the correct format (e.g., `DDMMYYYY` for `Stichtag`).
*   **Job Status and Logging**: The `DWMSG_ErmittleNr`, `DWMSG_Logdateiname`, `DWMSG_ErzeugeEintrag`, `DWMSG_SetzeStichtagInfo`, `DWMSG_MeldeFehler`, and `DWMSG_SetzeStatusOK` functions will be replaced by `INSERT` statements into a BigQuery `job_audit` table, capturing job start, success, and error messages, along with relevant metadata like `job_nr`, `job_kennung`, `stichtag`, and `log_timestamp`.
*   **Orchestration of Core Logic**: The invocation of `${Name_Kernskript}` (which is `k_ausd_bp_ta_iccid_vertrag.ksh`) will be replaced by a `CALL` statement to its migrated BigQuery Stored Procedure equivalent, passing the necessary parameters.

**Logic Not Directly Preserved / Replaced**:
*   **Shell `getopts`**: Replaced by direct BigQuery Stored Procedure input parameters.
*   **Sourcing Environment Files (`. $HOME/.dw_init`)**: Global environment setup will be managed through BigQuery project/dataset configuration or potentially by a higher-level orchestration tool (e.g., Airflow DAG) that invokes the BigQuery Stored Procedure.
*   **Shell Traps (`trap`)**: OS-level signal handling will be replaced by BigQuery's `BEGIN...EXCEPTION` blocks for error management within the stored procedure, providing robust error trapping and logging.
*   **Filesystem Log File Handling**: Direct writing to a local filesystem log file will be replaced by structured logging within BigQuery (inserting into `job_audit` table).
*   **Direct Execution of Another `.ksh` Script**: The call to `k_ausd_bp_ta_iccid_vertrag.ksh` will be replaced by a `CALL` to its corresponding BigQuery Stored Procedure.

**BigQuery SQL Pseudocode for `ausd_bp_ta_iccid_vertrag_wrapper` Stored Procedure:**

```sql
-- BigQuery Script / Stored Procedure Pseudocode
-- Purpose: replicate wrapper behavior of ksh job

CREATE OR REPLACE PROCEDURE `project.dataset.ausd_bp_ta_iccid_vertrag_wrapper`(
  IN p_stichtag STRING,
  IN p_wiederanlaufWert INT64
)
BEGIN
  DECLARE v_sysdate STRING;
  DECLARE v_stichtag STRING;
  DECLARE v_wiederanlaufWert INT64;
  DECLARE v_jobkennung STRING DEFAULT 'ausd_bp_ta_iccid_vertrag';
  DECLARE v_dwh_eintragsnr INT64;
  DECLARE v_log_message STRING;

  -- Equivalent to DWDate_Gib_Zeitraum 1 'D' 'DDMMYYYY'
  SET v_sysdate = FORMAT_DATE('%d%m%Y', CURRENT_DATE());

  -- Default restart value
  SET v_wiederanlaufWert = IFNULL(p_wiederanlaufWert, 0);

  -- Default stichtag
  SET v_stichtag = IFNULL(p_stichtag, v_sysdate);

  -- Validate required parameter
  ASSERT v_stichtag IS NOT NULL AND LENGTH(v_stichtag) = 8
    AS 'Stichtag must be provided in DDMMYYYY format';

  -- Optional: validate date parseability
  ASSERT SAFE.PARSE_DATE('%d%m%Y', v_stichtag) IS NOT NULL
    AS 'Stichtag is not a valid DDMMYYYY date';

  -- Job number generation placeholder
  -- Replace with sequence/table-based job numbering if required
  SET v_dwh_eintragsnr = (
    SELECT IFNULL(MAX(job_nr), 0) + 1
    FROM `project.dataset.job_audit`
    WHERE job_kennung = v_jobkennung
  );

  -- Write start audit entry
  INSERT INTO `project.dataset.job_audit` (
    job_nr,
    job_kennung,
    script_name,
    log_timestamp,
    stichtag,
    status,
    message
  )
  VALUES (
    v_dwh_eintragsnr,
    v_jobkennung,
    'ausd_bp_ta_iccid_vertrag_wrapper',
    CURRENT_TIMESTAMP(),
    v_stichtag,
    'STARTED',
    'Job started'
  );

  BEGIN
    -- Call downstream business procedure replacing:
    -- ${Name_Kernskript} -j $JobKennung -s $p_stichtag -f ${DW_EintragsNr} -l ${p_wiederanlaufWert}
    CALL `project.dataset.k_ausd_bp_ta_iccid_vertrag`(
      v_jobkennung,
      v_stichtag,
      v_dwh_eintragsnr,
      v_wiederanlaufWert
    );

    -- Success audit
    INSERT INTO `project.dataset.job_audit` (
      job_nr,
      job_kennung,
      script_name,
      log_timestamp,
      stichtag,
      status,
      message
    )
    VALUES (
      v_dwh_eintragsnr,
      v_jobkennung,
      'ausd_bp_ta_iccid_vertrag_wrapper',
      CURRENT_TIMESTAMP(),
      v_stichtag,
      'OK',
      'Die Abarbeitung wurde ohne erkennbare Fehler beendet'
    );

  EXCEPTION WHEN ERROR THEN
    -- Failure audit
    INSERT INTO `project.dataset.job_audit` (
      job_nr,
      job_kennung,
      script_name,
      log_timestamp,
      stichtag,
      status,
      message
    )
    VALUES (
      v_dwh_eintragsnr,
      v_jobkennung,
      'ausd_bp_ta_iccid_vertrag_wrapper',
      CURRENT_TIMESTAMP(),
      v_stichtag,
      'ERROR',
      'AppError: Abbruch'
    );

    RAISE USING MESSAGE = 'Job failed';
  END;

END;
```

## 6. External Dependencies
The `lineage_assembled_jobs` query indicated no explicit external systems (`external_systems: []`) for this job. However, the source code reveals the following dependencies:
*   **Environment Initialization**: `$HOME/.dw_init` - This file is sourced to set up the shell environment. In the migrated BigQuery context, this will be handled by GCP project/dataset configuration or a higher-level orchestration framework like Cloud Composer.
*   **Utility Scripts**:
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`
    These scripts provide error handling, parameter parsing, and date utility functions. Their logic will need to be translated into equivalent BigQuery SQL functions or sub-procedures, or their functionality absorbed directly into the wrapper stored procedure.
*   **Core Business Logic Script**: `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_bp_ta_iccid_vertrag.ksh` - This is the critical dependency. This script contains the actual data preparation and provisioning logic and must be migrated to a BigQuery Stored Procedure (`project.dataset.k_ausd_bp_ta_iccid_vertrag`) as part of this migration or as a prerequisite. Its conversion is beyond the scope of this document but is essential for the complete job.

## 7. Unresolved / Risks
*   **Missing Complexity Tier Data**: The `file_complexity` table returned no rows for this file. This means there is no automated assessment of its complexity tier (simple, medium, complex, very_complex) or specific migration flags from this source. The "semi_auto" automation bucket provides a general hint but the detailed complexity analysis is missing.
*   **Core Script (`k_ausd_bp_ta_iccid_vertrag.ksh`) Migration**: The most significant unresolved item is the content and migration strategy for the core script invoked by this wrapper. This design assumes that `k_ausd_bp_ta_iccid_vertrag.ksh` will also be migrated to a BigQuery Stored Procedure with an interface compatible with the wrapper's call. A detailed analysis and design for `k_ausd_bp_ta_iccid_vertrag.ksh` is required for a complete end-to-end solution.
*   **Utility Script Translation**: The specific functions within `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, and `h_alis_date.ksh` need to be fully understood and translated into BigQuery SQL equivalents or incorporated into the wrapper procedure.
*   **Job Number Generation**: The current BigQuery pseudocode uses a simple `MAX(job_nr) + 1` for `v_dwh_eintragsnr`. For a robust production system, a more resilient mechanism like a sequence table or a dedicated job ID generation service might be required, especially in concurrent execution scenarios.
*   **Orchestration Beyond BigQuery**: While the wrapper logic can reside in a BigQuery Stored Procedure, if this job is part of a larger workflow or requires external scheduling, a Cloud Composer (Airflow) DAG or Cloud Scheduler job would be needed to invoke this BigQuery Stored Procedure. This is implied by the `gcp_target_hint: Cloud Composer (Airflow)` in the `file_analysis`.

## 8. Build Plan
The build plan for migrating `r_ausd_bp_ta_iccid_vertrag.ksh` involves generating BigQuery SQL artifacts:

1.  **Generate `job_audit` Table DDL (BigQuery SQL)**:
    *   Create a DDL statement for the `job_audit` table.
    *   Columns: `job_nr` (INT64), `job_kennung` (STRING), `script_name` (STRING), `log_timestamp` (TIMESTAMP), `stichtag` (DATE/STRING), `status` (STRING), `message` (STRING), etc.

2.  **Generate `ausd_bp_ta_iccid_vertrag_wrapper` Stored Procedure (BigQuery SQL)**:
    *   Create the `CREATE OR REPLACE PROCEDURE` statement for `project.dataset.ausd_bp_ta_iccid_vertrag_wrapper`.
    *   Implement parameter handling, validation, date logic, and audit logging as per the Transformation Logic section.
    *   Include the `CALL` statement to the `k_ausd_bp_ta_iccid_vertrag` BigQuery Stored Procedure.

3.  **Generate `k_ausd_bp_ta_iccid_vertrag` Stored Procedure (BigQuery SQL)**:
    *   **Prerequisite**: This step requires a separate migration design and implementation for the core script `k_ausd_bp_ta_iccid_vertrag.ksh`. The generated BigQuery Stored Procedure must accept the same parameters as intended by the wrapper.

4.  **Optional: Generate Airflow DAG (Python)**:
    *   If external orchestration is required, generate a Python Airflow DAG that schedules and invokes the `project.dataset.ausd_bp_ta_iccid_vertrag_wrapper` BigQuery Stored Procedure, passing the `Stichtag` and `Wiederanlaufwert` parameters (potentially derived from Airflow's execution date or configuration).

This build plan focuses on the BigQuery components, assuming the `k_ausd_bp_ta_iccid_vertrag.ksh` script's logic is also migrated to BigQuery.