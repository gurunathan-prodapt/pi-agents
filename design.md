# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_austausch.ksh

## 1. Purpose & Scope

This document outlines the migration design for the KornShell script `r_ausd_austausch.ksh`, which is responsible for preparing and providing a snapshot extract of the contract cache base table for the BERT report and for Forderungsscoring. The script orchestrates the execution of a core data preparation script (`k_ausd_austausch.ksh`), handles parameter processing, date logic, error handling, and logging.

The job `5af228f1` is assembled from this single component file.

**Source System:** Unix/Linux environment running KornShell scripts, likely invoked by a UC4 scheduler.
**Target Platform:** Google BigQuery for data processing and storage, potentially complemented by Cloud Composer/Cloud Workflows for orchestration.

## 2. Source Inventory

**File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_austausch.ksh`
*   **Technology:** KornShell script
*   **Category:** Shell script
*   **Tool (Detected):** KornShell
*   **Summary:** Orchestrates a core data preparation script, handles parameters, date logic, error handling, and logging for BERT reports.
*   **File Purpose:** Orchestrator/ETL
*   **Complexity Tier:** Not explicitly available from `file_complexity`. Given its role as an orchestrator invoking other scripts and using utility functions, it's likely "medium" to "complex".
*   **Automation Bucket:** Not explicitly available from `automation_rate`. Due to its orchestration nature, it will likely be "semi_auto" (B2) requiring a combination of automated conversion for core logic and manual design for orchestration.

## 3. Target Architecture

The migrated solution will primarily reside within Google BigQuery as stored procedures. Orchestration aspects will be managed either directly within BigQuery (for simple sequencing) or using Google Cloud Composer (Apache Airflow) / Cloud Workflows for more complex dependencies and external system interactions.

**BigQuery Components:**
*   **Main Stored Procedure:** `project.dataset.BERT_AUSTAUSCH_KSH`
    *   This procedure will replicate the orchestration logic of the original `r_ausd_austausch.ksh` script, handling parameter parsing, date determination, logging, and invoking the core data preparation logic.
*   **Core Logic Stored Procedure:** `project.dataset.k_ausd_austausch` (placeholder)
    *   This procedure will contain the actual data extraction, transformation, and loading logic that was originally within `k_ausd_austausch.ksh` (invoked by `r_ausd_austausch.ksh`). This will be the main data processing component.
*   **Logging Table:** `project.dataset.job_log`
    *   A BigQuery table to store job execution logs, error messages, and status updates, replacing file-based logging.
*   **Job Status Table:** `project.dataset.job_status`
    *   A BigQuery table to track the overall status of the job runs, similar to how the shell script updates status.
*   **Source Data Tables:** `project.dataset.contract_cache` (hypothesized)
    *   BigQuery tables holding the source data (e.g., contract information) from which the snapshot is extracted.
*   **Target Data Table:** `project.dataset.fos_table` (hypothesized)
    *   BigQuery table where the prepared data snapshot is written for Forderungsscoring.

## 4. Data Flow & Lineage

The current script `r_ausd_austausch.ksh` acts as a wrapper.

**Legacy Flow:**
1.  **Invocation:** The script `r_ausd_austausch.ksh` is invoked by a UC4 job, specifically `DW.BERT_P_AUSTAUSCH.xml`.
2.  **Parameter Handling & Setup:** `r_ausd_austausch.ksh` processes command-line arguments (`-s` for Stichtag, `-l` for Wiederanlaufwert), sources environment and utility scripts (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`), and initializes logging.
3.  **Core Script Invocation:** `r_ausd_austausch.ksh` then executes the core data preparation script `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_austausch.ksh`, passing validated parameters to it.
4.  **Logging & Exit:** After `k_ausd_austausch.ksh` completes, `r_ausd_austausch.ksh` handles final logging and exits with an appropriate status code.

**Migrated Flow (BigQuery centric):**
1.  **Orchestration Layer:**
    *   A Cloud Composer DAG (or Cloud Workflows) will replace the UC4 job to initiate the BigQuery process.
    *   This orchestration layer will call the `project.dataset.BERT_AUSTAUSCH_KSH` BigQuery Stored Procedure, passing required parameters.
2.  **`BERT_AUSTAUSCH_KSH` Stored Procedure:**
    *   Handles parameter validation, defaults (e.g., `Stichtag` to current date if not provided, `Wiederanlaufwert` to 0).
    *   Records job start and metadata into `project.dataset.job_log`.
    *   Invokes the `project.dataset.k_ausd_austausch` BigQuery Stored Procedure, passing the necessary execution context.
    *   Records job success or failure into `project.dataset.job_log` and updates `project.dataset.job_status`.
3.  **`k_ausd_austausch` Stored Procedure:**
    *   Performs the core data processing. This will involve:
        *   Reading data from source BigQuery tables (e.g., `project.dataset.contract_cache`).
        *   Applying filters based on `Stichtag` (`Gueltig_von <= Stichtag < Gueltig_bis AND LADEDATUM < Stichtag`).
        *   Applying filters based on `Wiederanlaufwert` (`DWH_VERTRAG_ID > restart_threshold`).
        *   Deleting existing data in the target table where `dwh_vertrag_id >= restart_threshold`.
        *   Inserting the filtered snapshot into the target BigQuery table (e.g., `project.dataset.fos_table`).
    *   Records processing steps into `project.dataset.job_log`.

## 5. Transformation Logic

The `r_ausd_austausch.ksh` script primarily serves an orchestration role. Its transformation logic is minimal:

*   **Parameter Defaulting:**
    *   `p_wiederanlaufWert` is initialized to `0` if not provided. This will be translated to `IFNULL(p_wiederanlaufWert, 0)` in BQSQL.
    *   `p_stichtag` defaults to `v_sysdate` (current system date in `DDMMYYYY` format) if not provided. This will be translated to `FORMAT_DATE('%d%m%Y', CURRENT_DATE())` if `p_stichtag IS NULL OR p_stichtag = ''`.
*   **Error Handling:** The script uses `set -e` and `trap` for error management. In BigQuery, this will be handled by `EXCEPTION WHEN ERROR THEN` blocks within stored procedures, and error details will be logged to `project.dataset.job_log`.
*   **Logging:** The script uses helper functions (`DWMSG_ErmittleNr`, `DWMSG_Logdateiname`, `DWMSG_ErzeugeEintrag`, etc.) and redirects output to a log file (`>> $LogDatei 2>&1`). In BigQuery, this will be replaced by `INSERT` statements into the `project.dataset.job_log` table.
*   **Invocation:** The key transformation is replacing the shell `exec` call to `k_ausd_austausch.ksh` with a `CALL` to the corresponding BigQuery stored procedure `project.dataset.k_ausd_austausch`.

The actual data transformations (filtering, selection, insertion) are expected to be present in the `k_ausd_austausch.ksh` script. The MCP tool's pseudocode provides a high-level representation of these rules for the `k_ausd_austausch` BQ procedure:

```sql
  -- Select records where:
  --   Gueltig_von <= Stichtag < Gueltig_bis
  --   AND LADEDATUM < Stichtag
  --   AND DWH_VERTRAG_ID > restart_threshold

  -- Delete existing entries >= restart threshold before insert/reload
  DELETE FROM `project.dataset.fos_table`
  WHERE dwh_vertrag_id >= v_restart_threshold;

  -- Insert filtered snapshot into target table
  INSERT INTO `project.dataset.fos_table`
  SELECT
    *
  FROM tmp_filtered_contracts;
```

## 6. External Dependencies

The original script has several dependencies that need to be addressed in the migration:

1.  **UC4 Scheduler:** The UC4 job `DW.BERT_P_AUSTAUSCH.xml` invokes `r_ausd_austausch.ksh`.
    *   **Replacement:** This will be replaced by a Google Cloud Composer (Airflow) DAG or Cloud Workflows, which will be responsible for scheduling and initiating the BigQuery stored procedure.
2.  **Sourced Shell Scripts:**
    *   `. $HOME/.dw_init`: Environment initialization.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`: Error handling framework.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`: Parameter parsing helpers.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`: Date handling utilities.
    *   **Replacement:**
        *   Environment variables will be managed through Cloud Composer environment variables or directly configured within the BigQuery project/dataset structure.
        *   Error handling, parameter validation, and date functions will be reimplemented using native BigQuery SQL functions and control flow within the stored procedures. The `DWDate_Gib_Zeitraum` function will map to BigQuery's `CURRENT_DATE()` and `FORMAT_DATE()`.
3.  **Core Script (`k_ausd_austausch.ksh`):** This is the main business logic component invoked by `r_ausd_austausch.ksh`.
    *   **Replacement:** This script's logic will be fully migrated into the BigQuery Stored Procedure `project.dataset.k_ausd_austausch`.
4.  **Mail Notification:** The commented-out `mail` command suggests a potential email notification system.
    *   **Replacement:** If required, this can be implemented via Cloud Composer's notification features (e.g., email operators) or by integrating with Cloud Functions/Cloud Run that send notifications upon specific BigQuery job events or log entries.

No explicit external systems (like Oracle, SFTP, S3) were found to be directly referenced *by this specific script*. However, the underlying data sources and targets that `k_ausd_austausch.ksh` interacts with might have external system dependencies, which are outside the scope of this particular migration design but will be covered when `k_ausd_austausch.ksh` itself is analyzed.

## 7. Unresolved / Risks

1.  **Missing Complexity and Automation Rate:** The `file_complexity` and `automation_rate` tables did not contain entries for `r_ausd_austausch.ksh`. This means a formal assessment of its migration effort and automation potential is missing, requiring a manual review.
2.  **`k_ausd_austausch.ksh` Detail:** The full logic of the core script `k_ausd_austausch.ksh` is not detailed in this design. Its migration will require a separate, detailed analysis. The current design assumes it will be migrated to a single BigQuery stored procedure.
3.  **`FOSHoleLadedatum`:** The commented-out line `FOSHoleLadedatum "DWH$TA_C_VERTRAG" v_ladedatum` suggests that the `Stichtag` could historically have been determined by `MIN(sysdate, max_load_date)` from a table (`DWH$TA_C_VERTRAG`). While the active code uses `v_sysdate`, it's important to verify if this historical logic needs to be considered for any re-implementation of `k_ausd_austausch.ksh` to maintain data integrity.
4.  **`DWH_VERTRAG_ID` and `FOS-Tabelle` Schema:** The exact schemas for `DWH_VERTRAG_ID`, `contract_cache` (hypothesized `DWH$TA_C_VERTRAG`), `fos_table`, `Gueltig_von`, `Gueltig_bis`, and `LADEDATUM` are unknown and will need to be discovered and mapped to BigQuery types.
5.  **Error Code Handling:** The specific meaning and handling of `ErrNr` values (192, 193) will need to be translated into meaningful BigQuery error messages or logging categories.

## 8. Build Plan

The build plan focuses on generating the necessary BigQuery components.

1.  **Define BigQuery Dataset:**
    *   Create a BigQuery dataset (e.g., `project.dataset`) to house the migrated procedures and tables.
    *   **Language:** DDL/Console
2.  **Create `job_log` Table:**
    *   Design and create the DDL for `project.dataset.job_log` to capture job execution details.
    *   **Language:** BigQuery DDL
3.  **Create `job_status` Table:**
    *   Design and create the DDL for `project.dataset.job_status` to track job run statuses.
    *   **Language:** BigQuery DDL
4.  **Create `k_ausd_austausch` Stored Procedure:**
    *   Translate the business logic of the legacy `k_ausd_austausch.ksh` script into a BigQuery Stored Procedure: `project.dataset.k_ausd_austausch`. This will involve:
        *   Defining input parameters: `p_jobkennung`, `p_stichtag`, `p_eintragsnr`, `p_wiederanlaufWert`.
        *   Implementing the data filtering logic (based on `Gueltig_von`, `Gueltig_bis`, `LADEDATUM`, `DWH_VERTRAG_ID`).
        *   Implementing the `DELETE` and `INSERT` operations into the `fos_table`.
        *   Integrating logging into `project.dataset.job_log`.
    *   **Language:** BigQuery SQL Stored Procedure
5.  **Create `BERT_AUSTAUSCH_KSH` Stored Procedure:**
    *   Generate the BigQuery Stored Procedure `project.dataset.BERT_AUSTAUSCH_KSH` based on the provided pseudocode from the MCP tool.
    *   This procedure will encapsulate the orchestration logic, parameter handling, error handling, logging, and call to `project.dataset.k_ausd_austausch`.
    *   **Language:** BigQuery SQL Stored Procedure
6.  **Create Orchestration (Cloud Composer/Workflows):**
    *   Design and implement a Cloud Composer DAG or Cloud Workflow to trigger the `project.dataset.BERT_AUSTAUSCH_KSH` stored procedure. This will replace the UC4 scheduler.
    *   **Language:** Python (for Airflow DAG) or YAML/JSON (for Cloud Workflows)
7.  **Data Ingestion for Source Tables:**
    *   Ensure source data for `contract_cache` (and potentially `DWH$TA_C_VERTRAG` if needed for `max_load_date` logic) is ingested into BigQuery.
    *   **Language:** Varies (e.g., `gsutil`, `bq load`, Dataflow, Dataproc)
8.  **Target Table DDL:**
    *   Create the DDL for `project.dataset.fos_table`.
    *   **Language:** BigQuery DDL