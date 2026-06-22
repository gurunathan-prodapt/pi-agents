# Migration Summary for r_ausd_bp_ta_bpr_evn.ksh

This document summarizes the migration of the KornShell script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_evn.ksh` to Google BigQuery.

## Original Job Purpose
The original script served as an orchestration wrapper for provisioning basic product data for the BERT system, generating a snapshot of the Data Warehouse (DWH) contract cache for demand scoring. It handled parameter parsing (reference date `Stichtag`, restart value `Wiederanlaufwert`), error handling, and invoked a core script (`k_ausd_bp_ta_bpr_evn.ksh`) for the actual data processing.

## Target Architecture Components

1.  **BigQuery Stored Procedures:**
    *   `project.dataset.sp_r_ausd_bp_ta_bpr_evn`: This is the main orchestration stored procedure, replacing the wrapper KSH script. It handles:
        *   Parameter reception (`p_stichtag_in`, `p_wiederanlaufWert_in`).
        *   Defaulting of parameters (e.g., `Stichtag` to current date, `Wiederanlaufwert` to 0).
        *   Parameter validation.
        *   Job auditing to the `job_audit` table.
        *   Invocation of the core logic procedure (`sp_k_ausd_bp_ta_bpr_evn`).
        *   Robust error handling using `EXCEPTION WHEN ERROR` blocks, logging errors to `job_audit`.
    *   `project.dataset.sp_k_ausd_bp_ta_bpr_evn`: This procedure is designed to encapsulate the core data transformation logic identified from the original `k_ausd_bp_ta_bpr_evn.ksh` script. It performs:
        *   Conditional deletion of records from `project.dataset.fos_target_table` based on `p_wiederanlaufWert`.
        *   Extraction, filtering, and insertion of data from `project.dataset.source_contract_cache` into `project.dataset.fos_target_table`, applying date criteria (`gueltig_von`, `gueltig_bis`, `ladedatum`) and the `Wiederanlaufwert` filter (`dwh_vertrag_id`).
        *   **Note:** The exact column list for `SELECT *` and the precise schema of `source_contract_cache` and `fos_target_table` require detailed analysis of the original `k_ausd_bp_ta_bpr_evn.ksh`.

2.  **BigQuery Tables:**
    *   `project.dataset.job_audit`: A new table to centralize job execution logs, parameters, status, and error information, replacing file-based logging.
    *   `project.dataset.source_contract_cache`: Represents the migrated DWH contract cache source table.
    *   `project.dataset.fos_target_table`: The target table for the processed demand scoring data.

3.  **Orchestration (Airflow Example):**
    *   `airflow_dags/dag_r_ausd_bp_ta_bpr_evn.py`: An example Airflow DAG demonstrating how the BigQuery Stored Procedure `sp_r_ausd_bp_ta_bpr_evn` can be triggered. It shows how to pass parameters like `Stichtag` (e.g., using Airflow macros like `{{ ds_nodash }}`).

## Transformation Highlights

*   **Parameter Handling:** `getopts` logic is replaced by BigQuery Stored Procedure input parameters, with internal variables for defaulting and validation.
*   **Error Handling:** Shell `trap` mechanisms are replaced by BigQuery's `BEGIN...EXCEPTION WHEN ERROR` blocks, providing robust error capture and logging.
*   **Logging:** File-based logging (`DWMSG_` functions) is centralized into the `project.dataset.job_audit` BigQuery table.
*   **Date Operations:** KSH date utilities are replaced by BigQuery SQL functions like `CURRENT_DATE()`, `FORMAT_DATE()`, and `PARSE_DATE()`.
*   **Restartability:** The `Wiederanlaufwert` logic for conditional deletion and insertion is preserved, allowing for idempotent and restartable job executions.

## Further Considerations / Risks Addressed

*   **Core Script Logic (`k_ausd_bp_ta_bpr_evn.ksh`):** The provided `sp_k_ausd_bp_ta_bpr_evn.sql` is a framework based on the design document's summary. A thorough analysis of the original `k_ausd_bp_ta_bpr_evn.ksh` is crucial to fully implement the `SELECT` statement's column list and any complex transformations.
*   **Target Table Deletion:** The `DELETE` statement in `sp_k_ausd_bp_ta_bpr_evn` currently deletes based solely on `dwh_vertrag_id`. If `fos_target_table` contains data for multiple `Stichtag` values, it might be necessary to refine the `DELETE` logic to include `Stichtag` to avoid unintended data loss.
*   **Dynamic `MAX(ladedatum)`:** If the commented-out `FOSHoleLadedatum` logic needs to be reactivated, an additional BigQuery query to find `MAX(ladedatum)` from the relevant source table would be integrated into `sp_r_ausd_bp_ta_bpr_evn` or `sp_k_ausd_bp_ta_bpr_evn`.
*   **`project` and `dataset` placeholders:** All generated BigQuery SQL uses `project.dataset` as placeholders, which must be replaced with actual GCP project IDs and BigQuery dataset IDs during deployment.