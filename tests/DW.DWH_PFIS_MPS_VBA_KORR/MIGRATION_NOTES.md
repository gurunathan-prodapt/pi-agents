# Migration Notes: DW.DWH_PFIS_MPS_VBA_KORR

These migration notes document the transition of the legacy UC4 Unix job `DW.DWH_PFIS_MPS_VBA_KORR` and its associated scripts from an on-premises Oracle and Korn Shell (KSH) environment to Google Cloud Platform (GCP) using **Apache Airflow (Cloud Composer)** and **BigQuery**.

---

## 1. Summary

The legacy job `DW.DWH_PFIS_MPS_VBA_KORR` (Title: *Korrektur nicht ermittelbarer VBA-IDs*) is responsible for correcting unidentifiable Vertriebsart (VBA) IDs within the MPS usage fact table (`dwh$ta_f_mps_nutzung`). It aligns level-6 and level-7 IDs based on text-matching from a lookup view (`dwh$vi_l_m2_vba`) and nullifies the raw text columns once the correct IDs are written.

This migration transfers:
*   **Orchestration**: From UC4 to an Apache Airflow DAG.
*   **Execution Wrapper**: From Korn Shell (`r_pfis_mps_vba_korrektur`) to Python 3.
*   **Database Logic**: From Oracle PL/SQL (`d_pfis_mps_vba_korrektur.sql`) to BigQuery Standard SQL Scripting.

---

## 2. Generated Artifacts

The migration process generated three primary artifacts, each serving a distinct role in the target architecture:

| Artifact Path | Language / Type | Role |
| :--- | :--- | :--- |
| `dw_dwh_pfis_mps_vba_korr.py` | Python (Airflow DAG) | Orchestrates the workflow. Defines the task sequence, manages environment variables, and triggers both the Python wrapper and the BigQuery SQL script. |
| `d_pfis_mps_vba_korrektur.sql` | BigQuery SQL | Contains the core data correction logic. Rewritten from Oracle PL/SQL to BigQuery Standard SQL, utilizing transactions and `MERGE` statements. |
| `r_pfis_mps_vba_korrektur.py` | Python 3 | Replaces the legacy KSH wrapper script. Handles command-line argument parsing (`-v`, `-h`), local logging, and environment validation. |

---

## 3. Key Design Decisions

### 3.1. BigQuery `MERGE` instead of Oracle `ROWID` Updates
*   **Legacy Approach**: The original Oracle SQL script used self-correlated updates matching on physical `ROWID` combined with Oracle-proprietary outer join syntax `(+)`.
*   **Migrated Approach**: BigQuery does not support physical `ROWID` or `(+)` syntax. These updates were refactored into high-performance, set-based `MERGE` statements. By grouping the lookup view (`dwh$vi_l_m2_vba`) and taking the `MIN(m2_vba_ebene7_id)`, we guarantee that the `MERGE` source has unique keys, preventing runtime "multiple source rows matched" errors in BigQuery.

### 3.2. Transactional Integrity (`BEGIN TRANSACTION` / `COMMIT` / `ROLLBACK`)
*   **Decision**: To preserve the atomic nature of the legacy script, the BigQuery SQL script is wrapped in a standard scripting `BEGIN TRANSACTION` and `COMMIT TRANSACTION` block.
*   **Exception Handling**: An `EXCEPTION WHEN ERROR THEN` block catches any runtime failures, executes a `ROLLBACK TRANSACTION`, captures error metadata via system variables (`@@error.message` and `@@error.code`), and raises a native BigQuery error to halt downstream orchestration.

### 3.3. Python 3 for the Wrapper Script
*   **Decision**: The legacy shell script `r_pfis_mps_vba_korrektur` was migrated to Python 3 rather than Bash.
*   **Reasoning**: Python provides cross-platform portability, robust exception handling, and cleaner integration with modern cloud logging frameworks. It replaces legacy `getopts` with `argparse` and safely emulates the original signal traps.

### 3.4. Parameterization via Airflow
*   **Decision**: The legacy SQL script accepted a positional parameter (`&1`) representing a log entry ID. This is mapped to a BigQuery query parameter `@P1`.
*   **Implementation**: The Airflow `BigQueryInsertJobOperator` passes this parameter dynamically using the Airflow task instance try number (`{{ task_instance.try_number }}`) as a runtime placeholder.

---

## 4. Manual Steps Before Go-Live

Before enabling the DAG in production, the following setup steps must be completed in the target GCP environment:

### 4.1. Schema and Dataset Creation
Ensure that the target BigQuery dataset exists and contains the required tables and views:
1.  **Fact Table**: `dwh$ta_f_mps_nutzung`
2.  **Lookup View**: `dwh$vi_l_m2_vba`

### 4.2. Deploy Logging Stored Procedure Stub
The migrated SQL script calls a custom logging procedure `dwpa_meldung_fehler` upon failure. If this procedure does not exist, the script will fail to compile. Deploy the following stub (or map it to your enterprise logging framework) in the target dataset:

```sql
CREATE OR REPLACE PROCEDURE your_dataset.dwpa_meldung_fehler(
  severity STRING,
  eintrags_nr INT64,
  fehler_nr INT64,
  err_text STRING,
  err_code STRING
)
BEGIN
  -- Insert into a centralized log table or output to system logs
  INSERT INTO your_dataset.migration_error_logs (log_time, severity, eintrags_nr, fehler_nr, error_message, error_code)
  VALUES (CURRENT_TIMESTAMP(), severity, eintrags_nr, fehler_nr, err_text, err_code);
END;
```

### 4.3. IAM & Permissions
The service account running the Cloud Composer worker nodes must have the following IAM roles:
*   **BigQuery Data Editor** on the target dataset.
*   **BigQuery Job User** on the GCP project.
*   **Storage Object Viewer** (if SQL templates are loaded from GCS).

### 4.4. Airflow Variables
Define the following Airflow Variable in the Airflow UI (**Admin -> Variables**):
*   **Key**: `GCP_PROJECT`
*   **Value**: *[Your GCP Project ID]*

### 4.5. Environment Variables
The Python wrapper script requires the environment variable `DW_DIR_ROOT` to resolve paths. Ensure this is configured in your Cloud Composer environment variables:
*   **Name**: `DW_DIR_ROOT`
*   **Value**: `/home/airflow/gcs/dags` (or the directory where your SQL scripts are deployed).

---

## 5. Known Gaps & Unresolved References

### 5.1. Missing Legacy Shell Libraries
*   **Gap**: The legacy script sourced several utility files (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`, `h_alis_sqlplus.ksh`) which were not provided in the migration bundle.
*   **Resolution**: The Python wrapper script has been decoupled from these shell-specific utilities. Standard Python libraries (`argparse`, `os`, `sys`) are used instead.

### 5.2. Legacy `DWMSG` Logging Framework
*   **Gap**: The legacy script relied on `DWMSG_ErmittleNr`, `DWMSG_Logdateiname`, `DWMSG_ErzeugeEintrag`, and `DWMSG_SetzeStatusOK` to register job states in an Oracle-backed metadata repository.
*   **Resolution**: These calls have been bypassed or mocked in the Python wrapper. It is highly recommended to integrate these steps with native **Google Cloud Logging** or a custom metadata tracking table in BigQuery.

---

## 6. Validation

To validate the migration, perform the following tests in a non-production environment:

### 6.1. Unit Testing the BigQuery SQL Script
1.  Populate `dwh$ta_f_mps_nutzung` with dummy records containing unmatched `m2_vba_ebene6_text` and `m2_vba_ebene7_text` values.
2.  Execute `d_pfis_mps_vba_korrektur.sql` manually in the BigQuery console, passing a dummy value for `@P1`.
3.  **Verify Success Criteria**:
    *   The corresponding `m2_vba_ebene6_id` and `m2_vba_ebene7_id` columns are updated with correct IDs from `dwh$vi_l_m2_vba`.
    *   The text columns `m2_vba_ebene6_text` and `m2_vba_ebene7_text` are nullified for successfully matched rows.
    *   Rows with unmatched texts retain their default IDs and their text columns remain intact.

### 6.2. DAG Execution Test
1.  Trigger the `dw_dwh_pfis_mps_vba_korr` DAG manually from the Airflow UI.
2.  Verify that all three tasks (`dw_dwh_pfis_mps_vba_korr_task` -> `run_r_pfis_mps_vba_korrektur` -> `run_d_pfis_mps_vba_korrektur_sql`) complete with a `success` state.
3.  Check the Airflow task logs for `run_r_pfis_mps_vba_korrektur` to ensure the Python wrapper executed and logged headers correctly.

---

## 7. Rollback Procedure

In the event of an unexpected failure or data corruption during or after go-live, execute the following rollback steps:

1.  **Pause the Airflow DAG**:
    ```bash
    airflow dags pause dw_dwh_pfis_mps_vba_korr
    ```
2.  **Restore the Fact Table**:
    Since the SQL script performs in-place updates, restore the `dwh$ta_f_mps_nutzung` table to its pre-execution state using BigQuery Time Travel:
    ```sql
    CREATE OR REPLACE TABLE `your_project.your_dataset.dwh$ta_f_mps_nutzung` AS
    SELECT * FROM `your_project.your_dataset.dwh$ta_f_mps_nutzung`
    FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR);
    ```
    *(Adjust the interval to target a timestamp immediately preceding the failed job run).*
3.  **Investigate Logs**:
    Review the `migration_error_logs` table and Cloud Composer task logs to identify the root cause of the failure.