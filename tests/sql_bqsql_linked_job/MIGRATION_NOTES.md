# Migration Notes: DW.BERT_AUSD_V_TA_PERIOD

This document details the migration of the legacy UC4 UNIX job `DW.BERT_AUSD_V_TA_PERIOD` and its associated KornShell and Oracle SQL scripts to Google Cloud Platform (GCP) using Cloud Composer (Apache Airflow) and BigQuery.

---

## 1. Summary
The legacy UC4 job `DW.BERT_AUSD_V_TA_PERIOD` has been migrated from an on-premises Oracle/Unix environment to **Google Cloud Platform (GCP)**. 

*   **Source Components**: 
    *   UC4 UNIX Job: `DW.BERT_AUSD_V_TA_PERIOD`
    *   Wrapper Shell Script: `r_ausd_v_ta_period.ksh`
    *   Control Shell Script: `k_ausd_v_ta_period.ksh`
    *   Oracle SQL Script: `d_ausd_v_ta_period.sql`
*   **Target Platform**: 
    *   **Orchestration**: Cloud Composer (Apache Airflow)
    *   **Execution Runtime**: Python 3 (for wrapper and control logic)
    *   **Database & Query Engine**: BigQuery (replacing Oracle and DB Links)

### Business Purpose
This workflow mirrors Carmen period definitions into the Isbert Data Warehouse schema (`sof$ta_period`) to support downstream contract and reporting alignment processes.

---

## 2. Generated Artifacts

The migration process generated the following files:

1.  **`sql_bqsql_linked_job/DWH_BERT_JOB/dw_bert_ausd_v_ta_period.py`** (Airflow DAG)
    *   *Role*: Replaces the UC4 scheduler. It defines the DAG structure, execution parameters, retries, and schedules. It contains a placeholder task `bert_ausd_v_ta_period` to trigger the execution.
2.  **`sql_bqsql_linked_job/isbert/aufbereitung/bin/r_ausd_v_ta_period.py`** (Python Wrapper Script)
    *   *Role*: Replaces `r_ausd_v_ta_period.ksh`. It initializes job tracking, manages execution logs, handles system signals/traps, and calls the core control script.
3.  **`sql_bqsql_linked_job/isbert/aufbereitung/bin/k_ausd_v_ta_period.py`** (Python Control Script)
    *   *Role*: Replaces `k_ausd_v_ta_period.ksh`. It parses command-line arguments, validates parameters, and triggers the execution of the BigQuery SQL script.
4.  **`sql_bqsql_linked_job/isbert/aufbereitung/sql/d_ausd_v_ta_period.sql`** (BigQuery SQL Script)
    *   *Role*: Replaces the Oracle SQL script. It dynamically retrieves the business cutoff date, truncates the target table `sof$ta_period`, and populates it using BigQuery SQL syntax.

---

## 3. Key Design Decisions

*   **KornShell to Python 3 Translation**: The shell scripts (`.ksh`) were converted to Python 3 (`.py`) to align with modern cloud-native execution environments, improve error handling, and leverage standard Python libraries (like `argparse` and `subprocess`).
*   **Dynamic SQL with `EXECUTE IMMEDIATE`**: To avoid hardcoding GCP Project IDs and BigQuery Dataset names, the SQL script uses `EXECUTE IMMEDIATE` with parameterized variables (`@gcp_project`, `@bq_dataset`, `@carmen_bq_dataset`). This ensures the script is environment-agnostic and can be deployed seamlessly across Dev, Test, and Prod environments.
*   **Retirement of Legacy Utilities**: Sourced shell utilities such as `h_alis_parameter.ksh`, `h_alis_date.ksh`, and `.dw_init` were retired. Their functionalities were replaced by native Python libraries (`argparse`, `datetime`) and Cloud Composer environment variables.
*   **Preservation of German Log Messages**: To maintain compatibility with legacy log parsers and monitoring tools, all print statements and log messages (e.g., `"Bitte ueber Rahmenscript aufrufen"`, `"Die Abarbeitung wurde ohne erkennbare Fehler beendet"`) were preserved verbatim in German.
*   **Database Link Elimination**: The legacy Oracle DB Link (`@pcrs1.de.tinternal.com`) was retired. It is replaced by direct BigQuery table references, assuming the Carmen source tables are replicated into a BigQuery dataset.

---

## 4. Manual Steps Before Go-Live

Before deploying and running this workflow in production, the following manual setup steps must be completed:

### Schema & Dataset Creation
1.  Ensure the target BigQuery dataset (e.g., `isbert_schema`) exists in your GCP project.
2.  Ensure the replicated Carmen source dataset exists and contains the following tables:
    *   `cds$ta_period`
    *   `CDS$TA_TIME_MEAS_CV`
    *   `cds$ta_description`
3.  Create the target table `sof$ta_period` in the target dataset with the appropriate schema.

### IAM & Permissions
1.  The Cloud Composer service account must be granted the following IAM roles:
    *   `BigQuery Data Editor` on the target dataset (`isbert_schema`).
    *   `BigQuery Data Viewer` on the replicated Carmen source dataset.
    *   `BigQuery Job User` on the GCP project.
2.  If running the Python scripts on a GKE Pod or Compute Engine VM, ensure the execution service account has read permissions for the scripts.

### Airflow Variables
Configure the following Airflow Variables in your Cloud Composer environment:
*   `GCP_PROJECT`: The target GCP Project ID.
*   `GCP_REGION`: The GCP region (e.g., `europe-west3`).
*   `BERT_DIR_ROOT`: The root directory path where the migrated Python scripts reside.
*   `DW_DIR_UTL`: The directory path used for writing temporary execution metrics.

### Scheduling
The DAG is currently configured with `schedule=None` (manual or external trigger only), matching the legacy standalone behavior. If this job needs to run on a specific time schedule, update the `schedule` parameter in `dw_bert_ausd_v_ta_period.py`.

---

## 5. Known Gaps & Unresolved References

*   **Airflow Operator Configuration**: The task `bert_ausd_v_ta_period` in `dw_bert_ausd_v_ta_period.py` is currently defined as an `EmptyOperator`. This is a placeholder. Before go-live, this must be updated to a `BashOperator`, `SSHOperator`, or `KubernetesPodOperator` to execute the wrapper script:
    ```python
    # Example replacement:
    bert_ausd_v_ta_period = BashOperator(
        task_id='bert_ausd_v_ta_period',
        bash_command='python3 $BERT_DIR_ROOT/aufbereitung/bin/r_ausd_v_ta_period.py',
        env={'BERT_DIR_ROOT': Variable.get('BERT_DIR_ROOT'), 'DW_DIR_UTL': Variable.get('DW_DIR_UTL')}
    )
    ```
*   **Shared Library Dependency**: `k_ausd_v_ta_period.py` imports `starteSQLSkript` from `h_alis_sqlplus`. Ensure that the migrated `h_alis_sqlplus.py` utility is present in the Python search path (`PYTHONPATH`) or packaged within the environment.
*   **Upstream Data Replication**: This workflow assumes that the Carmen source tables are already replicated to BigQuery. The replication pipeline must be fully operational before this job is scheduled.

---

## 6. Validation

To validate the migration, perform the following tests:

### Dry-Run Validation
1.  Execute the BigQuery SQL script manually in the BigQuery console, replacing the `@gcp_project`, `@bq_dataset`, and `@carmen_bq_dataset` parameters with your test environment details. Verify that the query compiles and executes without syntax errors.
2.  Run the Python scripts locally or in a development container:
    ```bash
    export BERT_DIR_ROOT="/path/to/migrated/code"
    export DW_DIR_UTL="/tmp"
    python3 r_ausd_v_ta_period.py
    ```

### Verification of "Passing" State
The migration is considered successful and "passing" when:
1.  The Airflow DAG runs and completes with a `SUCCESS` status.
2.  The target table `sof$ta_period` is successfully truncated and repopulated.
3.  The row count in `sof$ta_period` matches the expected count from the source tables based on the `v_datum` cutoff.
4.  The execution log file is created in the directory specified by `DW_DIR_UTL` and contains the verbatim success message:
    `Die Abarbeitung wurde ohne erkennbare Fehler beendet`

---

## 7. Rollback Procedure

In the event of a critical failure or data corruption during deployment, follow these rollback steps:

1.  **Pause the Airflow DAG**: Pause the `dw_bert_ausd_v_ta_period` DAG in the Cloud Composer Airflow UI to prevent further executions.
2.  **Restore Target Table**: Restore the BigQuery table `sof$ta_period` to its pre-migration state using BigQuery Time Travel or from a backup snapshot:
    ```sql
    -- Example Time Travel Restore (restoring to 1 hour ago)
    CREATE OR REPLACE TABLE `isbert_schema.sof$ta_period`
    AS SELECT * FROM `isbert_schema.sof$ta_period`
    FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR);
    ```
3.  **Re-enable Legacy Job**: Re-enable the legacy UC4 job `DW.BERT_AUSD_V_TA_PERIOD` on the on-premises scheduler to resume legacy execution.