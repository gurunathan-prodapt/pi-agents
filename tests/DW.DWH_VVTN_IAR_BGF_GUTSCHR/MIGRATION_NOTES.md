# Migration Notes: DW.DWH_VVTN_IAR_BGF_GUTSCHR

This document provides comprehensive migration notes for transitioning the legacy UC4 Unix job `DW.DWH_VVTN_IAR_BGF_GUTSCHR` to Apache Airflow (Google Cloud Composer).

---

## 1. Summary

The legacy UC4 job `DW.DWH_VVTN_IAR_BGF_GUTSCHR` has been migrated to Apache Airflow. The primary responsibility of this workflow is to preprocess and transform "Gutschrift" (credit note) files into a single, unified CSV format. 

* **Source Platform:** UC4 (Automic Workload Automation) running on a legacy Unix host (`|DWHDWH1P|HOST`).
* **Target Platform:** Apache Airflow (Google Cloud Composer) running on Google Cloud Platform (GCP).
* **Core Logic:** The workflow orchestrates file-level validation, metadata injection (appending footers with source filenames), and consolidation of data streams.

---

## 2. Generated Artifacts

The migration process generated three primary files, each serving a distinct role in the target architecture:

| File Path | Language | Role |
| :--- | :--- | :--- |
| `DWH_IAR_BGF_GUTSCHRIFT_JOB/dw_dwh_vvtn_iar_bgf_gutschr.py` | Python (Airflow DAG) | **Orchestrator:** Defines the Airflow DAG structure, handles dynamic date calculations (`Month_ID`), prints execution logs, and defines task dependencies. |
| `isdwh/vorverarbeitung/tn/awk/k_vvtn_iar_bgf_gutsch_foot.py` | Python 3 | **Metadata Preprocessor:** Replaces the legacy AWK script `k_vvtn_iar_bgf_gutsch_foot.awk`. It streams input records and injects the source filename (`FLNM`) into each row. |
| `isdwh/vorverarbeitung/tn/awk/k_vvtn_iar_bgf_gutschrift.py` | Python 3 | **Data Validator:** Replaces the legacy AWK script `k_vvtn_iar_bgf_gutschrift.awk`. It performs strict row-by-row schema validation (asserting exactly 25 fields) and tags valid rows. |

---

## 3. Key Design Decisions

### Python 3 Chosen Over BigQuery SQL for AWK Migrations
While BigQuery SQL is the preferred target for data transformations, both AWK scripts were migrated to standalone Python 3 scripts due to the following architectural constraints:
1. **Out-of-Band Metadata Injection (`k_vvtn_iar_bgf_gutsch_foot.awk`):** The script requires injecting an external execution-time parameter (`FLNM`, representing the input filename) into every processed row. This is a BQSQL-disqualifying condition because standard declarative SQL cannot dynamically bind external file-stream metadata to individual rows during a stateless load.
2. **Process-Level Exit Codes (`k_vvtn_iar_bgf_gutschrift.awk`):** The validation script acts as an execution gatekeeper. If a row does not contain exactly 25 fields, the script must immediately halt execution and return a process-level exit code of `2` (`exit 2`) to fail the pipeline. BigQuery SQL cannot natively raise custom OS-level exit codes to an external orchestrator mid-query.

### Dynamic Date Resolution via Airflow Macros
The legacy UC4 variable `&LASTMONTH_YYYYMM` has been replaced with a native Airflow Jinja macro:
```python
"{{ (data_interval_start.add(months=-1)).strftime('%Y%m') }}"
```
This ensures that the `Month_ID` is calculated dynamically relative to the DAG's logical execution date, maintaining idempotency during backfills.

### Preservation of Print Literals
To maintain strict character-for-character compliance with legacy logging and downstream log-scraping utilities, the exact print statement has been preserved:
```python
print(f"Lastmonth is {month_id}")
```

---

## 4. Manual Steps Before Go-Live

Before activating the migrated DAG in production, the following configuration and environment setup steps must be completed:

### 1. Airflow Variables Setup
Ensure the following Airflow Variables are defined in your Cloud Composer environment:
* `GCP_PROJECT`: The target Google Cloud Project ID.
* `DATAPROC_REGION`: The GCP region where compute resources are located.
* `DATAPROC_CLUSTER`: The name of the Dataproc cluster (if applicable).
* `GCS_BUCKET`: The Google Cloud Storage bucket replacing the legacy `$HOME` directory for file storage.

### 2. GCS Directory Structure
Create the target directory structure within the designated `GCS_BUCKET`:
* `gs://<GCS_BUCKET>/aktuell/vorverarbeitung/tn/bin/`
* `gs://<GCS_BUCKET>/isdwh/vorverarbeitung/tn/awk/`

Upload the migrated Python scripts (`k_vvtn_iar_bgf_gutsch_foot.py` and `k_vvtn_iar_bgf_gutschrift.py`) to their respective `awk` directories.

### 3. IAM & Permissions
The service account running the Cloud Composer workers must have:
* **Storage Object Viewer** (`roles/storage.objectViewer`) on the input GCS buckets.
* **Storage Object Admin** (`roles/storage.objectAdmin`) on the output/temporary GCS buckets.
* Execution permissions (`chmod +x`) on the Python scripts if executed within a container or persistent worker.

### 4. Scheduling & Triggering
Because no parent workflow (JOBP) or schedule (EVNT_TIME) was provided in the legacy extraction, the DAG is configured with `schedule=None`. If this workflow needs to be triggered by an upstream process, configure an Airflow `TriggerDagRunOperator` in the upstream DAG, or set up a GCS Sensor to trigger the DAG upon file arrival.

---

## 5. Known Gaps & Unresolved References

### 1. Unresolved Wrapper Script (`r_vvtn_iar_bgf_gutschrift`)
The legacy job calls a local Unix wrapper script:
```bash
$HOME/aktuell/vorverarbeitung/tn/bin/r_vvtn_iar_bgf_gutschrift
```
This wrapper script was **not** supplied in the migration bundle. Consequently, the main execution task in the Airflow DAG (`dw_dwh_vvtn_iar_bgf_gutschr_task`) is currently implemented as an `EmptyOperator` placeholder.

* **Action Required:** Once the wrapper script `r_vvtn_iar_bgf_gutschrift` is migrated (e.g., to a Python script or a series of Airflow tasks), the `EmptyOperator` must be replaced with the appropriate operator (such as `BashOperator`, `SSHOperator`, or `GKEStartPodOperator`) to execute the migrated wrapper logic.

### 2. Host Execution Environment
The legacy job executed on host `|DWHDWH1P|HOST`. The target execution environment for the wrapper script must be finalized (e.g., running directly on Composer workers, on a persistent VM via SSH, or inside a Google Kubernetes Engine pod).

---

## 6. Validation

To validate the migrated scripts and DAG, perform the following tests:

### Unit Testing the Python Scripts Locally

#### Test 1: Metadata Preprocessor (`k_vvtn_iar_bgf_gutsch_foot.py`)
Run the script with a sample input file and verify that the filename is correctly injected:
```bash
# Create sample input
echo "row1_col1;row1_col2" > sample.txt

# Run script
python3 k_vvtn_iar_bgf_gutsch_foot.py --flnm "test_file.csv" sample.txt
```
* **Expected Output:**
  ```text
  X;Datei test_file.csv;row1_col1;row1_col2;File for BGF IAR Gutschrift;row1_col1
  ```

#### Test 2: Data Validator (`k_vvtn_iar_bgf_gutschrift.py`)
Verify that the validator passes valid 25-field rows and fails on invalid rows.

* **Valid Case (Exactly 25 fields):**
  ```bash
  # Generate a 25-field row (24 semicolons)
  echo "1;2;3;4;5;6;7;8;9;10;11;12;13;14;15;16;17;18;19;20;21;22;23;24;25" > valid.txt
  python3 k_vvtn_iar_bgf_gutschrift.py valid.txt
  ```
  * **Expected Output:** Exit code `0`. Output prefixed with `D;`.
    ```text
    D;1;2;3;4;5;6;7;8;9;10;11;12;13;14;15;16;17;18;19;20;21;22;23;24;25
    ```

* **Invalid Case (Fewer than 25 fields):**
  ```bash
  echo "1;2;3" > invalid.txt
  python3 k_vvtn_iar_bgf_gutschrift.py invalid.txt
  ```
  * **Expected Output:** Exit code `2`. Output contains the exact error message:
    ```text
    Error: Incorrect nos of Fields 
    ```

### DAG Integration Testing
1. Upload `dw_dwh_vvtn_iar_bgf_gutschr.py` to the Airflow DAGs folder.
2. Trigger the DAG manually via the Airflow UI.
3. Verify that `print_lastmonth_task` succeeds and prints the correct `Lastmonth is YYYYMM` string in the task logs.

---

## 7. Rollback Procedure

In the event of an issue during deployment or go-live, follow these steps to roll back to the legacy environment:

1. **Pause the Airflow DAG:**
   Go to the Airflow UI and toggle the switch next to `dw_dwh_vvtn_iar_bgf_gutschr` to **Off** (Paused).
2. **Re-enable the UC4 Job:**
   Log into the UC4 client, locate the job `DW.DWH_VVTN_IAR_BGF_GUTSCHR`, and ensure its active status is set to `Active (1)`.
3. **Verify Legacy Execution:**
   Confirm that the legacy Unix host `|DWHDWH1P|HOST` is receiving and processing the Gutschrift files as expected.
4. **Investigate Logs:**
   Analyze the Cloud Composer task logs and GCS execution outputs to diagnose the failure before attempting redeployment.