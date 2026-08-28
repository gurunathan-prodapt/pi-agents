# Migration Notes: DW.DWH_ALL_TYPES_MASTER

This document details the migration of the legacy UC4/Automic workload `DW.DWH_ALL_TYPES_MASTER` to Google Cloud Platform (GCP). 

---

## 1. Summary

The legacy workload `DW.DWH_ALL_TYPES_MASTER` is a showcase workflow that orchestrates an Ab Initio graph, an Oracle SQL database refresh, and a Korn Shell (KSH) script containing AWK post-processing logic. 

This workload has been fully migrated from its on-premises Unix/Oracle/Ab Initio environment to a modern, cloud-native architecture on **Google Cloud Platform (GCP)**.

### Target Architecture Mapping
* **Orchestration**: UC4 scheduling and job definitions are migrated to an **Apache Airflow DAG** hosted on **Google Cloud Composer**.
* **Ab Initio Graph**: The legacy `all_types_graph` is migrated to a **PySpark application** executed on **Google Cloud Dataproc Serverless**.
* **Database Operations**: Oracle SQL*Plus scripts are migrated to native **BigQuery SQL** utilizing BigQuery Scripting for procedural control.
* **Shell & AWK Processing**: The KSH wrapper and AWK transformation scripts are migrated to **Python 3 scripts** executed within the Cloud Composer environment.

---

## 2. Generated Artifacts

The migration process generated the following files, each playing a specific role in the target environment:

| Target File Path | Language / Tech | Role / Description |
| :--- | :--- | :--- |
| `DWH_ALL_TYPES_JOB/dw_dwh_all_types_master.py` | Python (Airflow DAG) | The master orchestration DAG. Defines the execution sequence, task dependencies, and environment variables. |
| `isall/aufbereitung/bin/r_all_types_master.py` | Python 3 | Replaces the legacy KSH wrapper (`r_all_types_master.ksh`). Coordinates the BigQuery SQL execution and invokes the AWK-replacement Python script. |
| `isall/aufbereitung/awk/k_all_types_transform.py` | Python 3 | Replaces the legacy AWK script (`k_all_types_transform.awk`). Performs streaming, line-by-line validation and formatting of the exported CSV data. |
| `isall/aufbereitung/sql/d_all_types.sql` | BigQuery SQL | Replaces the legacy Oracle SQL script (`d_all_types.sql`). Performs a truncate-and-load operation on the target staging table. |
| `.dw_init` | *Retired* | **No conversion required.** Legacy environment initialization script. Its variables and paths have been mapped directly to Airflow Variables and GCP environment configurations. |

---

## 3. Key Design Decisions

### 3.1. AWK to Python Conversion for Strict Control Flow
The legacy AWK script (`k_all_types_transform.awk`) validates that every record in the export file contains exactly 12 fields. If a malformed record is encountered, it must immediately halt processing and return a process-level exit code of `2` (`exit 2`). 
* **Decision**: Migrated to a standalone Python script (`k_all_types_transform.py`) utilizing a custom `AwkContext` class to mimic AWK's streaming behavior.
* **Trade-off**: While BigQuery can load CSVs directly, it cannot conditionally abort a load mid-stream with a custom OS-level exit code based on row-level validation. Using Python ensures strict compatibility with legacy error-handling and orchestration requirements.

### 3.2. KSH to Python Conversion for Native API Integration
The legacy wrapper `r_all_types_master.ksh` executed SQL*Plus and AWK via shell subprocesses.
* **Decision**: Converted the wrapper to Python (`r_all_types_master.py`). This allows the script to use the native `google-cloud-bigquery` client library to execute SQL scripts, improving error handling, logging, and security (IAM-based authentication) over raw shell execution.

### 3.3. Emulating SQL*Plus Directives in BigQuery Scripting
The legacy SQL script used SQL*Plus directives like `WHENEVER SQLERROR CONTINUE` for the `TRUNCATE` step (allowing the script to proceed even if the table did not exist) and `WHENEVER SQLERROR EXIT FAILURE` for the `INSERT` step.
* **Decision**: Implemented BigQuery Scripting block structures (`BEGIN ... EXCEPTION ... END`). The `TRUNCATE` statement is wrapped in a nested block that catches and ignores errors, while the `INSERT` statement is wrapped in a block that raises an exception on failure, preserving the exact operational behavior.

### 3.4. Retirement of `.dw_init`
The legacy `.dw_init` script set up local directory paths and Oracle environment variables.
* **Decision**: Retired the file. Hardcoded local paths (e.g., `/vobs/dw_source/daten/...`) are replaced by dynamic GCS bucket paths and Airflow environment variables, ensuring portability across Dev, Test, and Prod environments.

---

## 4. Manual Steps Before Go-Live

Before activating the migrated workflow in production, the following setup steps must be completed:

### 4.1. BigQuery Schema and Dataset Creation
Ensure the target dataset and tables exist in BigQuery:
1. Create the dataset defined in your Airflow variables (e.g., `dwh_all_types`).
2. Create the raw staging table `cds$ta_all_types_raw` and the target table `sof$ta_all_types` with schemas matching the legacy Oracle definitions.

### 4.2. IAM Permissions
The Google Cloud Service Account running the Cloud Composer environment must be granted the following IAM roles:
* **BigQuery**: `roles/bigquery.jobUser` and `roles/bigquery.dataEditor` (on the target dataset).
* **Dataproc**: `roles/dataproc.editor` (to submit Serverless PySpark jobs).
* **Cloud Storage**: `roles/storage.objectAdmin` (on the Composer and data buckets).

### 4.3. Airflow Variables Configuration
The following Airflow Variables must be configured in the Cloud Composer UI (`Admin -> Variables`):
* `GCP_PROJECT`: The GCP Project ID hosting the resources.
* `GCP_REGION`: The GCP region for Dataproc and Composer (e.g., `europe-west3`).
* `GCP_CLUSTER_NAME`: The name of the Dataproc cluster (if using a shared cluster).
* `GCS_BUCKET`: The GCS bucket name used for staging scripts and data (e.g., `my-dwh-migration-bucket`).
* `BQ_DATASET`: The target BigQuery dataset name.

### 4.4. Code Deployment to GCS
1. Upload the migrated PySpark script `all_types_graph.py` (migrated from the Ab Initio graph) to:
   `gs://{GCS_BUCKET}/pyspark_scripts/all_types_graph.py`
2. Upload the Python and SQL scripts to the Cloud Composer DAGs bucket:
   * `dags/dw_dwh_all_types_master.py` $\rightarrow$ `/dags/`
   * `isall/aufbereitung/bin/r_all_types_master.py` $\rightarrow$ `/dags/isall/aufbereitung/bin/`
   * `isall/aufbereitung/awk/k_all_types_transform.py` $\rightarrow$ `/dags/isall/aufbereitung/awk/`
   * `isall/aufbereitung/sql/d_all_types.sql` $\rightarrow$ `/dags/isall/aufbereitung/sql/`

### 4.5. Scheduling
The DAG is currently configured with `schedule=None` (manual or external trigger), matching the legacy UC4 configuration. If this job needs to be scheduled, update the `schedule` parameter in `dw_dwh_all_types_master.py` (e.g., `schedule="0 2 * * *"` for daily at 2:00 AM).

---

## 5. Known Gaps & Unresolved References

* **Upstream PySpark Script (`all_types_graph.py`)**: This script represents the core Ab Initio graph logic. It must be fully tested and deployed to GCS before running this master DAG.
* **Legacy DB User (`DW_ORAUSER`)**: The environment variable `DW_ORAUSER` is passed to the Python script as a dummy value (`dummy_user`) to maintain interface compatibility. BigQuery uses IAM-based authentication, so this variable is not used for database connections. Ensure no downstream legacy scripts rely on this variable for actual database authentication.

---

## 6. Validation

To validate the migration, perform the following tests:

### 6.1. Unit Test: AWK-to-Python Validation
Run the Python validation script locally or in a Cloud Shell environment with test data:
```bash
# Test with a valid 12-field record
echo "1;2;3;4;5;6;7;8;9;10;11;12" | python3 isall/aufbereitung/awk/k_all_types_transform.py -
# Expected Output: D;1;2;3;4;5;6;7;8;9;10;11;12
# Expected Exit Code: 0

# Test with an invalid record (11 fields)
echo "1;2;3;4;5;6;7;8;9;10;11" | python3 isall/aufbereitung/awk/k_all_types_transform.py -
# Expected Output: Error: Incorrect nos of Fields 
# Expected Exit Code: 2
```

### 6.2. Integration Test: End-to-End DAG Run
1. Populate the BigQuery raw table `cds$ta_all_types_raw` with sample records where `status = 'READY'`.
2. Place a sample CSV file containing 12-field records at `/home/airflow/gcs/dags/isall/data/all_types_export.csv` (simulating the output of the PySpark job).
3. Trigger the DAG `dw_dwh_all_types_master` manually from the Airflow UI.
4. Verify that:
   * The `all_types_graph` task completes successfully.
   * The `task_r_all_types_master` task completes successfully.
   * The file `/home/airflow/gcs/dags/isall/data/all_types_export.out` is created and contains the prefixed records (`D;...`).
   * The BigQuery table `sof$ta_all_types` is populated with the records from `cds$ta_all_types_raw`, and `processed_at` contains the current timestamp.
   * A log file is generated at `/home/airflow/gcs/dags/isall/protokoll/all_types_master_{DDMMYYYY}.log`.

---

## 7. Rollback Procedure

In the event of a critical failure during go-live, follow these steps to roll back to the legacy environment:

1. **Pause the Airflow DAG**:
   Go to the Cloud Composer Airflow UI and toggle the switch to pause `dw_dwh_all_types_master`.
2. **Clean Up Target Tables**:
   If necessary, truncate the BigQuery target table to prevent partial data loads:
   ```sql
   TRUNCATE TABLE `your_project.your_dataset.sof$ta_all_types`;
   ```
3. **Re-enable Legacy Workload**:
   Re-activate the `DW.DWH_ALL_TYPES_MASTER` job in the UC4/Automic scheduler.
4. **Verify Legacy Execution**:
   Monitor the legacy UC4 execution logs to ensure the job runs and processes data successfully.