# Migration Notes: DW.DWH_PFPL_CL_TARIF_SMART

This document provides comprehensive migration notes and operational instructions for transitioning the legacy UC4 native UNIX job `DW.DWH_PFPL_CL_TARIF_SMART` and its associated scripts to Google Cloud Platform (GCP) using Cloud Composer (Apache Airflow) and BigQuery.

---

## 1. Summary

The legacy UC4 job `DW.DWH_PFPL_CL_TARIF_SMART` has been migrated from an on-premises UNIX/Oracle environment to **Google Cloud Platform (GCP)**. 

### Scope of Migration
* **Orchestration**: Migrated from a UC4 native UNIX job (`JOBS_UNIX`) to an **Apache Airflow DAG** running in Cloud Composer.
* **Execution Wrapper**: Migrated the KornShell framework utility `r_sqlscript` to a **Python 3 orchestration wrapper** (`r_sqlscript.py`).
* **Business Logic**: Migrated the Oracle SQL validation script `d_pfpl_classic_tarif_smart.sql` to a **BigQuery SQL Scripting Block**.

### Business Purpose
This job performs a critical data integrity and freshness check on the Plato-Classic smart-tarif-mapping data. It compares the current state of the tariff data (`aktuell`) against the target aggregated data (`smart`) using a mutual set-difference calculation. If any discrepancies are found, the job raises an error to halt downstream processing.

---

## 2. Generated Artifacts

The migration process generated three core artifacts, each playing a specific role in the target architecture:

```
gcs-bucket/
├── dags/
│   └── DW_DWH_PFPL_CL_TARIF_SMART.py          # Airflow DAG (Orchestrator)
├── scripts/
│   └── r_sqlscript.py                         # Python Wrapper (Execution & Logging)
└── sql/
    └── d_pfpl_classic_tarif_smart.sql         # BigQuery SQL Scripting Block (Business Logic)
```

### 1. `DW_DWH_PFPL_CL_TARIF_SMART.py` (Airflow DAG)
* **Role**: Defines the workflow orchestration. It schedules and triggers the validation task.
* **Implementation**: Uses a `BashOperator` to execute the Python wrapper script with the appropriate command-line arguments, passing environment variables (`GCP_PROJECT`, `BQ_DATASET`) dynamically.

### 2. `r_sqlscript.py` (Python Wrapper)
* **Role**: Replaces the legacy KornShell wrapper. It handles command-line argument parsing, resolves SQL file paths, manages execution logging, performs environment-specific variable substitutions within the SQL script, executes the query on BigQuery, and translates query failures into standard OS exit codes.

### 3. `d_pfpl_classic_tarif_smart.sql` (BigQuery SQL Scripting Block)
* **Role**: Contains the core business logic. It defines the CTEs (`aktuell`, `smart`, `differenz_aktuell_smart`, etc.) and performs the set-difference validation.
* **Implementation**: Uses BigQuery Scripting (`DECLARE`, `SET`, `IF...THEN ERROR()`) to raise a runtime exception if discrepancies are detected.

---

## 3. Key Design Decisions

### Python Wrapper Strategy (KSH to Python 3)
* **Decision**: The legacy `r_sqlscript` KornShell utility was rewritten as a Python 3 script rather than being replaced entirely by native Airflow operators.
* **Reasoning**: The legacy wrapper contains complex operational logic, including dynamic path resolution, parameter injection, and custom logging/metadata registration (`DWMSG_*`). A Python wrapper preserves this unified execution facade, allowing the SQL scripts to be executed with identical command-line interfaces while leveraging the modern `google-cloud-bigquery` client library.

### Set Operations Mapping (`MINUS` to `EXCEPT DISTINCT`)
* **Decision**: Oracle's `MINUS` operator was mapped to BigQuery's `EXCEPT DISTINCT`.
* **Reasoning**: `EXCEPT DISTINCT` is the ANSI SQL equivalent of `MINUS` and guarantees identical duplicate-elimination semantics during the mutual set-difference evaluation.

### Temporal Data Type Mapping (`DATE` to `DATETIME`)
* **Decision**: Oracle `DATE` columns were mapped to BigQuery `DATETIME` instead of `DATE`.
* **Reasoning**: Oracle's `DATE` type includes a time component. Mapping to BigQuery's `DATETIME` prevents time-truncation, ensuring that temporal comparisons in the set-difference logic remain 100% accurate.

### Exit Code Propagation via BigQuery Scripting
* **Decision**: Used BigQuery's native `ERROR()` function inside a scripting block to handle validation failures.
* **Reasoning**: BigQuery SQL engine does not natively return OS-level exit codes. By raising a scripting error (`ERROR()`) when differences are found, the BigQuery client library throws a `GoogleAPIError`. The Python wrapper catches this exception, logs the failure, and exits with a non-zero status code (`1`), which Airflow interprets as a task failure.

---

## 4. Manual Steps Before Go-Live

To deploy and run this job in production, the following manual setup steps must be completed:

### 1. BigQuery Dataset & Schema Setup
Ensure that the target BigQuery dataset exists and contains the following tables with schemas matching the legacy Oracle definitions:
* `d_tarif`
* `dwh$ta_l_map_plato_mp_tarif`
* `dwh$ta_l_map_plato_param`
* `dwh$ta_l_map_plato_tarif_smart`

### 2. IAM & Permissions
The Service Account running the Cloud Composer worker nodes must be granted the following IAM roles on the target GCP project and BigQuery dataset:
* **BigQuery Job User** (`roles/bigquery.jobUser`): To run query jobs.
* **BigQuery Data Viewer** (`roles/bigquery.dataViewer`): To read from the source tables.
* **Storage Object Viewer** (`roles/storage.objectViewer`): To read DAG and SQL artifacts from the GCS bucket.

### 3. Airflow Variables Configuration
Configure the following Airflow Variables in the Cloud Composer environment (via Airflow UI -> Admin -> Variables):

| Variable Name | Example Value | Description |
| :--- | :--- | :--- |
| `GCP_PROJECT` | `prod-dwh-gcp-project` | The target GCP Project ID. |
| `BQ_LOCATION` | `EU` | The geographic location of the BigQuery dataset. |
| `BQ_DATASET` | `plato_classic_dataset` | The target BigQuery dataset name. |
| `GCS_BUCKET` | `us-central1-composer-bucket` | The GCS bucket associated with Composer. |
| `R_SQLSCRIPT_PATH` | `/home/airflow/gcs/dags/scripts/r_sqlscript.py` | Absolute path to the Python wrapper script. |

### 4. File Deployment
Copy the generated files to their respective directories in the Composer GCS bucket:
```bash
gsutil cp DW_DWH_PFPL_CL_TARIF_SMART.py gs://<composer-bucket>/dags/
gsutil cp r_sqlscript.py gs://<composer-bucket>/dags/scripts/
gsutil cp d_pfpl_classic_tarif_smart.sql gs://<composer-bucket>/dags/sql/
```

### 5. Scheduling & Triggering
The DAG is configured with `schedule=None` (active but waiting for external triggers). If this job must run as part of a larger pipeline, integrate it using an `AirflowSensor`, a `TriggerDagRunOperator` in a parent DAG, or update the `schedule` parameter to a cron expression (e.g., `schedule="0 6 * * *"`).

---

## 5. Known Gaps & Unresolved References

### 1. Omission of Legacy SQL Initialization (`d_alis_init.sql`)
* **Gap**: The legacy SQL script contains the command `START $DW_DIR_ROOT/allgemein/is/util/sql/d_alis_init.sql`. This initialization script was not supplied in the migration bundle and has been omitted from the BigQuery SQL script.
* **Action Required**: Verify if `d_alis_init.sql` performs any critical session-level configurations or DDL. If it does, those configurations must be manually ported into the BigQuery SQL script or handled within the Python wrapper.

### 2. Legacy Logging Framework Simulation (`DWMSG_*`)
* **Gap**: The legacy KornShell script relies on a suite of database-backed logging utilities (`DWMSG_ErmittleNr`, `DWMSG_Logdateiname`, `DWMSG_ErzeugeEintrag`, etc.). These have been simulated using local file-based logging in `r_sqlscript.py`.
* **Action Required**: For a true production-grade implementation, replace these local file-logging stubs with integrations to **Google Cloud Logging** or write execution metadata to a centralized BigQuery audit table.

---

## 6. Validation

To validate the migrated job, perform the following test cases in a non-production environment:

### Test Case 1: Successful Validation (No Discrepancies)
1. Populate the source tables (`d_tarif`, `dwh$ta_l_map_plato_mp_tarif`, `dwh$ta_l_map_plato_tarif_smart`) with identical, matching records.
2. Trigger the Airflow DAG `dw_dwh_pfpl_cl_tarif_smart` manually from the Airflow UI.
3. **Expected Result**: 
   * The BigQuery query completes with `col_anz_differenzen = 0`.
   * The Python wrapper logs `Anzahl Differenzen : 0` and exits with code `0`.
   * The Airflow DAG run finishes with a status of **Success**.

### Test Case 2: Validation Failure (Discrepancies Detected)
1. Introduce a discrepancy (e.g., insert an extra row in `d_tarif` or modify a `tarif_bez` value in `dwh$ta_l_map_plato_tarif_smart`).
2. Trigger the Airflow DAG manually.
3. **Expected Result**:
   * The BigQuery query identifies the mismatch and executes the `ERROR()` block.
   * The Python wrapper catches the query exception, logs the validation failure, and exits with code `1`.
   * The Airflow DAG run finishes with a status of **Failed**.
   * The task log contains the error message: `Data validation check failed. Validation code: 100. Differences detected: <count>`.

---

## 7. Rollback Procedure

If issues are encountered post-go-live and a rollback to the legacy UC4/Oracle environment is required, execute the following steps:

1. **Pause the Airflow DAG**:
   Go to the Airflow UI and toggle the switch for `dw_dwh_pfpl_cl_tarif_smart` to **Off** (paused) to prevent any automated or accidental triggers.
2. **Re-enable the UC4 Job**:
   Log into the UC4/Automic interface, locate the job `DW.DWH_PFPL_CL_TARIF_SMART`, and set its active flag back to active (`active=1`).
3. **Verify Legacy Environment**:
   Ensure that the legacy Oracle database connections, environment variables (`$DW_DIR_ROOT`), and the physical SQL script paths on host `dwhdwh5p` are intact and accessible.
4. **Audit Logs**:
   Check the legacy execution logs to confirm that the job resumes successful execution under the UC4 agent.