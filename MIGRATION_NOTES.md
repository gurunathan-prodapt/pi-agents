# MIGRATION_NOTES.md — ausd_bp_ta_apn_carmen

This document provides the migration notes, design decisions, manual setup steps, validation procedures, and rollback strategies for the migrated job `ausd_bp_ta_apn_carmen`.

---

## 1. Summary
The legacy Oracle/UC4 job `ausd_bp_ta_apn_carmen` has been migrated to **Google Cloud Platform (GCP)**. 

* **Source Platform:** Oracle Database, UC4/Automic Scheduler, KornShell (KSH) wrappers.
* **Target Platform:** Google Cloud BigQuery (Data Warehouse) and Google Cloud Composer / Apache Airflow (Orchestration).
* **Business Purpose:** Prepares and populates access point connection details from the instantiated basic products of the CARMEN source system and stores them locally in the target table `sof.ta_apn_carmen`.

---

## 2. Generated Artifacts
The migration process replaced the legacy shell scripts and UC4 XML configurations with the following cloud-native artifacts:

| Artifact File Name | Target Path / Location | Role |
| :--- | :--- | :--- |
| `ddl_ta_apn_carmen.sql` | `/ddl/sof/` | BigQuery DDL script to initialize the target table `sof.ta_apn_carmen`. |
| `d_ausd_bp_ta_apn_carmen.sql` | `/queries/` | Native BigQuery SQL script containing the temporal join, truncation, and insertion logic. |
| `ausd_bp_ta_apn_carmen.py` | `/dags/` | Apache Airflow DAG file that orchestrates the execution of the BigQuery transformation. |

---

## 3. Key Design Decisions

* **Direct BigQuery Scripting:** Instead of splitting the workflow into multiple Python tasks or external execution steps, we utilized BigQuery's native scripting capabilities (`DECLARE`, `SET`, `TRUNCATE`, `INSERT INTO`). This keeps the execution logic close to the data engine, minimizes network round-trips, and simplifies the Airflow DAG structure.
* **Elimination of Shell Wrappers:** The legacy KornShell wrappers (`r_ausd_bp_ta_apn_carmen.ksh` and `k_ausd_bp_ta_apn_carmen.ksh`) were completely retired. Parameter parsing, logging, and execution limits are now handled natively by Apache Airflow and BigQuery logging.
* **Decoupled Source Ingestion:** The Oracle database link (`@pcrs1`) was replaced by a replicated BigQuery dataset (`src_carmen`). This decouples the analytical query from the transactional source system, eliminating cross-database performance bottlenecks and ensuring high availability.
* **Temporal Logic Preservation:** The point-in-time temporal tracking logic (`insert_at`, `modified_at`, `valid_from`, and `valid_to`) was preserved exactly as defined in the legacy system to guarantee functional parity.

---

## 4. Manual Steps Before Go-Live

Before activating the Airflow DAG in production, the following manual setup steps must be completed:

### 4.1. Schema & Dataset Creation
Ensure that the following BigQuery datasets exist in your target GCP project:
* `sof` (Core data warehouse layer)
* `isbert_schema` (Metadata layer)
* `src_carmen` (Landing/Replication layer)

Execute the DDL script to initialize the target table:
```sql
CREATE TABLE IF NOT EXISTS `your-gcp-project.sof.ta_apn_carmen` (
  CNTRCT_ID INT64 OPTIONS(description="Contract identifier"),
  ACCESS_POINT_NAME STRING OPTIONS(description="Name of the access point")
);
```

### 4.2. IAM & Permissions
Ensure that the Cloud Composer / Airflow Service Account has been granted the following IAM roles:
* **`roles/bigquery.dataEditor`** on the `sof` and `isbert_schema` datasets.
* **`roles/bigquery.dataViewer`** on the `src_carmen` dataset.
* **`roles/bigquery.jobUser`** at the project level to run BigQuery jobs.

### 4.3. Connection Strings & Variables
* Configure the Airflow variable `gcp_project` to point to your target Google Cloud Project.
* Ensure the BigQuery default connection (`bigquery_default`) is properly configured in Airflow.

### 4.4. Scheduling & Upstream Alignment
The DAG is configured to run daily at **04:00 UTC** (`0 4 * * *`). Verify that this schedule starts *after* the upstream replication of the CARMEN tables (`src_carmen.*`) and the update of `isbert_schema.dwtk_meldungen` have completed.

---

## 5. Known Gaps & Unresolved References

* **Upstream Replication Dependency (Redesign B4 Item):** The current DAG assumes that the CARMEN source tables are fully replicated before execution. If the replication pipeline fails or is delayed, this job will process stale data. 
  * *Follow-up:* Implement a BigQuery sensor task (`BigQueryTablePartitionSensor` or a custom SQL sensor) in the DAG to verify that the source tables have been updated within the last 24 hours before running the transformation.
* **Hardcoded Project References:** The SQL scripts currently contain placeholder project IDs (`your-gcp-project`). These must be dynamically replaced during the CI/CD deployment process using Airflow template variables (e.g., `{{ var.value.gcp_project }}`).
* **Empty Reference Date Fallback:** If no record matching `BERT_DROP_TEMP_TABLE` is found in `dwtk_meldungen`, the reference date defaults to `'19000101'`, resulting in an empty target table.
  * *Follow-up:* Implement an Airflow post-execution check to trigger an alert if the target table contains 0 rows after a successful run.

---

## 6. Validation

To validate the migration and ensure functional equivalence, perform the following steps:

### 6.1. Test Execution
1. Seed the `isbert_schema.dwtk_meldungen` table with a test execution date.
2. Populate the `src_carmen` tables with a known snapshot of historical data.
3. Trigger the DAG manually from the Airflow UI (`dag_ausd_bp_ta_apn_carmen`).

### 6.2. Definition of "Passing"
The migration is considered successful and validated if:
* The Airflow DAG run finishes with a `SUCCESS` status.
* The target table `sof.ta_apn_carmen` is successfully truncated and repopulated.
* **Data Parity Check:** A comparison query between the legacy Oracle target table and the new BigQuery target table yields a **100% match** on row count and column values for the same reference date (`v_datum`).
* No duplicate `CNTRCT_ID` and `ACCESS_POINT_NAME` combinations exist unless explicitly allowed by the business logic.

---

## 7. Rollback Procedure

In the event of a critical failure or data corruption post-go-live, execute the following rollback steps:

1. **Pause the Airflow DAG:** Navigate to the Airflow UI and toggle the switch to pause `dag_ausd_bp_ta_apn_carmen`.
2. **Clean up Target Data:** If corrupted data was written to the target table, truncate the BigQuery table to prevent downstream jobs from consuming bad data:
   ```sql
   TRUNCATE TABLE `your-gcp-project.sof.ta_apn_carmen`;
   ```
3. **Re-enable Legacy Scheduler:** Reactivate the legacy UC4 job `DW.BERT_AUSD_BP_TA_APN_CARMEN.xml`.
4. **Execute Legacy Job:** Run the legacy KornShell wrapper (`r_ausd_bp_ta_apn_carmen.ksh`) manually or via UC4 to restore the target table state in the Oracle database.
5. **Investigate:** Check the Airflow task logs and BigQuery job history to diagnose the failure.