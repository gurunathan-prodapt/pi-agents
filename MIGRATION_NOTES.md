# Migration Notes: `ausd_bp_ta_bpr_instance`

This document provides the comprehensive migration notes for transitioning the legacy UC4 job `DW.BERT_AUSD_BP_TA_BPR_INSTANCE` and its associated KornShell wrappers and Oracle SQL*Plus scripts to Google Cloud BigQuery and Apache Airflow (Cloud Composer).

---

## 1. Summary

The legacy process executes an Oracle SQL script that queries remote tables over a database link (`@pcrs1`), filters records based on dynamic dates retrieved from a tracking/logging table, and loads the formatted base product instances into a target staging table (`sof$ta_bpr_instance`).

This process has been migrated from an on-premises Oracle/UC4/KornShell environment to **Google Cloud Platform (GCP)**. 
* **Orchestration:** Migrated from UC4 and KornShell wrappers (`r_ausd_bp_ta_bpr_instance.ksh`, `k_ausd_bp_ta_bpr_instance.ksh`) to an **Apache Airflow DAG** running on Cloud Composer.
* **Data Warehouse / Compute:** Migrated from Oracle SQL*Plus to **Google BigQuery SQL**.
* **Data Sources:** Remote database link tables (`cds$ta_cntrct@pcrs1` and `pds$ta_bpri_com@pcrs1`) are assumed to be replicated into BigQuery datasets (`cds` and `pds`) prior to running this job.

---

## 2. Generated Artifacts

The migration process generated the following files, which must be deployed to your Cloud Composer environment:

| Target File Path | Target Language | Role |
| :--- | :--- | :--- |
| `dags/ausd_bp_ta_bpr_instance_dag.py` | Python (Airflow DAG) | Orchestrates the job execution. It defines the DAG, sets up execution parameters, and calls the BigQuery operator. |
| `gcs/sql/d_ausd_bp_ta_bpr_instance.sql` | Google BigQuery SQL | Contains the core business logic, including dynamic date resolution, target table truncation, and the `INSERT-SELECT` statement. |

---

## 3. Key Design Decisions

### Consolidation of Shell Wrappers
The legacy architecture used two shell scripts (`r_...` and `k_...`) to handle environment setup, parameter parsing, date arithmetic, and logging. In the target architecture, these wrappers are consolidated into a single Airflow DAG. Parameter parsing (e.g., `stichtag`, `wiederanlauf_wert`) is handled natively via Airflow's `dag_run.conf` context.

### Dynamic Date Resolution (`v_datum`) inside BigQuery
In the legacy Oracle script, `v_datum` was resolved dynamically by querying the tracking table `isbert_schema.dwtk_meldungen`. To maintain atomic execution and minimize Airflow task overhead, this logic was moved directly into the BigQuery SQL script using standard SQL scripting (`DECLARE` and `SET`). 
* If a `stichtag` parameter is passed via the Airflow configuration, it is used directly.
* If no parameter is passed, the script dynamically queries `isbert_schema.dwtk_meldungen` for the last successful run of `BERT_DROP_TEMP_TABLE`.
* If both are missing, it defaults to `'19000101'`.

### Removal of Oracle-Specific Optimizer Hints
Oracle-specific hints such as `/*+ DRIVING_SITE(c) ORDERED ... */` were stripped out during translation. BigQuery's query engine dynamically optimizes execution plans and handles distributed joins automatically, making these hints obsolete.

### Parameterization of Project IDs
To support seamless promotion across environments (Dev, Test, Prod), all dataset references in the SQL script are parameterized using the Airflow variable `GCP_PROJECT_ID` (e.g., `{{ var.value.get("GCP_PROJECT_ID", "gcp-project") }}`).

---

## 4. Manual Steps Before Go-Live

Before deploying and running the migrated DAG, the following manual setup steps must be completed in the target GCP environment:

### 1. Schema & Dataset Creation
Ensure that the following BigQuery datasets exist in your target project and region (e.g., `EU`):
* `sof` (Target dataset containing `ta_bpr_instance`)
* `cds` (Source dataset containing `ta_cntrct`)
* `pds` (Source dataset containing `ta_bpri_com`)
* `isbert_schema` (Control dataset containing `dwtk_meldungen`)

### 2. Target Table DDL
If not already created by an external schema migration tool, initialize the target table `sof.ta_bpr_instance` using the following schema:

```sql
CREATE TABLE IF NOT EXISTS `your_project.sof.ta_bpr_instance` (
  CNTRCT_ID INT64,
  BPR_ID INT64,
  BPR_INSTANCE_ID INT64,
  ICCID STRING,
  IMSI_MCC STRING,
  IMSI_MNC STRING,
  IMSI_HLR STRING,
  IMSI_SI STRING,
  CNTRCT_ID_REF INT64
);
```

### 3. IAM & Permissions
The Cloud Composer service account (e.g., `service-XXX@gcp-sa-composer.iam.gserviceaccount.com`) must have the following roles:
* **BigQuery Data Editor** on the `sof` dataset.
* **BigQuery Data Viewer** on the `cds`, `pds`, and `isbert_schema` datasets.
* **BigQuery Job User** on the project level.
* **Storage Object Viewer** on the GCS bucket containing the SQL scripts.

### 4. Airflow Variables & Connections
* **Airflow Variable:** Create an Airflow variable named `GCP_PROJECT_ID` containing your target Google Cloud Project ID.
* **Airflow Connection:** Ensure the connection `google_cloud_default` is configured and has appropriate access to your GCP project.

### 5. Scheduling & Upstream Dependencies
The DAG is configured with `schedule_interval=None`. It should either be:
1. Triggered via an upstream Airflow DAG representing the prerequisite job (`BERT_DROP_TEMP_TABLE`) using a `TriggerDagRunOperator`.
2. Integrated into an external enterprise orchestrator via the Airflow REST API.

---

## 5. Known Gaps & Unresolved References

### 1. Null Handling in String Concatenation (Critical)
* **Legacy Behavior:** Oracle's concatenation operator (`||`) treats `NULL` values as empty strings (e.g., `'A' || NULL || 'B'` results in `'AB'`).
* **BigQuery Behavior:** BigQuery's `CONCAT` function returns `NULL` if *any* of its arguments are `NULL`.
* **Gap:** The migrated SQL uses:
  ```sql
  CONCAT(bp.iccid_mi, '-', bp.iccid_ii, '-', bp.iccid_iai, '-', bp.iccid_nr, '-', bp.iccid_cd)
  ```
  If any of these `iccid` components are `NULL` in the source data, the entire `iccid` field will result in `NULL`.
* **Recommended Redesign (B4):** Wrap each component in a `COALESCE` statement to match Oracle's behavior:
  ```sql
  CONCAT(
    COALESCE(bp.iccid_mi, ''), '-',
    COALESCE(bp.iccid_ii, ''), '-',
    COALESCE(bp.iccid_iai, ''), '-',
    COALESCE(bp.iccid_nr, ''), '-',
    COALESCE(bp.iccid_cd, '')
  )
  ```

### 2. Prerequisite Tracking Table (`dwtk_meldungen`)
The dynamic date resolution relies on `isbert_schema.dwtk_meldungen` being populated. If the upstream job `BERT_DROP_TEMP_TABLE` has not yet been migrated to BigQuery, this table will not contain the required success timestamp.
* **Mitigation:** Until the upstream job is migrated, you must manually pass the `stichtag` parameter when triggering the DAG, or manually insert a dummy tracking record into `isbert_schema.dwtk_meldungen`.

---

## 6. Validation

To validate the migration, perform the following steps:

### Execution Test
1. Upload `ausd_bp_ta_bpr_instance_dag.py` to your Composer DAGs folder.
2. Upload `d_ausd_bp_ta_bpr_instance.sql` to the `/gcs/sql/` directory in your Composer bucket.
3. Trigger the DAG manually via the Airflow UI with the following JSON configuration:
   ```json
   {
     "stichtag": "20260101",
     "wiederanlauf_wert": "0"
   }
   ```

### Verification of "Passing" Status
The run is considered successful if:
1. The Airflow DAG run completes with a `SUCCESS` status.
2. The BigQuery job logs show that the `TRUNCATE` and `INSERT` statements executed without errors.
3. A row count comparison between the legacy Oracle table `sof$ta_bpr_instance` and the BigQuery table `sof.ta_bpr_instance` for the same `stichtag` yields identical results.
4. Spot-check the `iccid` column to ensure formatting matches the legacy output (taking note of the NULL-handling gap described in Section 5).

---

## 7. Rollback Procedure

If a critical failure occurs post-go-live, execute the following rollback steps:

1. **Pause the Airflow DAG:** Disable the `ausd_bp_ta_bpr_instance` DAG in the Cloud Composer UI to prevent further executions.
2. **Redirect Downstream Consumers:** Point any downstream processes or reporting tools back to the legacy Oracle database instance/table (`sof$ta_bpr_instance`).
3. **Re-enable Legacy UC4 Job:** Reactivate the legacy UC4 job `DW.BERT_AUSD_BP_TA_BPR_INSTANCE` to resume processing on-premises.
4. **Data Cleanup (Optional):** If required, purge any partially loaded data in BigQuery by running:
   ```sql
   TRUNCATE TABLE `your_project.sof.ta_bpr_instance`;
   ```