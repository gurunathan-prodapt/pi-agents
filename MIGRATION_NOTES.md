# MIGRATION_NOTES.md — Job: DW.BERT_P_VERTRAG_JP

This document provides comprehensive migration notes and operational guidelines for the transition of the legacy UC4/Automic workflow `DW.BERT_P_VERTRAG_JP` to Google Cloud Platform (GCP) using BigQuery and Apache Airflow (Google Cloud Composer).

---

## 1. Summary

The legacy `DW.BERT_P_VERTRAG_JP` workflow processes and prepares contract-related master data for the BERT system. The legacy implementation relied on a UC4/Automic orchestrator, KornShell (`.ksh`) wrapper scripts, and Oracle PL/SQL transformations querying a remote Oracle Carmen instance via Database Links (`@pcrs1`).

This migration transfers all processing to a **fully relational, high-performance architecture on Google Cloud Platform**:
* **Target Platform**: Google Cloud Composer 2 (Apache Airflow 2.x) & Google Cloud BigQuery.
* **Migration Strategy**: Legacy KornShell control loops, SQL*Plus wrappers, and Oracle PL/SQL concepts (such as pipelined table functions and database links) have been retired. They are replaced by native Airflow operators (`BigQueryInsertJobOperator`) and optimized BigQuery SQL scripts using analytical functions, native aggregation (`STRING_AGG`), and dynamic watermark variables.

---

## 2. Generated Artifacts

The migrated job consists of the following files, organized within the target Cloud Composer repository:

```text
/home/gurunathan_t/migrated_composer/
├── dags/
│   └── bert_p_vertrag_jp_dag.py                        # Airflow DAG Orchestrator
└── sql/
    └── bert_p_vertrag_jp/
        ├── d_ausd_v_ta_acc_ref.sql                      # BigQuery SQL transformation 1
        ├── d_ausd_v_ta_action_assoc.sql                 # BigQuery SQL transformation 2
        ├── d_ausd_v_ta_apn_ve.sql                       # BigQuery SQL transformation 3
        ├── d_ausd_v_ta_barrier.sql                      # BigQuery SQL transformation 4
        ├── d_ausd_v_ta_barrier_zusgf.sql                # BigQuery SQL transformation 5 (Agg replacement)
        ├── d_ausd_v_ta_bp_ref.sql                       # BigQuery SQL transformation 6
        ├── d_ausd_v_ta_cntrct_crs.sql                   # BigQuery SQL transformation 7
        ├── d_ausd_v_ta_cntrct_valid.sql                 # BigQuery SQL transformation 8
        ├── d_ausd_v_ta_discount.sql                     # BigQuery SQL transformation 9
        ├── d_ausd_v_ta_discount_rr.sql                  # BigQuery SQL transformation 10
        ├── d_ausd_v_ta_disc_zusgf.sql                   # BigQuery SQL transformation 11 (Agg replacement)
        ├── d_ausd_v_ta_inv_acc.sql                      # BigQuery SQL transformation 12
        ├── d_ausd_v_ta_inv_assign.sql                   # BigQuery SQL transformation 13
        ├── d_ausd_v_ta_inv_def.sql                      # BigQuery SQL transformation 14
        ├── d_ausd_v_ta_period.sql                       # BigQuery SQL transformation 15
        ├── d_ausd_v_ta_vvl_dwh.sql                      # BigQuery SQL transformation 16
        └── d_ausd_v_ta_vvl_upgrade.sql                  # BigQuery SQL transformation 17
```

### Role of Each File:
* **`bert_p_vertrag_jp_dag.py`**: Defines the Airflow DAG, task dependencies, execution schedule, and dynamic environment variable resolution.
* **`d_ausd_v_ta_*.sql`**: Individual SQL scripts containing the refactored business logic for extracting, filtering, and transforming staging and master contract data.
* **`d_ausd_v_ta_barrier_zusgf.sql` & `d_ausd_v_ta_disc_zusgf.sql`**: Specialized scripts that replace legacy Oracle PL/SQL pipelined functions (`concat_barriers` and `concat_discounts`) with high-performance BigQuery `STRING_AGG` window groupings.

---

## 3. Key Design Decisions

### 3.1 Relational Shift to BigQuery (No Spark/Dataproc)
The legacy operations are pure SQL/PLSQL transformations. The migration avoids unnecessary overhead by bypassing Dataproc/PySpark. Instead, it executes transformations directly inside BigQuery using the `BigQueryInsertJobOperator`. This minimizes data movement, reduces execution costs, and leverages BigQuery's massive parallel processing power.

### 3.2 Replacement of Oracle Pipelined Functions
Oracle pipelined table functions and custom collection types used for string concatenation (`concat_barriers` and `concat_discounts`) were refactored into native BigQuery SQL using analytical CTEs and `STRING_AGG(...)` with explicit ordering. This simplifies the codebase and improves execution performance.

### 3.3 Dynamic Watermarking (`v_datum`)
The legacy system used a static snapshot variable (`&v_datum`). In the migrated SQL scripts, this is replaced by a dynamic local variable declaration:
```sql
DECLARE v_datum STRING;
SET v_datum = (
  SELECT COALESCE(FORMAT_DATE('%Y%m%d', MAX(m.timecreated)), '19000101')
  FROM `target_project.target_dataset.dwtk_meldungen` m
  WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
);
```
This ensures that all scripts run against a consistent, dynamically resolved watermark without requiring manual parameter injection.

### 3.4 Environment Parameterization
All SQL scripts use Jinja templating (`{{ var.value.gcp_project }}` and `{{ var.value.env_prefix }}`) to reference project and dataset names. This allows the same code assets to run unmodified across `dev`, `test`, and `prod` environments.

---

## 4. Manual Steps Before Go-Live

To ensure a successful deployment, the following setup steps must be performed manually in the target GCP environment:

### 4.1 Schema & Dataset Creation
Ensure that the following BigQuery datasets exist in your target GCP project:
* Staging Dataset: `<env_prefix>_staging` (e.g., `prod_staging`)
* Carmen Mirror Dataset: `<env_prefix>_carmen_mirror` (e.g., `prod_carmen_mirror`)
* Core DWH Dataset: `<env_prefix>_dwh` (e.g., `prod_dwh`)

### 4.2 IAM & Permissions
The Cloud Composer Service Account must have the following IAM roles:
* `roles/bigquery.jobUser` (to run BigQuery jobs)
* `roles/bigquery.dataEditor` on the staging, mirror, and DWH datasets.
* Read access to the Cloud Storage bucket hosting the SQL scripts.

### 4.3 Airflow Variables
Configure the following Airflow Variables in the Composer UI (`Admin -> Variables`):
* `gcp_project`: The target GCP Project ID (e.g., `prod-bert-dwh`).
* `env_prefix`: The environment prefix (e.g., `prod`, `test`, or `dev`).

### 4.4 Connection Strings
Verify that the Airflow connection `google_cloud_default` is configured correctly and has the necessary permissions to execute queries in the target GCP project.

### 4.5 Upstream Scheduling & Synchronization
Because the legacy `@pcrs1` database link is retired, an upstream replication pipeline must be scheduled to sync the remote Oracle Carmen tables into the `<env_prefix>_carmen_mirror` dataset daily. 
* Ensure this replication completes *before* the `DW.BERT_P_VERTRAG_JP` DAG starts at `02:00 AM`.
* If necessary, configure an `ExternalTaskSensor` in the DAG to wait for the replication pipeline's completion.

---

## 5. Known Gaps & Unresolved References

* **Upstream Table Replication**: This job assumes that all source tables prefixed with `cds_` or `pds_` (e.g., `cds_ta_acc_ref`, `pds_ta_pdp_context_assoc`) are already mirrored in the `<env_prefix>_carmen_mirror` dataset. The replication mechanism itself is external to this job.
* **Downstream Integration**: The final target tables generated by this job (such as `sof_ta_barrier_zusgf`, `sof_ta_disc_zusgf`, and `sof_ta_inv_acc`) are consumed by downstream BERT reconciliation processes. Ensure downstream DAGs are updated to reference these BigQuery tables instead of legacy Oracle tables.
* **Metadata Logging**: The watermark table `dwtk_meldungen` must be populated by upstream processes with the `job_kennung = 'BERT_DROP_TEMP_TABLE'` entry to allow correct date filtering.

---

## 6. Validation

### 6.1 How to Run the Tests
1. **Dry Run**: Validate the SQL syntax and Airflow DAG parsing by running:
   ```bash
   airflow dags show DW.BERT_P_VERTRAG_JP
   ```
2. **SQL Dry Run**: Execute the SQL scripts in the BigQuery Console using the "Dry Run" feature to verify that all table references and schemas are valid.
3. **Manual DAG Trigger**: Trigger a manual run of the DAG in the Airflow UI:
   ```bash
   airflow dags trigger DW.BERT_P_VERTRAG_JP
   ```

### 6.2 What "Passing" Means
* All 17 tasks in the DAG complete with a `success` status.
* The target tables in the `<env_prefix>_staging` dataset are successfully created or replaced.
* Row counts in the target BigQuery tables match the expected counts from the legacy Oracle database for the corresponding watermark date (`v_datum`).
* No syntax or type mismatch errors are present in the Airflow task logs.

---

## 7. Rollback Procedure

In the event of a critical failure or data corruption during deployment:

1. **Pause the DAG**: Immediately pause the Airflow DAG in the Composer UI or via the CLI:
   ```bash
   airflow dags pause DW.BERT_P_VERTRAG_JP
   ```
2. **Restore Target Tables**: If target tables were corrupted, restore them to their previous state using BigQuery's time-travel feature:
   ```sql
   CREATE OR REPLACE TABLE `prod_staging.sof_ta_barrier`
   AS SELECT * FROM `prod_staging.sof_ta_barrier` FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR);
   ```
3. **Legacy Fallback**: If a complete fallback to the legacy system is required, re-enable the legacy UC4/Automic workflow `DW.BERT_P_VERTRAG_JP.xml` and ensure the database link `@pcrs1` is active.