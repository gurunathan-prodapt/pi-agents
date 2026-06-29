# Migration Notes: `ausd_bp_ta_bpr_basis_his`

This document provides comprehensive migration notes for transitioning the snapshot-based historical extraction job `ausd_bp_ta_bpr_basis_his` from the legacy on-premises environment to Google Cloud Platform (GCP).

---

## 1. Summary

The job `ausd_bp_ta_bpr_basis_his` is a key snapshot-based historical extraction job within the **BERT system**. Its business purpose is to prepare and stage active and reactivatable base product instances (such as MultiSIM, Twin-Cards, voice products, and connections) linked to contracts, including formatting and staging physical attributes like SIM ICCID and IMSI fields.

### Migration Scope
*   **Source Platform**: Legacy on-premises stack utilizing UC4 (Automic) scheduler, KornShell wrapper/control scripts (`r_ausd_bp_ta_bpr_basis_his.ksh`, `k_ausd_bp_ta_bpr_basis_his.ksh`), and Oracle SQL*Plus executing remote joins via a database link (`@pcrs1`).
*   **Target Platform**: Google Cloud Platform (GCP). Orchestration is migrated to **Google Cloud Composer (Apache Airflow 2.x)**, and data processing is migrated to **Google BigQuery** using Standard SQL scripting.

---

## 2. Generated Artifacts

The migration process has generated the following target artifacts to replace the legacy codebase:

| Artifact Name | Target Path | Role / Description |
| :--- | :--- | :--- |
| **Airflow DAG** | `dags/ausd_bp_ta_bpr_basis_his.py` | Replaces the UC4 job definition and KornShell wrappers. Orchestrates the execution of the BigQuery SQL script. |
| **BigQuery SQL Script** | `sql/d_ausd_bp_ta_bpr_basis_his.sql` | Replaces `d_ausd_bp_ta_bpr_basis_his.sql`. Contains the core ETL logic, including dynamic date lookup, target truncation, and high-performance joins. |
| **Target Table DDL** | `ddl/sof_ta_bpr_basis_his.sql` | Defines the schema and partitioning strategy for the target BigQuery table `sof.ta_bpr_basis_his`. |

---

## 3. Key Design Decisions

### 3.1. Elimination of Shell Wrappers & OS-Level Dependencies
The legacy KornShell wrappers (`r_ausd_bp_ta_bpr_basis_his.ksh` and `k_ausd_bp_ta_bpr_basis_his.ksh`) handled environment setup, logging, and date validation. In the target architecture, these are completely retired:
*   **Orchestration**: Handled natively by Airflow's `BigQueryExecuteQueryOperator`.
*   **Logging**: Custom shell logging (`f_alis_msgerr.ksh`) is replaced by native **Google Cloud Logging** integrated with Cloud Composer.
*   **Date Validation**: Handled inline within the BigQuery SQL script using standard procedural SQL (`DECLARE` and `SET`).

### 3.2. Idempotent Truncate-and-Reload Strategy
Although the legacy shell scripts accepted a restart parameter (`p_wiederanlaufWert` / `-l`), the underlying Oracle SQL script always performed a complete truncation and reload of the target table. 
*   **Decision**: The BigQuery script maintains this idempotent behavior by executing a `TRUNCATE TABLE` followed by an `INSERT INTO`. The unused restart parameter has been safely retired.

### 3.3. Null-Safe String Concatenation
In Oracle SQL, concatenating a string with a `NULL` value treats the `NULL` as an empty string (e.g., `'A' || NULL || 'B'` results in `'AB'`). In BigQuery Standard SQL, the `CONCAT` function returns `NULL` if *any* of its arguments are `NULL`.
*   **Decision**: To prevent entire ICCID strings from resolving to `NULL`, every sub-component of the ICCID concatenation is wrapped in a `COALESCE` function:
    ```sql
    CONCAT(
      COALESCE(bp.iccid_mi, ''), '-',
      COALESCE(bp.iccid_ii, ''), '-',
      COALESCE(bp.iccid_iai, ''), '-',
      COALESCE(bp.iccid_nr, ''), '-',
      COALESCE(bp.iccid_cd, '')
    ) AS ICCID
    ```

### 3.4. Table Partitioning
To optimize downstream query performance and manage storage costs, the target table `sof.ta_bpr_basis_his` is partitioned by the `VALID_FROM` date column.

### 3.5. Removal of Optimizer Hints
Oracle-specific hints (such as `/*+ DRIVING_SITE(c) ... */`) used to force execution paths across the database link have been completely removed. BigQuery's query planner optimizes execution plans natively without manual hinting.

---

## 4. Manual Steps Before Go-Live

Before deploying the migrated job to production, the following setup steps must be completed:

### 4.1. Schema & Dataset Creation
Ensure that the following BigQuery datasets exist in your target GCP project:
*   `sof` (Target staging dataset)
*   `cds` (Source contract dataset)
*   `pds` (Source product instance dataset)
*   `isbert_schema` (Metadata dataset)

Run the DDL script to create the target table:
```sql
-- Execute in the target BigQuery project
CREATE TABLE IF NOT EXISTS `sof.ta_bpr_basis_his` (
  CNTRCT_ID STRING OPTIONS(description="Contract identifier"),
  BPR_ID INT64 OPTIONS(description="Base product type identifier"),
  BPRI_COM_ID STRING OPTIONS(description="Product instance identifier"),
  ICCID STRING OPTIONS(description="SIM card serial number formatted"),
  IMSI_MCC STRING OPTIONS(description="Mobile Country Code"),
  IMSI_MNC STRING OPTIONS(description="Mobile Network Code"),
  IMSI_HLR STRING OPTIONS(description="Home Location Register indicator"),
  IMSI_SI STRING OPTIONS(description="Service Indicator"),
  CNTRCT_ID_REF STRING OPTIONS(description="Referenced contract identifier"),
  VALID_FROM DATE OPTIONS(description="Validity start date"),
  VALID_TO DATE OPTIONS(description="Validity end date"),
  MODIFIED_AT TIMESTAMP OPTIONS(description="Last modification timestamp"),
  INSERT_AT TIMESTAMP OPTIONS(description="Insertion timestamp"),
  SLAVE_NUMBER INT64 OPTIONS(description="MultiSIM sequence identifier"),
  E_ID STRING OPTIONS(description="SIM evolution e-ID")
)
PARTITION BY VALID_FROM;
```

### 4.2. IAM & Permissions
The Cloud Composer Service Account must be granted the following IAM roles:
*   `roles/bigquery.jobUser` on the GCP project level.
*   `roles/bigquery.dataEditor` on the `sof` dataset.
*   `roles/bigquery.dataViewer` on the `cds`, `pds`, and `isbert_schema` datasets.

### 4.3. Connection Strings & Airflow Connections
*   Verify that the Airflow connection `google_cloud_default` is configured correctly and points to the target GCP project.
*   If using a custom connection ID, update the `gcp_conn_id` parameter in the Airflow DAG file (`dags/ausd_bp_ta_bpr_basis_his.py`).

### 4.4. Scheduling & Upstream Dependencies
This job relies on upstream data replication. Ensure that:
1.  The replication pipelines for `cds.ta_cntrct` and `pds.ta_bpri_com` are scheduled and complete successfully before this job runs.
2.  The metadata table `isbert_schema.dwtk_meldungen` is updated with the correct processing date context (`job_kennung = 'BERT_DROP_TEMP_TABLE'`) prior to execution.

---

## 5. Known Gaps & Unresolved References

### 5.1. Upstream Replication Latency
*   **Risk**: If the replication of `cds.ta_cntrct` or `pds.ta_bpri_com` lags behind the operational systems, the snapshot generated by this job will be stale.
*   **Mitigation**: It is highly recommended to implement Airflow Sensors (e.g., `BigQueryTablePartitionSensor` or custom SQL sensors) in the DAG to verify that upstream tables have been updated for the current processing day before executing the main query.

### 5.2. Hardcoded Base Product IDs (B4 Redesign Candidate)
*   **Gap**: The base product IDs (`31`, `2759`, `2800`, `2835`, `2836`, `2837`, `3848`) are hardcoded directly inside the SQL `WHERE` clause.
*   **Redesign Recommendation**: To improve maintainability and avoid code changes when new base products are introduced, these IDs should be moved to a metadata configuration table (e.g., `sof.cfg_base_products`). The SQL query can then perform a dynamic join or subquery against this configuration table.

---

## 6. Validation

To validate the migration, perform the following test execution:

### 6.1. Test Execution Steps
1.  **Deploy to Test Environment**: Deploy the DDL, SQL script, and Airflow DAG to a non-production GCP environment.
2.  **Prepare Test Data**:
    *   Populate `isbert_schema.dwtk_meldungen` with a test record where `job_kennung = 'BERT_DROP_TEMP_TABLE'` and `timecreated` is set to a known historical date.
    *   Ensure test records exist in `cds.ta_cntrct` and `pds.ta_bpri_com` matching the criteria for that date.
3.  **Trigger DAG**: Manually trigger the DAG `ausd_bp_ta_bpr_basis_his` from the Airflow UI.

### 6.2. Definition of "Passing"
The validation is successful if:
*   The Airflow DAG run completes with a status of `SUCCESS`.
*   The target table `sof.ta_bpr_basis_his` is populated with data.
*   **Data Parity Check**: Run the legacy Oracle query and the new BigQuery query on the same source dataset snapshot. The row count and column values (especially the concatenated `ICCID` field) must match exactly.
*   **Null-Safety Check**: Verify that records with missing/null ICCID sub-components are successfully concatenated with empty strings instead of resulting in a completely null `ICCID` value.

---

## 7. Rollback Procedure

In the event of a critical failure during or immediately after go-live, follow these steps to roll back:

1.  **Pause Airflow DAG**: Go to the Airflow UI and pause the DAG `ausd_bp_ta_bpr_basis_his` to prevent further scheduled runs.
2.  **Data Recovery (Time Travel)**: If the target table `sof.ta_bpr_basis_his` was corrupted by a failed run, restore it to its pre-migration state using BigQuery's Time Travel feature:
    ```sql
    -- Restore table to its state 1 hour ago
    CREATE OR REPLACE TABLE `sof.ta_bpr_basis_his`
    AS SELECT * FROM `sof.ta_bpr_basis_his`
    FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR);
    ```
3.  **Re-enable Legacy Job**: Re-enable the UC4 job definition in the legacy scheduler to resume processing on the on-premises Oracle database.
4.  **Redirect Downstream Consumers**: If downstream systems were already pointed to BigQuery, redirect them back to the legacy Oracle database views/tables until the migration issue is resolved.