# Migration Notes: DW.BERT_AUSD_BP_TA_TARIFOPTION

This document provides the technical migration details for transitioning the legacy UC4 job `DW.BERT_AUSD_BP_TA_TARIFOPTION` to Google Cloud Platform (GCP).

---

## 1. Summary

The legacy job `DW.BERT_AUSD_BP_TA_TARIFOPTION` has been migrated from an on-premises Oracle and UC4 environment to **Google Cloud Composer (Apache Airflow)** and **Google BigQuery**.

### Scope of Migration
* **Legacy Components Retired**: 
  * UC4 Job: `DW.BERT_AUSD_BP_TA_TARIFOPTION.xml`
  * KornShell Wrapper Script: `r_ausd_bp_ta_tarifoption.ksh`
  * KornShell Core Script: `k_ausd_bp_ta_tarifoption.ksh`
  * Oracle SQL/PLSQL Script: `d_ausd_bp_ta_tarifoption.sql`
* **Target Platform**: Google Cloud Platform (GCP)
  * **Orchestration**: Apache Airflow (Cloud Composer)
  * **Execution Engine**: Google BigQuery (SQL Script / Multi-Statement Query)

### Business & Technical Purpose
The job prepares and aggregates contract-level basic product tariff options for the **BERT** (demand scoring) platform. It dynamically identifies the latest run-date suffix (`v_datum`) from a metadata table, joins a dynamic daily contract text table with a static lookup filter table, and aggregates the filtered options by Contract ID (`cntrct_id`). The aggregated descriptions are concatenated alphabetically into three distinct target columns (`business_option`, `sonstige_option`, and `gprs_option`) based on their category, and written to the target table `sof_ta_tarifoption`.

---

## 2. Generated Artifacts

The migration process generated the following files:

| File Name | Path / Location | Role / Description |
| :--- | :--- | :--- |
| **`d_ausd_bp_ta_tarifoption.sql`** | `gcp/bigquery/sql/` | The refactored BigQuery SQL script. It handles dynamic table resolution via procedural SQL (`EXECUTE IMMEDIATE`) and performs parallelized string aggregation. |
| **`dag_bert_ausd_bp_ta_tarifoption.py`** | `gcp/airflow/dags/` | The Airflow DAG file that orchestrates the execution of the BigQuery SQL script using the `BigQueryInsertJobOperator`. |

---

## 3. Key Design Decisions

### 3.1. Stateful to Stateless Refactoring (String Aggregation)
* **Legacy Approach**: The legacy Oracle SQL relied on a stateful custom package (`sof$ab_con`) containing accumulators (`concat1`, `concat2`, etc.) and an analytical `LEAD` function over an unordered set to detect the final row of a contract group. This required pre-sorting the entire dataset in an inline view, resulting in single-threaded, row-by-row execution.
* **BigQuery Approach**: Replaced entirely with native, stateless SQL functions. We use **`STRING_AGG`** with an internal **`ORDER BY pds_description`** clause, combined with conditional **`IF`** statements to isolate categories. This allows BigQuery to fully parallelize the aggregation across its execution slots, drastically improving performance and code maintainability.

### 3.2. Dynamic Table Name Resolution
* **Legacy Approach**: KornShell scripts calculated the date suffix (`v_datum`) and passed it as a parameter to the Oracle SQL session, which resolved the table name `sof$ta_bpr_opt_text_<v_datum>`.
* **BigQuery Approach**: Handled natively within a single BigQuery procedural SQL script using `DECLARE`, `SET`, and **`EXECUTE IMMEDIATE FORMAT(...)`**. This eliminates the need for external shell scripting or complex parameter passing from Airflow.

### 3.3. Schema Standardization
* Special characters in Oracle table names (such as `$`) have been replaced with underscores (`_`) to comply with BigQuery naming conventions (e.g., `sof$ta_tarifoption` $\rightarrow$ `sof_ta_tarifoption`).
* Column names and data types have been mapped to native BigQuery types (e.g., `VARCHAR2` $\rightarrow$ `STRING`).

### 3.4. Truncation Preservation
* The legacy logic truncated concatenated option strings to 500 characters. This business rule is preserved using BigQuery's `SUBSTR(..., 1, 500)` to prevent downstream schema overflow errors in legacy-consuming systems.

---

## 4. Manual Steps Before Go-Live

Before deploying and enabling the Airflow DAG, the following manual setup steps must be completed in the target GCP environment:

### 4.1. Schema and Dataset Creation
Ensure the target BigQuery dataset exists. If not, create it:
```bash
bq mk --dataset --location=EU target_project:target_dataset
```

### 4.2. Static Lookup Table Migration
Ensure the static lookup filter table `sof_ta_l_bpr_optionen_filter` is created and populated with the correct mapping data:
```sql
CREATE OR REPLACE TABLE `target_project.target_dataset.sof_ta_l_bpr_optionen_filter` (
  bpr_id STRING,
  opt_kategorie STRING
);
-- Populate table with legacy filter mappings (BUDGET, SONST, GPRS)
```

### 4.3. Metadata Table Setup
Ensure the metadata table `dwtk_meldungen` exists and contains the necessary tracking records:
```sql
CREATE OR REPLACE TABLE `target_project.target_dataset.dwtk_meldungen` (
  job_kennung STRING,
  timecreated TIMESTAMP
);
```

### 4.4. IAM & Permissions
The Cloud Composer environment's service account must be granted the following roles on the target BigQuery dataset:
* **`roles/bigquery.jobUser`** (at the project level to run query jobs)
* **`roles/bigquery.dataEditor`** (on the target dataset to read/write tables)

### 4.5. Airflow Variables & Connections
* Ensure the default Google Cloud connection (`google_cloud_default`) is configured in Airflow and has access to the target GCP project.
* Replace the placeholder project and dataset names (`target_project.target_dataset`) in the generated DAG and SQL files with your actual environment-specific values (e.g., via Airflow variables or CI/CD deployment templates).

### 4.6. Scheduling Coordination
* Disable the legacy UC4 job `DW.BERT_AUSD_BP_TA_TARIFOPTION` to prevent concurrent executions and race conditions on the target tables.

---

## 5. Known Gaps & Unresolved References

### 5.1. Dynamic Table Dependency Risk
* **Description**: The SQL script dynamically queries `sof_ta_bpr_opt_text_<v_datum>`. If the upstream process that creates this table fails or is delayed, the BigQuery script will fail with a `Table not found` error.
* **Mitigation**: A future enhancement (Redesign Tier B4) should introduce an Airflow **`BigQueryTableExistenceSensor`** task in the DAG to verify the existence of the dynamic table before executing the SQL script.

### 5.2. Hardcoded Environment References
* **Description**: The generated SQL script contains hardcoded references to `target_project.target_dataset`.
* **Mitigation**: These must be parameterized during the CI/CD deployment pipeline using search-and-replace or templating tools (e.g., dbt or Airflow template variables).

---

## 6. Validation

To validate the migration, perform the following tests in a non-production environment:

### 6.1. Dry-Run Validation
Run a manual dry-run of the BigQuery SQL script in the BigQuery Console. Replace the dynamic `v_datum` logic with a hardcoded historical date suffix that contains known test data:
```sql
-- Test execution with a static date suffix
DECLARE v_datum STRING DEFAULT '20260421'; 
-- Execute Steps 2 and 3 manually to verify syntax and schema creation.
```

### 6.2. DAG Execution Test
1. Upload `dag_bert_ausd_bp_ta_tarifoption.py` to the Cloud Composer DAGs folder.
2. Trigger the DAG manually from the Airflow UI.
3. Verify that all tasks complete successfully (`success` state).

### 6.3. Data Reconciliation (Query-Based)
Run the following reconciliation query to compare the output of the migrated BigQuery pipeline with the legacy Oracle table for a given run:

```sql
-- Row count validation
SELECT 
  (SELECT COUNT(*) FROM `target_project.target_dataset.sof_ta_tarifoption`) AS bq_row_count,
  (SELECT COUNT(*) FROM `legacy_oracle_export.sof_ta_tarifoption`) AS oracle_row_count;

-- Content mismatch validation (should return 0 rows)
SELECT 
  bq.cntrct_id,
  bq.business_option AS bq_bus, ora.business_option AS ora_bus,
  bq.sonstige_option AS bq_son, ora.sonstige_option AS ora_son,
  bq.gprs_option AS bq_gprs, ora.gprs_option AS ora_gprs
FROM 
  `target_project.target_dataset.sof_ta_tarifoption` bq
FULL OUTER JOIN 
  `legacy_oracle_export.sof_ta_tarifoption` ora
ON 
  bq.cntrct_id = ora.cntrct_id
WHERE 
  bq.business_option != ora.business_option
  OR bq.sonstige_option != ora.sonstige_option
  OR bq.gprs_option != ora.gprs_option;
```

---

## 7. Rollback Procedure

In the event of a critical failure during go-live, follow these steps to revert to the legacy system:

1. **Pause the Airflow DAG**:
   Go to the Airflow UI and toggle the switch for `dw_bert_ausd_bp_ta_tarifoption` to **Off** (Paused).
2. **Re-enable UC4 Scheduling**:
   Re-activate the legacy UC4 job `DW.BERT_AUSD_BP_TA_TARIFOPTION` in the UC4 scheduler.
3. **Verify Legacy Database State**:
   Ensure that the legacy Oracle target table `sof$ta_tarifoption` is intact. If it was modified or truncated during testing, restore it from the last database backup or trigger a manual run of the legacy UC4 job to repopulate it.
4. **Document the Incident**:
   Log the failure reason, error messages, and BigQuery job IDs for troubleshooting.