# MIGRATION NOTES: DW.BERT_AUSD_V_TA_CNTRCT_CRS2

This document provides comprehensive technical notes for the migration of the `DW.BERT_AUSD_V_TA_CNTRCT_CRS2` job from its legacy Oracle and UC4/Automic environment to Google Cloud Platform (GCP).

---

## 1. Summary

The primary objective of the `DW.BERT_AUSD_V_TA_CNTRCT_CRS2` job is to process contract data, resolve parent-child relationships for contract agreements (specifically mapping child contracts to their parent frame contract numbers), and populate the target contract staging table.

### Migration Scope
*   **Source Platform**: Oracle Database (SQL*Plus, PL/SQL), UC4/Automic Scheduler, Korn Shell (`.ksh`) wrappers.
*   **Target Platform**: Google Cloud Platform (GCP).
    *   **Orchestration**: Cloud Composer (Apache Airflow 2.x).
    *   **Data Transformation**: Dataform (BigQuery SQLX).
    *   **Storage & Compute**: BigQuery.
*   **Key Logic Migrated**: Resolving parent-child contract hierarchies by self-joining the staging contract data (`sof_ta_cntrct_crs`) to extract parent frame contract numbers (`rv_num`) where the parent contract type is `10` (Frame Contract / RV), while filtering out parent frame contracts from the final output dataset.

---

## 2. Generated Artifacts

The migration process has produced two primary code artifacts to replace the legacy shell scripts, SQL*Plus scripts, and UC4 XML definitions:

### 1. Cloud Composer DAG
*   **File Path**: `dags/dag_dw_bert_ausd_v_ta_cntrct_crs2.py`
*   **Role**: Orchestrates the execution of the data pipeline. It programmatically triggers a Dataform compilation for the target repository and invokes the specific workflow execution for the `sof_ta_cntrct_crs2` table. It replaces the legacy UC4 scheduler XML and the `.ksh` wrapper scripts (`r_ausd_v_ta_cntrct_crs2.ksh`, `k_ausd_v_ta_cntrct_crs2.ksh`).

### 2. Dataform SQLX Model
*   **File Path**: `definitions/sof_ta_cntrct_crs2.sqlx`
*   **Role**: Defines the target table schema, metadata documentation, and the BigQuery SQL transformation logic. It replaces the legacy Oracle SQL*Plus script `d_ausd_v_ta_cntrct_crs2.sql`. It handles the self-join logic and filters out frame contracts (`cntrct_ty = 10`) from the primary record set while preserving their reference numbers on child records.

---

## 3. Key Design Decisions

### 1. Elimination of Legacy Special Characters in Table Names
*   **Decision**: The legacy Oracle table names contained dollar signs (e.g., `sof$ta_cntrct_crs` and `sof$ta_cntrct_crs2`). These have been renamed to use underscores (e.g., `sof_ta_cntrct_crs` and `sof_ta_cntrct_crs2`).
*   **Reasoning**: BigQuery and Dataform identifier standards discourage or prohibit special characters like `$` in table and dataset names. Using underscores ensures compatibility with standard SQL syntax and prevents parsing issues.

### 2. Removal of Oracle-Specific Performance Hints
*   **Decision**: The legacy Oracle SQL hint `/*+ PARALLEL(4) */` was completely removed from the SQL statement.
*   **Reasoning**: BigQuery is a serverless, auto-scaling data warehouse. It manages query parallelization and resource allocation dynamically under the hood. Hardcoded execution hints are obsolete and unsupported in BigQuery SQL.

### 3. Transition from Truncate-and-Insert to Dataform Table Re-creation
*   **Decision**: The target table is configured with `type: "table"` in Dataform.
*   **Reasoning**: In the legacy Oracle script, the target table was explicitly truncated before inserting new records. In Dataform, configuring a model as `type: "table"` automatically handles the drop-and-recreate (or overwrite) lifecycle during execution, ensuring an atomic update without manual DDL/DML management.

### 4. Decoupling of Metadata Logging (`dwtk_meldungen`)
*   **Decision**: The legacy script's dependency on the Oracle logging table `dwtk_meldungen` for execution timestamps has been decoupled from the core transformation query.
*   **Reasoning**: Airflow and Dataform natively capture execution metadata, run durations, and status logs. Keeping operational logging inside the SQL transformation pollutes the business logic.

---

## 4. Manual Steps Before Go-Live

Before deploying the DAG and executing the pipeline in a production environment, the following setup steps must be completed:

### 1. Schema and Dataset Creation
Ensure the target BigQuery dataset exists in your project:
*   **Dataset Name**: `isbert_schema` (or the environment-specific equivalent configured in your `environments.json` in Dataform).
*   **Location**: Must match the region defined in your Airflow DAG and Dataform settings (e.g., `europe-west3`).

### 2. IAM & Permissions
The Cloud Composer environment's service account must have the following IAM roles assigned:
*   **Dataform Editor** (`roles/dataform.editor`) or **Dataform Service Agent** on the target repository `dwh-bert-dataform`.
*   **BigQuery Data Editor** (`roles/bigquery.dataEditor`) on the target dataset `isbert_schema`.
*   **BigQuery Job User** (`roles/bigquery.jobUser`) on the project level to execute the compiled SQL queries.

### 3. Connection Strings & Secrets
*   **Dataform Git Connection**: Ensure that the Dataform repository `dwh-bert-dataform` is connected to your Git provider (e.g., Cloud Source Repositories, GitHub, or GitLab) and that the SSH key or personal access token is stored securely in **Secret Manager**.
*   **Airflow Variables**: Update the following variables in the Airflow environment if they differ from the defaults:
    *   `gcp-project-id` (Your target GCP Project ID)
    *   `europe-west3` (Your target GCP Region)

### 4. Scheduling & Dependencies
*   The DAG is scheduled to run nightly at `02:00 UTC` (`0 2 * * *`).
*   **Upstream Dependency**: Ensure that the upstream table `sof_ta_cntrct_crs` is populated *before* this job runs. If `sof_ta_cntrct_crs` is managed by another Dataform model or Airflow DAG, establish a DAG dependency or include both models in the same Dataform dependency tree using `dependencies: [ "sof_ta_cntrct_crs" ]` in the SQLX config.

---

## 5. Known Gaps & Unresolved References

### 1. Upstream Table Availability
*   **Gap**: The SQLX model references `${ref("sof_ta_cntrct_crs")}`. This table must be defined as a source or another model within the same Dataform workspace. If `sof_ta_cntrct_crs` is populated by an external system (e.g., an ingestion pipeline), it must be declared in a `declarations.js` file within Dataform.

### 2. Legacy DB-Link (`@pcrs1`)
*   **Gap**: The legacy system utilized a database link (`@pcrs1`) to query remote Oracle tables.
*   **Resolution**: This migration assumes that the data from `@pcrs1` has already been ingested into the BigQuery table `sof_ta_cntrct_crs` by an upstream ingestion process. Direct federated queries (BigQuery Omni or Cloud SQL Federated Queries) are not implemented in this model.

---

## 6. Validation

To validate the migration and ensure functional equivalence, perform the following tests:

### 1. Compilation Test
Run the Dataform compilation in your development workspace:
```bash
dataform compile
```
*   **Passing Criteria**: The project compiles without syntax errors, and the dependency graph correctly resolves `sof_ta_cntrct_crs2` as a downstream child of `sof_ta_cntrct_crs`.

### 2. Dry-Run Execution
Execute a dry run of the SQL query in the BigQuery console to verify syntax and estimate bytes processed:
*   **Passing Criteria**: The query validator returns a green checkmark indicating the SQL is valid.

### 3. Data Reconciliation (Row Count & Logic Verification)
Run the following validation query on both the legacy Oracle database and the new BigQuery table after a test run:
```sql
-- Validation Query
SELECT 
  COUNT(*) as total_records,
  COUNT(DISTINCT cntrct_id) as unique_contracts,
  COUNT(CASE WHEN rv_num IS NOT NULL THEN 1 END) as resolved_parents,
  COUNT(CASE WHEN cntrct_ty = 10 THEN 1 END) as invalid_frame_contracts_remaining
FROM `isbert_schema.sof_ta_cntrct_crs2`;
```
*   **Passing Criteria**:
    *   `total_records` and `unique_contracts` match the legacy Oracle target table exactly.
    *   `invalid_frame_contracts_remaining` is exactly `0` (confirming the `WHERE c.cntrct_ty != 10` filter worked).
    *   `resolved_parents` matches the count of child contracts successfully mapped to their parent frame contracts.

---

## 7. Rollback Procedure

In the event of a critical failure or data corruption during the go-live window, execute the following rollback steps:

### Step 1: Pause the Airflow DAG
Disable the active DAG to prevent subsequent scheduled executions:
```bash
gcloud composer environments run <composer-env-name> \
    --location <region> \
    dags pause -- dw_bert_ausd_v_ta_cntrct_crs2
```

### Step 2: Revert Target Table to Previous State
If you need to restore the target table to its pre-execution state, use BigQuery's time travel feature to restore the table from a known good timestamp:
```sql
-- Restore table to state 1 hour ago
CREATE OR REPLACE TABLE `isbert_schema.sof_ta_cntrct_crs2` AS
SELECT * FROM `isbert_schema.sof_ta_cntrct_crs2`
FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR);
```

### Step 3: Fallback to Legacy Scheduler (If Co-existence is Maintained)
If the legacy UC4 scheduler and Oracle database are still active during the transition phase:
1.  Re-enable the UC4 job `DW.BERT_AUSD_V_TA_CNTRCT_CRS2`.
2.  Trigger an ad-hoc run of the legacy UC4 job to catch up on missed data processing.