# Migration Notes: `ausd_bp_ta_bpr_basis`

This document outlines the migration details, generated artifacts, design decisions, manual setup steps, known gaps, validation procedures, and rollback strategies for the `ausd_bp_ta_bpr_basis` pipeline.

---

## 1. Summary

The legacy KornShell (KSH) and Oracle PL/SQL pipeline `ausd_bp_ta_bpr_basis` has been migrated to a modern cloud-native architecture on **Google Cloud Platform (GCP)**. 

* **Source Technology Stack**: KornShell (KSH) wrapper/control scripts (`r_ausd_bp_ta_bpr_basis.ksh`, `k_ausd_bp_ta_bpr_basis.ksh`) and Oracle PL/SQL scripts (`d_ausd_bp_ta_bpr_basis.sql`).
* **Target Technology Stack**: **Cloud Composer (Apache Airflow)** for orchestration and parameter validation, **Dataform** for SQL-based transformations, and **Google Cloud BigQuery** as the storage and processing engine.
* **Migration Pattern**: The migration strictly adheres to the prescribed pattern of replacing shell-based orchestration with Airflow DAGs and PL/SQL transformations with Dataform SQLX models.

---

## 2. Generated Artifacts

The migration process has generated the following files, each serving a specific role in the target architecture:

| File Path | Type | Role |
| :--- | :--- | :--- |
| `definitions/sof_ta_sim.sqlx` | Dataform SQLX | Recreates and populates the staging table `sof_ta_sim` with the latest active SIM records based on the execution date (`v_datum`) extracted from system metadata. |
| `definitions/sof_ta_bpr_basis.sqlx` | Dataform SQLX | Populates the final target table `sof_ta_bpr_basis` by joining historical product instances with the staging SIM data. |
| `dags/ausd_bp_ta_bpr_basis_orchestration.py` | Airflow DAG (Python) | Orchestrates the pipeline execution. Handles parameter validation (checking `stichtag` format), triggers Dataform compilation, and executes the Dataform workflow. |

---

## 3. Key Design Decisions

### 3.1. Decoupled Orchestration and Transformation
* **Decision**: Use Cloud Composer to validate parameters and trigger Dataform, while letting Dataform manage the dependency graph and SQL compilation.
* **Trade-off**: Introduces two GCP services (Composer and Dataform) instead of a single tool, but ensures clean separation of concerns. Airflow acts strictly as the scheduler/gatekeeper, while Dataform handles database-level transformations natively.

### 3.2. Dynamic Metadata Date Extraction (`v_datum`)
* **Decision**: The legacy pipeline extracted `v_datum` using shell utilities and Oracle queries. In the migrated design, this is handled natively inside BigQuery using a Common Table Expression (CTE) (`v_datum_cte`) within the `sof_ta_sim.sqlx` file.
* **Trade-off**: Eliminates the need to pass dynamic variables from Airflow to Dataform for standard runs, simplifying the Airflow DAG logic and keeping the SQL self-contained.

### 3.3. Removal of Oracle-Specific Features
* **Decision**: 
  * Oracle performance hints (e.g., `/*+ parallel(sim,4) */`) were completely removed. BigQuery's serverless execution engine automatically scales and optimizes query execution.
  * Explicit `TRUNCATE` and `COMMIT` statements were replaced by Dataform's native `type: "table"` configuration, which handles table recreation and transactional safety automatically.

---

## 4. Manual Steps Before Go-Live

Before deploying the pipeline to production, the following manual setup steps must be completed:

### 4.1. Schema and Dataset Creation
Ensure the following BigQuery datasets exist in your target GCP project:
* `isbert_schema` (Target dataset for `sof_ta_sim`, `sof_ta_bpr_basis`, and metadata table `dwtk_meldungen`).
* `carmen` (Or the corresponding dataset containing the synced tables `rma_ta_sim` and `rma_ta_sim_card_type`).

### 4.2. IAM & Permissions
The Cloud Composer environment's service account must be granted the following roles:
* **Dataform Editor** (`roles/dataform.editor`) or **Dataform Service Agent** to compile and execute workflows.
* **BigQuery Job User** (`roles/bigquery.jobUser`) in the project where execution occurs.
* **BigQuery Data Editor** (`roles/bigquery.dataEditor`) on the target datasets.

The Dataform Service Account (automatically created by GCP) must have:
* **BigQuery Data Viewer** (`roles/bigquery.dataViewer`) on source datasets.
* **BigQuery Data Editor** (`roles/bigquery.dataEditor`) on target datasets.

### 4.3. Dataform Repository Configuration
1. Create a Dataform repository named `bert-data-transforms` in the `us-central1` region (or your target region).
2. Connect the repository to your Git provider (e.g., Cloud Source Repositories, GitHub, GitLab).
3. Declare the source tables (`dwtk_meldungen`, `rma_ta_sim`, `rma_ta_sim_card_type`, `sof_ta_bpr_basis_his`) as Dataform declarations (`declarations.js` or individual `.sqlx` files) so that `${ref()}` statements resolve correctly.

### 4.4. Airflow Variables & Connections
* Update the `PROJECT_ID`, `REGION`, and `REPOSITORY_ID` constants in `ausd_bp_ta_bpr_basis_orchestration.py` to match your target GCP environment.

---

## 5. Known Gaps & Unresolved References

* **Upstream Table Syncing**: The pipeline assumes that `rma_ta_sim`, `rma_ta_sim_card_type`, and `sof_ta_bpr_basis_his` are already replicated/synced from the source Oracle system into BigQuery. If the replication pipeline is delayed, this pipeline will process stale data.
* **Metadata Table Dependency**: The calculation of `v_datum` depends on `isbert_schema.dwtk_meldungen` containing a record for `job_kennung = 'BERT_DROP_TEMP_TABLE'`. If this metadata job is deprecated or renamed in the cloud environment, the fallback date `19000101` will be used, which may result in empty or incorrect outputs.

---

## 6. Validation

To validate the migrated pipeline, perform the following tests:

### 6.1. Airflow DAG Validation
1. Upload `ausd_bp_ta_bpr_basis_orchestration.py` to the Composer DAGs folder.
2. Verify that the DAG parses successfully without syntax or import errors in the Airflow UI.
3. Trigger the DAG manually with a custom configuration to test parameter validation:
   ```json
   {
     "stichtag": "31122023"
   }
   ```
4. Trigger the DAG with an invalid date format (e.g., `2023-12-31`) and verify that the `validate_params` task fails as expected.

### 6.2. Dataform Compilation & Execution Validation
1. In the Dataform UI, trigger a manual compilation of the workspace and ensure there are no dependency cycle errors.
2. Execute the Dataform actions for `sof_ta_sim` and `sof_ta_bpr_basis`.
3. Verify that the tables are created successfully in BigQuery and that row counts are non-zero (assuming source data exists).

### 6.3. Data Reconciliation (Passing Criteria)
* Compare the row count and column values of `sof_ta_bpr_basis` generated in BigQuery with the legacy Oracle table for the same execution date (`v_datum`).
* **Passing Threshold**: 100% match on primary keys (`cntrct_id`, `bpr_id`) and matching non-null values for critical fields (`iccid`, `valid_to`, `card_type_name`).

---

## 7. Rollback Procedure

In the event of a critical failure or data corruption post-deployment, execute the following rollback steps:

1. **Pause the Airflow DAG**:
   Disable the `ausd_bp_ta_bpr_basis_orchestration` DAG in the Cloud Composer UI to prevent further scheduled runs.
2. **Revert Target Tables**:
   If the target tables contain corrupted data, restore them to their pre-execution state using BigQuery Time Travel:
   ```sql
   -- Example: Restore sof_ta_bpr_basis to its state 1 hour ago
   CREATE OR REPLACE TABLE `isbert_schema.sof_ta_bpr_basis`
   AS SELECT * FROM `isbert_schema.sof_ta_bpr_basis`
   FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR);
   ```
3. **Revert Code Changes**:
   Revert the Git commit in the Dataform repository to the last known stable commit and redeploy the compilation result.
4. **Legacy Fallback**:
   If necessary, resume the legacy KornShell/Oracle pipeline execution on-premises until the cloud issue is resolved. Ensure that any data processed on-premises is manually synced to BigQuery to maintain consistency.