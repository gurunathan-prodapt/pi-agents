# Migration Notes: DW.DWH_DUMMY_ABSD_PLATO_TARIFE

This document details the migration of the UC4 UNIX job `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` to Apache Airflow (Cloud Composer on Google Cloud Platform).

---

## 1. Summary

The legacy UC4 job `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` has been migrated from an Automic/UC4 UNIX placeholder job to a native Apache Airflow DAG. 

* **Source Platform:** Automic/UC4 (UNIX Job Type `JOBS_UNIX`)
* **Target Platform:** Apache Airflow / Google Cloud Composer (GCP)
* **Migration Strategy:** 1:1 functional translation to an Airflow DAG containing an `EmptyOperator` task. Because no parent workflow (`JOBP`) or script trigger (`SCRI`) was supplied in the source bundle, this job is deployed as an independent, externally triggered DAG.

---

## 2. Generated Artifacts

The migration process generated the following file:

| File Path | Role | Description |
| :--- | :--- | :--- |
| `uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/dw_dwh_dummy_absd_plato_tarife.py` | Airflow DAG Definition | Python script defining the Airflow DAG `dw_dwh_dummy_absd_plato_tarife` and its single `EmptyOperator` task. |

---

## 3. Key Design Decisions

### 1. Operator Selection (`EmptyOperator`)
The legacy UC4 job was identified as a "dummy" or placeholder step. Its script body consisted solely of the command `:print Doing nothinig`. To replicate this non-operational behavior without incurring unnecessary compute overhead, the task is mapped to Airflow's `EmptyOperator`.

### 2. Standalone DAG Structure
Because the parent workflow (`DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP`) was not provided in the migration bundle, this job has been wrapped in its own standalone DAG with `schedule=None`. It is designed to be triggered manually or externally.

### 3. Environment-Specific Variable Externalization
To adhere to cloud-native best practices, environment-specific configurations are not hardcoded. The DAG dynamically references Airflow Variables for GCP environment details:
* `GCP_PROJECT`
* `GCP_REGION`
* `GCS_BUCKET`

### 4. Legacy Infrastructure Retirement
* **Host (`|DWHDWH1P|HOST`)**: Retired. Execution is handled natively within the Cloud Composer GKE/Kubernetes worker context.
* **Login (`DW.UNIX.ISTNS`)**: Retired. Security and execution context are managed via GCP Service Accounts and IAM roles assigned to the Cloud Composer environment.

---

## 4. Manual Steps Before Go-Live

Before deploying and running this DAG in a production environment, the following manual setup steps must be completed:

### 1. Airflow Variables Configuration
Ensure the following Airflow Variables are defined in the target Cloud Composer environment:

```bash
gcloud composer environments run <COMPOSER_ENV_NAME> \
    --location <COMPOSER_REGION> \
    variables set GCP_PROJECT "your-gcp-project-id"

gcloud composer environments run <COMPOSER_ENV_NAME> \
    --location <COMPOSER_REGION> \
    variables set GCP_REGION "your-gcp-region"

gcloud composer environments run <COMPOSER_ENV_NAME> \
    --location <COMPOSER_REGION> \
    variables set GCS_BUCKET "your-gcs-bucket-name"
```

### 2. IAM & Permissions
Verify that the Cloud Composer environment's service account has the necessary IAM permissions to parse and execute DAGs. Since this is an `EmptyOperator` task, no external GCP resource permissions (such as BigQuery or Cloud Storage read/write) are required for this specific DAG.

### 3. Scheduling & Downstream Integration
Because this DAG is configured with `schedule=None`, it will not run automatically. You must coordinate its execution with the downstream daily workflow `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP` once that workflow is migrated.

---

## 5. Known Gaps & Unresolved References

### 1. Downstream Workflow Not Yet Migrated
* **Gap:** The parent/downstream workflow `DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP` is not yet migrated. 
* **Impact:** This dummy step cannot be automatically wired into its orchestrator. 
* **Resolution:** Once the downstream workflow is converted, this DAG must either be triggered via a `TriggerDagRunOperator` from the parent DAG, or integrated directly into the parent DAG as a task block.

### 2. Redesign (B4) Opportunity: Task Consolidation
* **Gap:** Deploying an empty dummy step as a standalone DAG file introduces scheduling and monitoring overhead in Cloud Composer.
* **Resolution:** It is highly recommended to merge this step directly into the downstream DAG `dw_dwh_plato_tarif_mapping_taeglich_jp` as a starting `EmptyOperator` task rather than maintaining it as an independent DAG file.

### 3. Typo Preservation
* **Note:** The legacy print log `:print Doing nothinig` contains a spelling error (`nothinig`). This has been documented in the DAG's task comments to preserve legacy context and should not be modified.

---

## 6. Validation

To validate the migrated DAG, perform the following tests:

### 1. Local Syntax and Compilation Test
Run a syntax check on the generated Python file to ensure there are no import or compilation errors:

```bash
python3 uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/dw_dwh_dummy_absd_plato_tarife.py
```
* **Passing Criteria:** The command exits with code `0` and outputs no errors.

### 2. Airflow DAG Bag Validation
Upload the DAG to the Cloud Composer DAGs folder and verify that it is successfully parsed by Airflow without import errors:

```bash
gcloud composer environments storage dags import \
    --environment <COMPOSER_ENV_NAME> \
    --location <COMPOSER_REGION> \
    --source uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/dw_dwh_dummy_absd_plato_tarife.py
```
* **Passing Criteria:** The DAG appears in the Airflow UI under the ID `dw_dwh_dummy_absd_plato_tarife` with no "DAG Import Errors" displayed in the banner.

### 3. Execution Test
Trigger the DAG manually via the Airflow UI or CLI:

```bash
gcloud composer environments run <COMPOSER_ENV_NAME> \
    --location <COMPOSER_REGION> \
    dags trigger -- dw_dwh_dummy_absd_plato_tarife
```
* **Passing Criteria:** The DAG run starts immediately, the task `dw_dwh_dummy_absd_plato_tarife` transitions to `success` instantly, and the DAG run completes successfully.

---

## 7. Rollback Procedure

If a rollback is required after deployment, execute the following steps:

1. **Delete the DAG File from Cloud Storage:**
   Remove the Python file from the Cloud Composer DAGs bucket to prevent Airflow from parsing it.
   ```bash
   gsutil rm gs://<GCS_BUCKET>/dags/dw_dwh_dummy_absd_plato_tarife.py
   ```

2. **Verify Removal in Airflow:**
   Access the Airflow UI and confirm that the DAG `dw_dwh_dummy_absd_plato_tarife` is no longer listed (this may take up to 2 minutes as the DAG directory resynchronizes).

3. **Re-enable Legacy Execution (If Applicable):**
   If the legacy UC4 environment is still active, ensure the active flag for `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` is set to `1` (Active) in the UC4 console to resume legacy operations.