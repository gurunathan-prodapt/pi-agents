# Migration Notes

**Job:** Shared Files — `vobs/dw_source/isdwh/allgemein/is/env/dw_files`  
**Source Path:** `/home/gurunathan_t/folder1_uc4_ksh_abinitio`  
**Target Platform:** Google Cloud Platform (BigQuery & Cloud Composer)  
**Migration Pattern:** `UC4_ONLY` (Configuration & Environment Initialization)

---

## 1. Summary

This migration transitions the legacy environment configuration files (`.dw_ai`, `.dw_db`, `.dw_global`, and `.dw_init`) into cloud-native configuration patterns on Google Cloud Platform. 

In the legacy environment, these files functioned as nested KornShell and Ab Initio environment sources to establish global variables, database paths, connection properties, and directory structures. On GCP, these configurations are split into:
*   **Cloud Composer (Airflow) Variables:** For orchestrating file paths, bucket locations, and system-wide metadata.
*   **BigQuery Procedural SQL:** For session-level parameterization, database connection mappings, and dynamic SQL execution boundaries.

---

## 2. Generated Artifacts

The following target files have been generated to replace the legacy environment scripts:

| Relative Path | Target Language | Source Legacy File(s) | Role / Description |
| :--- | :--- | :--- | :--- |
| `dags/config/dw_env_config.json` | JSON | `.dw_ai`, `.dw_db`, `.dw_init` | Centralized environment variable map for import into Cloud Composer. |
| `ddl/procedures/dw_init.sql` | BigQuery SQL | `.dw_init` | Persistent stored procedure initializing GCS paths and validating system-wide database configurations. |
| `ddl/procedures/dw_global.sql` | BigQuery SQL | `.dw_global` | Persistent stored procedure validating that all required environment variables are populated. |
| `dags/dw_shared_files_bootstrap.py` | Python (Airflow DAG) | `.dw_init`, `.dw_global` | Bootstrap DAG to load variables into the Airflow Metadata Store and verify BigQuery connectivity. |

---

## 3. Key Design Decisions

### Decoupling Orchestration from Database State
*   **Decision:** Split the legacy shell scripts into Airflow Variables (for file routing and orchestration) and BigQuery Stored Procedures (for SQL session parameters).
*   **Trade-off:** Downstream tasks must fetch variables from the appropriate control plane. Airflow tasks pull from the Airflow Metadata Store, while BigQuery scripts call `dw_init` or query configuration tables. This introduces a slight duplication of path definitions but guarantees strict separation of concerns.

### Standardizing Paths to Google Cloud Storage (GCS) URIs
*   **Decision:** Legacy POSIX file paths (e.g., `/appl/local/daten/...`) have been mapped to structured GCS URIs using the pattern `gs://<GCS_BUCKET_NAME>/dw_source/...`.
*   **Trade-off:** Any downstream legacy script expecting local file system access must be updated to use GCS-compatible operators or mount GCS buckets via Cloud Storage FUSE.

### Retaining Legacy Error Semantics
*   **Decision:** Verbatim German error messages and validation checks (e.g., `"Fehler in .dw_init: Konnte ORACLE_HOME nicht setzen !"`) are preserved inside the BigQuery stored procedures.
*   **Trade-off:** This ensures that legacy monitoring tools parsing logs for specific error patterns continue to function without modification.

---

## 4. Manual Steps Before Go-Live

To ensure a successful deployment, the following manual setup steps must be completed in the target GCP environment:

### 1. Schema & Dataset Creation
Ensure the target BigQuery dataset exists and contains the system tracking tables:
```sql
CREATE SCHEMA IF NOT EXISTS `GCP_PROJECT.BQ_DATASET`;

-- Create system paths table used by dw_init validation
CREATE TABLE IF NOT EXISTS `GCP_PROJECT.BQ_DATASET.system_paths` (
  path STRING,
  active BOOLEAN
);

-- Populate system paths to satisfy legacy ORACLE_HOME checks
INSERT INTO `GCP_PROJECT.BQ_DATASET.system_paths` (path, active)
VALUES 
  ('/appl/local/oracle/12.2.0.1.0', TRUE),
  ('/appl/local/oracle/11.2.0', TRUE);

-- Create error logging table
CREATE TABLE IF NOT EXISTS `GCP_PROJECT.BQ_DATASET.error_logs` (
  log_time TIMESTAMP,
  module STRING,
  message STRING
);
```

### 2. IAM & Permissions
*   The Cloud Composer Service Account must have the **BigQuery Admin** (`roles/bigquery.admin`) role to execute and create stored procedures.
*   The Cloud Composer Service Account must have **Storage Object Viewer** (`roles/storage.objectViewer`) on the environment bucket `gs://gcp-dwh-environment-bucket`.

### 3. Secrets Management
The legacy database password (`DB_PASSWD_DWH`) must be migrated to **Cloud Secret Manager**:
1. Create a secret named `dwh-oracle-db-password`.
2. Grant the Cloud Composer Service Account the **Secret Manager Secret Accessor** (`roles/secretmanager.secretAccessor`) role.

### 4. Airflow Variable Initialization
Before running any downstream DAGs, import the `dw_env_config.json` file into Cloud Composer:
```bash
gcloud composer environments run COMPOSER_ENV_NAME \
    --location COMPOSER_REGION \
    variables import -- /path/to/dw_env_config.json
```

---

## 5. Known Gaps & Unresolved References

The following legacy dependencies were identified in the source code but do not have physical counterparts in the migrated codebase:

*   **`SETPYA.SH` (Cognos Initialization):** This script was sourced by `.dw_global` but is missing from the scanned codebase. If Cognos reporting pipelines are migrated, this initialization logic must be redesigned.
*   **`.DW_LOKAL` (Local Settings):** Sourced at the end of `.dw_init`. Any local overrides previously defined in `.dw_lokal` must be manually appended to `dw_env_config.json` or handled via Airflow environment-specific variables.
*   **Downstream Consumers:** The following downstream processes are not yet migrated and must be wired to the new GCS paths once deployed:
    *   `DW.DWH_ABPZ_KKM_AIL_AGENT`
    *   `r_ai_start` / `vobs/dw_source/isdwh/abinitio/bin/r_ai_start`

---

## 6. Validation

To validate the migration of the shared environment files, execute the bootstrap DAG and verify the BigQuery stored procedures.

### Step 1: Run the Bootstrap DAG
1. Upload `dw_shared_files_bootstrap.py` to the Composer DAGs folder.
2. Trigger the DAG `dw_shared_files_bootstrap` from the Airflow UI.
3. **Passing Criteria:** The DAG completes with `SUCCESS`. Verify that all variables are visible under **Admin -> Variables** in the Airflow UI.

### Step 2: Validate BigQuery Stored Procedures
Execute the following validation script in the BigQuery console:

```sql
-- Declare output variables
DECLARE v_dw_dir_root STRING;
DECLARE v_dw_dir_prot STRING;
DECLARE v_dw_dir_cubes STRING;
DECLARE v_dw_dir_imp_d1 STRING;
DECLARE v_dw_dir_imp_bwa STRING;
DECLARE v_dw_dir_imp_xtra STRING;
DECLARE v_dw_dir_imp_ctel STRING;
DECLARE v_dw_dir_imp_vo STRING;
DECLARE v_dw_dir_imp_rv STRING;
DECLARE v_dw_dir_imp_if STRING;
DECLARE v_dw_dir_imp_nnv STRING;
DECLARE v_dw_dir_imp_sigma STRING;
DECLARE v_dw_dir_exp_sigma STRING;
DECLARE v_dw_dir_imp_trf STRING;
DECLARE v_dw_dir_imp_auf STRING;
DECLARE v_dw_dir_imp_gut STRING;
DECLARE v_dw_dir_imp_kdg STRING;
DECLARE v_dw_dir_imp_mp_kdg STRING;
DECLARE v_dw_dir_imp_mp_ts STRING;
DECLARE v_dw_dir_imp_mp_zm STRING;
DECLARE v_dw_dir_imp_ts STRING;
DECLARE v_dw_dir_imp_zm STRING;
DECLARE v_dw_dir_exp STRING;
DECLARE v_dw_dir_imp_bpm STRING;
DECLARE v_dw_dir_imp_zts STRING;
DECLARE v_dw_dir_imp_vrs STRING;
DECLARE v_dw_dir_imp_brunet STRING;
DECLARE v_dw_dir_imp_dwh STRING;
DECLARE v_dw_dir_imp_plato STRING;
DECLARE v_dw_dir_imp_carmen STRING;
DECLARE v_dw_dir_imp_sap STRING;
DECLARE v_dw_dir_imp_sr_rv STRING;
DECLARE v_dw_dir_imp_sap_l STRING;
DECLARE v_dw_dir_imp_l_mahnstyp_ist STRING;
DECLARE v_dw_dir_imp_l_mahnv_fi STRING;
DECLARE v_dw_dir_imp_l_mahnv_ist STRING;
DECLARE v_dw_dir_imp_l_gutgr STRING;
DECLARE v_dw_dir_imp_l_leist STRING;
DECLARE v_dw_dir_imp_l_prod STRING;
DECLARE v_dw_dir_imp_lkode STRING;
DECLARE v_dw_dir_imp_subse STRING;
DECLARE v_dw_dir_sms_prg STRING;
DECLARE v_dw_dir_sms_adr STRING;
DECLARE v_dw_dir_sms_tmp STRING;
DECLARE v_dw_dir_imp_dpps STRING;
DECLARE v_dw_dir_imp_planf2 STRING;
DECLARE v_dw_host_customer STRING;
DECLARE v_oracle_home STRING DEFAULT '';
DECLARE v_dw_dir_utl_file STRING;

-- Call the initialization procedure
CALL `GCP_PROJECT.BQ_DATASET.dw_init`(
  'gs://gcp-dwh-environment-bucket/dw_source',
  'eDWH3',
  v_dw_dir_root, v_dw_dir_prot, v_dw_dir_cubes, v_dw_dir_imp_d1,
  v_dw_dir_imp_bwa, v_dw_dir_imp_xtra, v_dw_dir_imp_ctel, v_dw_dir_imp_vo,
  v_dw_dir_imp_rv, v_dw_dir_imp_if, v_dw_dir_imp_nnv, v_dw_dir_imp_sigma,
  v_dw_dir_exp_sigma, v_dw_dir_imp_trf, v_dw_dir_imp_auf, v_dw_dir_imp_gut,
  v_dw_dir_imp_kdg, v_dw_dir_imp_mp_kdg, v_dw_dir_imp_mp_ts, v_dw_dir_imp_mp_zm,
  v_dw_dir_imp_ts, v_dw_dir_imp_zm, v_dw_dir_exp, v_dw_dir_imp_bpm,
  v_dw_dir_imp_zts, v_dw_dir_imp_vrs, v_dw_dir_imp_brunet, v_dw_dir_imp_dwh,
  v_dw_dir_imp_plato, v_dw_dir_imp_carmen, v_dw_dir_imp_sap, v_dw_dir_imp_sr_rv,
  v_dw_dir_imp_sap_l, v_dw_dir_imp_l_mahnstyp_ist, v_dw_dir_imp_l_mahnv_fi,
  v_dw_dir_imp_l_mahnv_ist, v_dw_dir_imp_l_gutgr, v_dw_dir_imp_l_leist,
  v_dw_dir_imp_l_prod, v_dw_dir_imp_lkode, v_dw_dir_imp_subse, v_dw_dir_sms_prg,
  v_dw_dir_sms_adr, v_dw_dir_sms_tmp, v_dw_dir_imp_dpps, v_dw_dir_imp_planf2,
  v_dw_host_customer, v_oracle_home, v_dw_dir_utl_file
);

-- Assertions
ASSERT v_dw_dir_root = 'gs://gcp-dwh-environment-bucket/dw_source/aktuell' AS 'Root path mismatch';
ASSERT v_oracle_home = '/appl/local/oracle/12.2.0.1.0' AS 'Oracle Home resolution mismatch';
ASSERT v_dw_dir_utl_file = '/appl/local/oracle/admin/eDWH3/utl_file' AS 'UTL file path mismatch';
```

*   **Passing Criteria:** The query execution completes successfully without raising any assertion errors.

---

## 7. Rollback Procedure

In the event of an operational failure or inconsistent environment state, perform the following steps to roll back the configuration:

1.  **Delete Airflow Variables:**
    Run the following command to remove the imported variables from Cloud Composer:
    ```bash
    gcloud composer environments run COMPOSER_ENV_NAME \
        --location COMPOSER_REGION \
        variables -- delete DW_DIR_ROOT DW_DIR_PROT DW_DIR_CUBES
    ```
2.  **Drop BigQuery Procedures:**
    Remove the generated stored procedures from BigQuery:
    ```sql
    DROP PROCEDURE IF EXISTS `GCP_PROJECT.BQ_DATASET.dw_init`;
    DROP PROCEDURE IF EXISTS `GCP_PROJECT.BQ_DATASET.dw_global`;
    ```
3.  **Restore Legacy State:**
    If running in a hybrid environment, redirect downstream execution triggers back to the legacy on-premise environment files (`.dw_init` and `.dw_global`) hosted on the legacy ETL host (`dxcsa4.bn.detemobil.de`).