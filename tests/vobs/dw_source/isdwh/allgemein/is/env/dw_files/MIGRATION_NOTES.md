# Migration Notes

**Job/Component:** Shared Files — `vobs/dw_source/isdwh/allgemein/is/env/dw_files`  
**Target Platform:** Google Cloud Platform (GCP) — Cloud Composer (Apache Airflow) & BigQuery

---

## 1. Summary

The environment configuration files (`.dw_ai`, `.dw_db`, `.dw_global`, and `.dw_init`) historically used to initialize local UNIX shell variables, filesystem paths, and database connection parameters for the Information Services Data Warehouse (`ISDWH`) have been migrated to Google Cloud Platform.

* **Source Path:** `vobs/dw_source/isdwh/allgemein/is/env/dw_files`
* **Target Platform:** GCP (Cloud Composer & BigQuery)
* **Migration Pattern:** **UC4_ONLY / Cloud Composer Orchestration**. Local filesystem dependencies and shell-sourced variables are replaced by Airflow Variables, Google Cloud Secret Manager secrets, BigQuery Connection Objects, and BigQuery Stored Procedures.

---

## 2. Generated Artifacts

The migration process has generated the following target artifacts:

| Generated File Path | Target Platform | Role / Description |
| :--- | :--- | :--- |
| `composer_env_config.json` | Cloud Composer | JSON configuration file containing global Ab Initio and ETL environment variables to be imported into the Airflow Variable store. |
| `sp_dw_global.sql` | BigQuery | Stored Procedure (`metadata.sp_dw_global`) that validates the presence of required environment paths and outputs standard session locale parameters (`NLS_LANG`, `NLS_DATE_FORMAT`). |
| `sp_dw_init.sql` | BigQuery | Stored Procedure (`metadata.dw_init`) that dynamically constructs environment directory paths, resolves `ORACLE_HOME` based on system flags, and calls the global validation procedure. |
| *GCP Secret Manager Secret* | Secret Manager | Secure storage of the database password (`DB_PASSWD_DWH`) replacing the legacy `m_password` decryption utility. |
| *BigQuery Connection Object* | BigQuery Connection | External connection resource (`conn-edwh3-devlab`) replacing the legacy TNS entry (`DB_TNS_NAME_DWH`). |

---

## 3. Key Design Decisions

* **Procedural Encapsulation in BigQuery:** The validation logic and path-building rules from `.dw_global` and `.dw_init` were translated into BigQuery SQL Stored Procedures (`sp_dw_global` and `dw_init`). This encapsulates environment validation directly within the data warehouse layer, ensuring that SQL-based downstream processes can verify their context natively.
* **Separation of Secrets from Code:** Database credentials from `.dw_db` are decoupled from the codebase. Plaintext passwords and local decryption scripts (`m_password`) are replaced by Google Cloud Secret Manager references, which are resolved at runtime by authorized service accounts.
* **Centralized Environment Metadata:** Static configuration parameters from `.dw_ai` are consolidated into a JSON file (`composer_env_config.json`) for easy import into Airflow Variables, and mirrored in a persistent BigQuery configuration table (`PROJECT_DATASET.ETL_ENVIRONMENT_CONFIG`) for SQL-level access.
* **Trade-off on Filesystem Emulation:** Because BigQuery and Cloud Storage do not use a traditional POSIX filesystem, the directory paths (e.g., `DW_DIR_ROOT`) are maintained as string variables. Downstream tasks must map these paths to corresponding Google Cloud Storage (GCS) URIs (e.g., `gs://<bucket>/daten/...`) or local container mount points.

---

## 4. Manual Steps Before Go-Live

The following manual setup steps must be completed in the target environment before executing any dependent jobs:

### 4.1 Schema & Dataset Creation
Ensure the metadata dataset exists in your target BigQuery project:
```sql
CREATE SCHEMA IF NOT EXISTS `metadata`
OPTIONS(location="europe-west3");
```

### 4.2 IAM & Permissions
* **Composer Service Account:** The service account running the Cloud Composer environment must be granted the following roles:
  * `roles/secretmanager.secretAccessor` (to read database credentials).
  * `roles/bigquery.connectionUser` (to execute queries via the external connection).
  * `roles/bigquery.dataEditor` and `roles/bigquery.jobUser` on the target datasets.

### 4.3 Secrets Creation
Create the database password secret in Google Cloud Secret Manager:
```bash
gcloud secrets create meyreis-dwh-password \
    --replication-policy="automatic"

echo -n "YOUR_DB_PASSWORD" | gcloud secrets versions add meyreis-dwh-password --data-file=-
```

### 4.4 BigQuery Connection Setup
Create the external connection to the target Oracle database (if hybrid queries are required):
```bash
gcloud transfer connections create oracle \
    --connection="conn-edwh3-devlab" \
    --location="europe-west3" \
    --project="gcp-devlab-project" \
    ...
```

### 4.5 Airflow Variables Import
Import the generated `composer_env_config.json` into your Cloud Composer environment:
```bash
gcloud composer environments run COMPOSER_ENV_NAME \
    --location europe-west3 \
    variables import -- /path/to/composer_env_config.json
```

---

## 5. Known Gaps & Unresolved References

1. **Missing Source File (`.dw_lokal`):** The legacy `.dw_init` script attempts to source `$HOME/.dw_lokal`. This file was not present in the source code repository. A placeholder call to `metadata.sp_dw_lokal` is commented out in `sp_dw_init.sql` and must be implemented if local overrides are required.
2. **Missing Source File (`setpya.sh`):** The legacy `.dw_global` script references `. /appl/local/cognos/pya60207/setpya.sh`. This Cognos environment setup script was not provided. If Cognos integration is required, these environment variables must be manually added to the Airflow Variable store.
3. **Downstream Integration (Redesign B4):** Downstream consumers such as `DW.DWH_ABPZ_KKM_AIL_AGENT` and `r_ai_start` have not yet been migrated. The environment variables initialized by these stored procedures and Airflow variables will not take full effect until those downstream workloads are redeployed to target GCP services (e.g., Dataproc, Dataflow, or BigQuery).

---

## 6. Validation

To validate the migration of the environment configuration, execute the generated stored procedures and verify their outputs.

### 6.1 Execution Test
Run the following validation block in the BigQuery console:

```sql
DECLARE io_oracle_home STRING DEFAULT '';
DECLARE out_nls_lang STRING;
DECLARE out_nls_date_format STRING;

-- Test execution of sp_dw_global with valid parameters
CALL `metadata.sp_dw_global`(
  '/home/dir/aktuell', '/home/dir/daten/logfiles', '/home/dir/daten/cubes',
  '/home/dir/daten/d1', '/home/dir/daten/xtra', '/home/dir/daten/ctel',
  '/home/dir/daten/vo', '/home/dir/daten/rv', '/home/dir/daten/ees',
  '/home/dir/daten/nnv', '/appl/local/oracle/12.2.0.1.0',
  out_nls_lang, out_nls_date_format, _, _
);

-- Verify outputs
SELECT 
  out_nls_lang AS expected_german_lang, 
  out_nls_date_format AS expected_date_format;

-- Test execution of dw_init
CALL `metadata.dw_init`(io_oracle_home, 'DWH_SID', '/home/dir', TRUE, FALSE);
SELECT io_oracle_home AS resolved_oracle_home;
```

### 6.2 "Passing" Criteria
* The `sp_dw_global` procedure executes without throwing exceptions.
* The output variables return `GERMAN_GERMANY.WE8ISO8859P1` and `DD.MM.YY` respectively.
* The `dw_init` procedure successfully resolves `io_oracle_home` to `/appl/local/oracle/12.2.0.1.0` when `i_dir_oracle_12_exists` is set to `TRUE`.
* No new error entries are written to the `metadata.dw_environment_log` table during successful runs.

---

## 7. Rollback Procedure

In the event of a deployment failure or validation error, perform the following rollback steps:

1. **Drop BigQuery Artifacts:**
   ```sql
   DROP PROCEDURE IF EXISTS `metadata.dw_init`;
   DROP PROCEDURE IF EXISTS `metadata.sp_dw_global`;
   DROP TABLE IF EXISTS `metadata.dw_environment_log`;
   ```
2. **Remove Airflow Variables:**
   Delete the imported variables from the Airflow Metadata database via the Airflow UI (**Admin -> Variables**) or via the gcloud CLI:
   ```bash
   gcloud composer environments run COMPOSER_ENV_NAME \
       --location europe-west3 \
       variables delete -- AB_HOME AB_AIR_ROOT AB_AIR_HOME ETL_Host ETL_Projekt AI_PRIV_SAND_ROOT AI_ENV_SAND_ROOT AI_REPOSIT_TRACKING
   ```
3. **Remove Secrets:**
   If necessary, delete the secret version or the entire secret from Secret Manager:
   ```bash
   gcloud secrets delete meyreis-dwh-password --quiet
   ```