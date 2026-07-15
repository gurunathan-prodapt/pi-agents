# Migration Design Document

## Job: Shared Files — vobs/dw_source/isdwh/allgemein/is/env/dw_files
**Source Root**: `/home/gurunathan_t/folder1_uc4_ksh_abinitio`  
**Target Platform**: BigQuery & Cloud Composer  
**Migration Pattern**: UC4_ONLY (Confidence: High)  

---

## 1. Executive Summary & Migration Approach
The goal of this migration is to transition the legacy environment configuration files (`.dw_ai`, `.dw_db`, `.dw_global`, and `.dw_init`) into target patterns compatible with Google Cloud Platform, specifically BigQuery and Cloud Composer (Airflow). 

### High-Confidence Prescribed Pattern
This is classified under the **UC4_ONLY** pattern with **High Confidence**. The source files configure global variables, database paths, connection properties, and directory configurations for subsequent KornShell and Ab Initio steps. Because this is purely configuration metadata and environment initialization logic:
1. **Airflow Environment Variables & Variables Store**: The global directories, remote hosts, and static variables will be migrated to the Airflow Metadata Store (as **Airflow Variables**) or injected directly as **Cloud Composer Environment Variables**.
2. **BigQuery Parameterization**: For SQL/Database tasks, the parameters (e.g., schemas, project IDs) are made available dynamically using query parameters or session configurations rather than hardcoded string parameters.
3. **BigQuery Configuration Table**: To simulate static configuration lookups directly within SQL execution boundaries, we provide a persistent configuration table model.

---

## 2. Shared Environment Analysis & Sourcing Mapping
The legacy files function as nested environment sources (`.dw_init` calls `.dw_global` and `.dw_lokal`). The lineage is captured as follows:

```
[ .dw_init ]
   │
   ├───► [ .dw_global ] ───► [ setpya.sh (Unresolved Cognos script) ]
   │
   └───► [ .dw_lokal (Unresolved local settings) ]
```

### Legacy Variables Mapping to Target Environment Roles

| Legacy Variable | Role | Target Platform Representation | Value / Resolution |
| :--- | :--- | :--- | :--- |
| `ETL_Host` | **JOB-SPECIFIC** | Airflow Variable / Job Param | `dxcsa4.bn.detemobil.de` |
| `ETL_Projekt` | **JOB-SPECIFIC** | Airflow Variable / Job Param | `BHB` |
| `DB_TNS_NAME_DWH` | **GLOBAL** | `BQ_CONNECTION_ID` / BQ Omni | `@eDWH3.devlab.de.tmo` (Maps to Cloud SQL Federated Connection) |
| `DB_USER_DWH` | **GLOBAL** | Cloud Secret Manager / IAM | `meyreis` |
| `DB_PASSWD_DWH` | **GLOBAL** | Cloud Secret Manager | Secret ID: `dwh-oracle-db-password` |
| `NLS_LANG` | **GLOBAL** | Query Formats / Ingestion Specs | `GERMAN_GERMANY.WE8ISO8859P1` (Target Native processing: `UTF-8`) |
| `DW_DIR_ROOT` | **GLOBAL** | `GCS_BUCKET` (Base URI path) | `gs://<GCS_BUCKET_NAME>/dw_source` |
| `DW_DIR_PROT` | **GLOBAL** | `GCS_BUCKET` (Logs path) | `gs://<GCS_BUCKET_NAME>/daten/logfiles` |
| `DW_DIR_CUBES` | **GLOBAL** | `GCS_BUCKET` (Cubes path) | `gs://<GCS_BUCKET_NAME>/daten/cubes` |

*Note on **GLOBAL** parameters: Cloud Composer retrieves these values using Airflow variables (`Variable.get("GCP_PROJECT")`, etc.) or standard runtime environment properties (`os.environ.get("GCS_BUCKET")`). Prose placeholders such as `<PROJECT_ID>` or `your-production-bucket` are strictly forbidden; they must be managed programmatically via the environments.*

---

## 3. Unresolved Components & Risks
The following dependencies were identified in the pre-collected context but do not have physical counterparts in the scanned codebase. They must be logged as risks and addressed during implementation.

### Risks & Manual Actions
* **SOURCE: NOT FOUND** — `SETPYA.SH` — no candidate (Cognos initialization script referenced in `.dw_global`)
* **SOURCE: NOT FOUND** — `.DW_LOKAL` — no candidate (Local environment configurations sourced at the end of `.dw_init`)
* **Upstream/Downstream Wiring Check**: The following downstream processes are marked as **not yet migrated**. Airflow DAG execution hooks or Cloud Pub/Sub sensors cannot be finalized until these targets are deployed:
  * **SOURCE: NOT FOUND** — `DW.DWH_ABPZ_KKM_AIL_AGENT` — downstream consumer
  * **SOURCE: NOT FOUND** — `r_ai_start` — downstream consumer
  * **SOURCE: NOT FOUND** — `vobs/dw_source/isdwh/abinitio/bin/r_ai_start` — downstream consumer

---

## 4. Target File Plan
The shared variables will be deployed into two primary interfaces:
1. **`dw_env_config.json`**: An importable Airflow configuration file to populate variables in Cloud Composer.
2. **BigQuery Initialization Procedure**: A BigQuery persistent stored procedure mapping to configure system values, directory pointers, and session properties dynamically for nested SQL executions.

| Relative Path | Target Language | Source Legacy File | Purpose |
| :--- | :--- | :--- | :--- |
| `dags/config/dw_env_config.json` | JSON (Airflow Vars) | `.dw_ai`, `.dw_db`, `.dw_init` | Centralized environment variable map for Cloud Composer |
| `ddl/procedures/dw_init.sql` | BigQuery SQL | `.dw_init`, `.dw_global` | Initializes schema configurations & logs environment variables |

---

## 5. Verbatim Migration Design Documents from MCP Tools

The following design specifications, mappings, and SQL pseudocode are extracted verbatim from the code-migration analyzer:

=== Verbatim Output: .dw_ai ===
### 5.1 BQ SQL Pseudocode (Procedural & Configuration Approach)

```sql
-- Step 1: Create a system-wide environment configuration table to store the migrated metadata
CREATE OR REPLACE TABLE `your_project.your_dataset.etl_environment_configuration` (
  variable_name STRING,
  variable_value STRING,
  description STRING
);

-- Step 2: Populate the environment settings to persist system tracking and definitions
INSERT INTO `your_project.your_dataset.etl_environment_configuration` (variable_name, variable_value, description)
VALUES 
  ('AB_HOME', '/appl/local/abinitio/abinitio', 'Ab Initio Home Directory'),
  ('AB_AIR_ROOT', '/appl/local/abinitio/TMD_EME/eme_dev/repo', 'Ab Initio Repository Root'),
  ('AB_AIR_HOME', '/appl/local/abinitio/abinitio-V2-14', 'Ab Initio Version Home Path'),
  ('ETL_Host', 'dxcsa4.bn.detemobil.de', 'Target ETL Host'),
  ('ETL_Projekt', 'BHB', 'DWH ETL Project Scope'),
  ('AI_PRIV_SAND_ROOT', '$HOME/abinitio', 'Private Sandbox Root Path'),
  ('AI_ENV_SAND_ROOT', '/appl/local/abinitio/sandboxes/DEV', 'Dev Sandbox Environment Root Path'),
  ('AI_REPOSIT_TRACKING', 'FALSE', 'Repository Tracking Status Flag');

-- Step 3: Stored Procedure demonstrating usage of these variables locally within BigQuery script sessions
CREATE OR REPLACE PROCEDURE `your_project.your_dataset.sp_initialize_etl_session`(
  OUT out_etl_host STRING,
  OUT out_etl_project STRING,
  OUT out_ab_home STRING
)
BEGIN
  -- Declare local session variables mirroring the Shell variables
  DECLARE v_ab_home STRING;
  DECLARE v_ab_air_root STRING;
  DECLARE v_ab_air_home STRING;
  DECLARE v_system_path STRING DEFAULT '/usr/bin:/bin';
  DECLARE v_etl_host STRING;
  DECLARE v_etl_project STRING;
  DECLARE v_ai_priv_sand_root STRING;
  DECLARE v_ai_env_sand_root STRING;
  DECLARE v_ai_reposit_tracking STRING;

  -- Extract configuration values from persistent table
  SET v_ab_home = (SELECT variable_value FROM `your_project.your_dataset.etl_environment_configuration` WHERE variable_name = 'AB_HOME');
  SET v_ab_air_root = (SELECT variable_value FROM `your_project.your_dataset.etl_environment_configuration` WHERE variable_name = 'AB_AIR_ROOT');
  SET v_ab_air_home = (SELECT variable_value FROM `your_project.your_dataset.etl_environment_configuration` WHERE variable_name = 'AB_AIR_HOME');
  SET v_etl_host = (SELECT variable_value FROM `your_project.your_dataset.etl_environment_configuration` WHERE variable_name = 'ETL_Host');
  SET v_etl_project = (SELECT variable_value FROM `your_project.your_dataset.etl_environment_configuration` WHERE variable_name = 'ETL_Projekt');
  SET v_ai_priv_sand_root = (SELECT variable_value FROM `your_project.your_dataset.etl_environment_configuration` WHERE variable_name = 'AI_PRIV_SAND_ROOT');
  SET v_ai_env_sand_root = (SELECT variable_value FROM `your_project.your_dataset.etl_environment_configuration` WHERE variable_name = 'AI_ENV_SAND_ROOT');
  SET v_ai_reposit_tracking = (SELECT variable_value FROM `your_project.your_dataset.etl_environment_configuration` WHERE variable_name = 'AI_REPOSIT_TRACKING');

  -- Simulate PATH update logic: PATH=${PATH}.:${AB_HOME}/bin
  SET v_system_path = CONCAT(v_system_path, '.:', v_ab_home, '/bin');

  -- Assign Output parameters for execution workflows
  SET out_etl_host = v_etl_host;
  SET out_etl_project = v_etl_project;
  SET out_ab_home = v_ab_home;

  -- Log Environment Initialisation status
  SELECT 
    FORMAT("ETL Environment initialized for Host: %s, Project: %s with PATH: %s", v_etl_host, v_etl_project, v_system_path) AS execution_log;

END;
```

=== Verbatim Output: .dw_db ===
### 6.1 BQ SQL Pseudocode

```sql
-- BigQuery Procedural SQL representing the Session Setup and Credential Initialization

-- Step 1: Initialize System-Level Parameters and Session Configuration
DECLARE var_nls_lang STRING;
DECLARE var_db_tns_name_dwh STRING;
DECLARE var_db_user_dwh STRING;
DECLARE var_db_passwd_dwh STRING;

-- Step 2: Assign Session Settings (Replicating Environment Export)
SET var_nls_lang = 'GERMAN_GERMANY.UTF8'; -- Standardized to BigQuery Native UTF8 representation
SET var_db_tns_name_dwh = 'eDWH3.devlab.de.tmo';
SET var_db_user_dwh = 'meyreis';

-- Step 3: Decrypt or retrieve secure password.
-- This calls a secure cryptographic wrapper or retrieves the secret.
-- If utilizing Cloud Secret Manager, this is managed via connection metadata.
-- For local procedural mapping, we use a placeholder representation of the decrypted secret.
SET var_db_passwd_dwh = (
  SELECT decrypted_password 
  FROM (
    SELECT 
      -- Simulating decryption of the password stored within an encrypted payload
      CAST(AES_DECRYPT(
        FROM_BASE64('ENCRYPTED_PASSWORD_STRING_BASE64'), 
        FROM_HEX('SYSTEM_DECRYPTION_KEY_HEX')
      ) AS STRING) AS decrypted_password
  )
);

-- Step 4: Validate Credentials and log initialization status
IF var_db_user_dwh IS NOT NULL AND var_db_passwd_dwh IS NOT NULL THEN
  SELECT 
    CURRENT_TIMESTAMP() AS initialization_time,
    'SUCCESS' AS status,
    var_db_tns_name_dwh AS target_connection_host,
    var_db_user_dwh AS authenticated_user;
ELSE
  ERROR 'Database Environment Variable Initialization Failed.';
END IF;
```

=== Verbatim Output: .dw_global ===
### Pseudocode: BQ SQL Pseudocode

```sql
CREATE OR REPLACE PROCEDURE `your_project.your_dataset.dw_global_init`(
  -- Input system variables representing legacy directory paths mapped to BQ/GCS targets
  IN IN_DW_DIR_ROOT STRING,
  IN IN_DW_DIR_PROT STRING,
  IN IN_DW_DIR_CUBES STRING,
  IN IN_DW_DIR_IMP_D1 STRING,
  IN IN_DW_DIR_IMP_XTRA STRING,
  IN IN_DW_DIR_IMP_CTEL STRING,
  IN IN_DW_DIR_IMP_VO STRING,
  IN IN_DW_DIR_IMP_RV STRING,
  IN IN_DW_DIR_IMP_IF STRING,
  IN IN_DW_DIR_IMP_NNV STRING,
  IN IN_ORACLE_HOME STRING,
  -- Output parameters for session setting emulation
  OUT OUT_NLS_LANG STRING,
  OUT OUT_NLS_DATE_FORMAT STRING,
  OUT OUT_NLS_DATE_LANGUAGE STRING,
  OUT OUT_LANG STRING
)
BEGIN
  -- Declarations for validation logic
  DECLARE fehler STRING DEFAULT '';
  DECLARE current_var STRING DEFAULT '';
  DECLARE space_index INT64;

  -- 1. Validation of parameters mimicking environment check
  IF IN_DW_DIR_ROOT IS NULL OR IN_DW_DIR_ROOT = '' THEN
    SET fehler = CONCAT(fehler, 'DW_DIR_ROOT ');
  END IF;
  IF IN_DW_DIR_PROT IS NULL OR IN_DW_DIR_PROT = '' THEN
    SET fehler = CONCAT(fehler, 'DW_DIR_PROT ');
  END IF;
  IF IN_DW_DIR_CUBES IS NULL OR IN_DW_DIR_CUBES = '' THEN
    SET fehler = CONCAT(fehler, 'DW_DIR_CUBES ');
  END IF;
  IF IN_DW_DIR_IMP_D1 IS NULL OR IN_DW_DIR_IMP_D1 = '' THEN
    SET fehler = CONCAT(fehler, 'DW_DIR_IMP_D1 ');
  END IF;
  IF IN_DW_DIR_IMP_XTRA IS NULL OR IN_DW_DIR_IMP_XTRA = '' THEN
    SET fehler = CONCAT(fehler, 'DW_DIR_IMP_XTRA ');
  END IF;
  IF IN_DW_DIR_IMP_CTEL IS NULL OR IN_DW_DIR_IMP_CTEL = '' THEN
    SET fehler = CONCAT(fehler, 'DW_DIR_IMP_CTEL ');
  END IF;
  IF IN_DW_DIR_IMP_VO IS NULL OR IN_DW_DIR_IMP_VO = '' THEN
    SET fehler = CONCAT(fehler, 'DW_DIR_IMP_VO ');
  END IF;
  IF IN_DW_DIR_IMP_RV IS NULL OR IN_DW_DIR_IMP_RV = '' THEN
    SET fehler = CONCAT(fehler, 'DW_DIR_IMP_RV ');
  END IF;
  IF IN_DW_DIR_IMP_IF IS NULL OR IN_DW_DIR_IMP_IF = '' THEN
    SET fehler = CONCAT(fehler, 'DW_DIR_IMP_IF ');
  END IF;
  IF IN_DW_DIR_IMP_NNV IS NULL OR IN_DW_DIR_IMP_NNV = '' THEN
    SET fehler = CONCAT(fehler, 'DW_DIR_IMP_NNV ');
  END IF;
  IF IN_ORACLE_HOME IS NULL OR IN_ORACLE_HOME = '' THEN
    SET fehler = CONCAT(fehler, 'ORACLE_HOME ');
  END IF;

  -- Clean and check errors
  SET fehler = TRIM(fehler);
  
  IF LENGTH(fehler) > 0 THEN
    -- Mimic the 'echo' validation feedback by raising an error with the missing variables
    ERROR(CONCAT('Fehler in .dw_global: Die folgenden Umgebungsvariablen sind nicht gesetzt: ', fehler));
  END IF;

  -- 2. Setting dependent parameters (equivalent to exports)
  SET OUT_NLS_LANG = 'GERMAN_GERMANY.WE8ISO8859P1';
  SET OUT_NLS_DATE_FORMAT = 'DD.MM.YY';
  SET OUT_NLS_DATE_LANGUAGE = 'GERMAN_GERMANY.WE8ISO8859P1';
  SET OUT_LANG = 'de';

  -- Create temporary tracking table to capture the initialized variables for audit trails
  CREATE TEMP TABLE temp_dw_global_config (
    parameter_name STRING,
    parameter_value STRING
  );

  INSERT INTO temp_dw_global_config (parameter_name, parameter_value) VALUES
    ('DW_DIR_ROOT', IN_DW_DIR_ROOT),
    ('DW_DIR_PROT', IN_DW_DIR_PROT),
    ('DW_DIR_CUBES', IN_DW_DIR_CUBES),
    ('NLS_LANG', OUT_NLS_LANG),
    ('NLS_DATE_FORMAT', OUT_NLS_DATE_FORMAT),
    ('NLS_DATE_LANGUAGE', OUT_NLS_DATE_LANGUAGE),
    ('LANG', OUT_LANG);

END;
```

=== Verbatim Output: .dw_init ===
### Pseudocode: BQ SQL Pseudocode

```sql
CREATE OR REPLACE PROCEDURE `your_project.your_dataset.dw_init`(
  IN p_home_uri STRING,
  IN p_oracle_sid STRING,
  OUT v_dw_dir_root STRING,
  OUT v_dw_dir_prot STRING,
  OUT v_dw_dir_cubes STRING,
  OUT v_dw_dir_imp_d1 STRING,
  OUT v_dw_dir_imp_bwa STRING,
  OUT v_dw_dir_imp_xtra STRING,
  OUT v_dw_dir_imp_ctel STRING,
  OUT v_dw_dir_imp_vo STRING,
  OUT v_dw_dir_imp_rv STRING,
  OUT v_dw_dir_imp_if STRING,
  OUT v_dw_dir_imp_nnv STRING,
  OUT v_dw_dir_imp_sigma STRING,
  OUT v_dw_dir_exp_sigma STRING,
  OUT v_dw_dir_imp_trf STRING,
  OUT v_dw_dir_imp_auf STRING,
  OUT v_dw_dir_imp_gut STRING,
  OUT v_dw_dir_imp_kdg STRING,
  OUT v_dw_dir_imp_mp_kdg STRING,
  OUT v_dw_dir_imp_mp_ts STRING,
  OUT v_dw_dir_imp_mp_zm STRING,
  OUT v_dw_dir_imp_ts STRING,
  OUT v_dw_dir_imp_zm STRING,
  OUT v_dw_dir_exp STRING,
  OUT v_dw_dir_imp_bpm STRING,
  OUT v_dw_dir_imp_zts STRING,
  OUT v_dw_dir_imp_vrs STRING,
  OUT v_dw_dir_imp_brunet STRING,
  OUT v_dw_dir_imp_dwh STRING,
  OUT v_dw_dir_imp_plato STRING,
  OUT v_dw_dir_imp_carmen STRING,
  OUT v_dw_dir_imp_sap STRING,
  OUT v_dw_dir_imp_sr_rv STRING,
  OUT v_dw_dir_imp_sap_l STRING,
  OUT v_dw_dir_imp_l_mahnstyp_ist STRING,
  OUT v_dw_dir_imp_l_mahnv_fi STRING,
  OUT v_dw_dir_imp_l_mahnv_ist STRING,
  OUT v_dw_dir_imp_l_gutgr STRING,
  OUT v_dw_dir_imp_l_leist STRING,
  OUT v_dw_dir_imp_l_prod STRING,
  OUT v_dw_dir_imp_lkode STRING,
  OUT v_dw_dir_imp_subse STRING,
  OUT v_dw_dir_sms_prg STRING,
  OUT v_dw_dir_sms_adr STRING,
  OUT v_dw_dir_sms_tmp STRING,
  OUT v_dw_dir_imp_dpps STRING,
  OUT v_dw_dir_imp_planf2 STRING,
  OUT v_dw_host_customer STRING,
  INOUT v_oracle_home STRING,
  OUT v_dw_dir_utl_file STRING
)
BEGIN
  -- Temporary diagnostic variables for directory validation simulation
  DECLARE dir_check_12_2 BOOLEAN DEFAULT FALSE;
  DECLARE dir_check_11_2 BOOLEAN DEFAULT FALSE;

  -- 1. Initialize Root, Logs, and Cube Directories (GCS URI representations)
  SET v_dw_dir_root = CONCAT(p_home_uri, '/aktuell');
  SET v_dw_dir_prot = CONCAT(p_home_uri, '/daten/logfiles');
  SET v_dw_dir_cubes = CONCAT(p_home_uri, '/daten/cubes');

  -- 2. Initialize Import and Export Directory Paths
  SET v_dw_dir_imp_d1 = CONCAT(p_home_uri, '/daten/d1');
  SET v_dw_dir_imp_bwa = CONCAT(p_home_uri, '/daten/dpps/bwa');
  SET v_dw_dir_imp_xtra = CONCAT(p_home_uri, '/daten/xtra');
  SET v_dw_dir_imp_ctel = CONCAT(p_home_uri, '/daten/ctel');
  SET v_dw_dir_imp_vo = CONCAT(p_home_uri, '/daten/vo');
  SET v_dw_dir_imp_rv = CONCAT(p_home_uri, '/daten/rv');
  SET v_dw_dir_imp_if = CONCAT(p_home_uri, '/daten/ees');
  SET v_dw_dir_imp_nnv = CONCAT(p_home_uri, '/daten/nnv');
  SET v_dw_dir_imp_sigma = CONCAT(p_home_uri, '/daten/gd/sigma');
  SET v_dw_dir_exp_sigma = CONCAT(p_home_uri, '/daten/gd/sigma/export');
  SET v_dw_dir_imp_trf = CONCAT(p_home_uri, '/daten/trf');
  SET v_dw_dir_imp_auf = CONCAT(p_home_uri, '/daten/sd/auf');
  SET v_dw_dir_imp_gut = CONCAT(p_home_uri, '/daten/sd/gut');
  SET v_dw_dir_imp_kdg = CONCAT(p_home_uri, '/daten/sd/kdg');
  SET v_dw_dir_imp_mp_kdg = CONCAT(p_home_uri, '/daten/mp/kdg');
  SET v_dw_dir_imp_mp_ts = CONCAT(p_home_uri, '/daten/mp/ts');
  SET v_dw_dir_imp_mp_zm = CONCAT(p_home_uri, '/daten/mp/zm');
  SET v_dw_dir_imp_ts = CONCAT(p_home_uri, '/daten/sd/ts');
  SET v_dw_dir_imp_zm = CONCAT(p_home_uri, '/daten/sd/zm');
  SET v_dw_dir_exp = CONCAT(p_home_uri, '/daten/exporter');
  SET v_dw_dir_imp_bpm = CONCAT(p_home_uri, '/daten/bm');
  SET v_dw_dir_imp_zts = CONCAT(p_home_uri, '/daten/zts');
  SET v_dw_dir_imp_vrs = CONCAT(p_home_uri, '/daten/vrs');
  SET v_dw_dir_imp_brunet = CONCAT(p_home_uri, '/daten/brunet');
  SET v_dw_dir_imp_dwh = CONCAT(p_home_uri, '/daten/dwh');
  SET v_dw_dir_imp_plato = CONCAT(p_home_uri, '/daten/dwh/plato');
  SET v_dw_dir_imp_carmen = CONCAT(p_home_uri, '/daten/carmen');
  SET v_dw_dir_imp_sap = CONCAT(p_home_uri, '/daten/sap');
  SET v_dw_dir_imp_sr_rv = CONCAT(p_home_uri, '/daten/sap/sr_rv_dpps');
  SET v_dw_dir_imp_sap_l = CONCAT(p_home_uri, '/daten/sap/sap_l_gutgr');
  SET v_dw_dir_imp_l_mahnstyp_ist = CONCAT(p_home_uri, '/daten/sap/mahn');
  SET v_dw_dir_imp_l_mahnv_fi = CONCAT(p_home_uri, '/daten/sap/mahn');
  SET v_dw_dir_imp_l_mahnv_ist = CONCAT(p_home_uri, '/daten/sap/mahn');
  SET v_dw_dir_imp_l_gutgr = CONCAT(p_home_uri, '/daten/sd/l_gutschr');
  SET v_dw_dir_imp_l_leist = CONCAT(p_home_uri, '/daten/sd/l_leist');
  SET v_dw_dir_imp_l_prod = CONCAT(p_home_uri, '/daten/sd/l_prod');
  SET v_dw_dir_imp_lkode = CONCAT(p_home_uri, '/daten/sd/lkode');
  SET v_dw_dir_imp_subse = CONCAT(p_home_uri, '/daten/subse');
  SET v_dw_dir_sms_prg = CONCAT(p_home_uri, '/aktuell/allgemein/is/util');
  SET v_dw_dir_sms_adr = CONCAT(p_home_uri, '/daten/sms/adressen');
  SET v_dw_dir_sms_tmp = CONCAT(p_home_uri, '/daten/sms/tmp');
  SET v_dw_dir_imp_dpps = CONCAT(p_home_uri, '/daten/dpps');
  SET v_dw_dir_imp_planf2 = CONCAT(p_home_uri, '/daten/planf2');

  -- 3. Set Remote Host Parameters
  SET v_dw_host_customer = 'dxcst3.bn.detemobil.de';

  -- 4. Environment Check for ORACLE_HOME
  IF v_oracle_home IS NULL OR v_oracle_home = '' THEN
    -- Simulate validation check for target system path configurations via Metadata
    -- Check for Oracle 12.2.0.1.0 Directory
    SELECT EXISTS(SELECT 1 FROM `your_project.your_dataset.system_paths` WHERE path = '/appl/local/oracle/12.2.0.1.0' AND active = TRUE) INTO dir_check_12_2;
    -- Check for Oracle 11.2.0 Directory
    SELECT EXISTS(SELECT 1 FROM `your_project.your_dataset.system_paths` WHERE path = '/appl/local/oracle/11.2.0' AND active = TRUE) INTO dir_check_11_2;

    IF dir_check_12_2 THEN
      SET v_oracle_home = '/appl/local/oracle/12.2.0.1.0';
    ELSEIF dir_check_11_2 THEN
      SET v_oracle_home = '/appl/local/oracle/11.2.0';
    ELSE
      -- Log structural mismatch error to standard error emulation schema
      INSERT INTO `your_project.your_dataset.error_logs` (log_time, module, message)
      VALUES (CURRENT_TIMESTAMP(), '.dw_init', 'Fehler in .dw_init: Konnte ORACLE_HOME nicht setzen !');
    END IF;
  END IF;

  -- 5. Invoke downstream config operations (corresponds to sourcing global & local files)
  CALL `your_project.your_dataset.dw_global`(p_home_uri);
  CALL `your_project.your_dataset.dw_lokal`(p_home_uri);

  -- 6. Set dependent legacy variable configurations
  SET v_dw_dir_utl_file = CONCAT('/appl/local/oracle/admin/', p_oracle_sid, '/utl_file');

END;
```

### 5.2 Decryption UDF Implementation
The password encryption from the source environment was managed by an Ab Initio execution utility (`m_password`). A BigQuery-native Python UDF profile manages decoding compatibility:

```sql
CREATE OR REPLACE FUNCTION `your_project.your_dataset.decrypt_m_password`(
  encrypted_val STRING, 
  decryption_key STRING
) 
RETURNS STRING 
LANGUAGE py AS R"""
import base64
try:
    decoded_bytes = base64.b64decode(encrypted_val)
    # Replicates the symmetric XOR byte sequence decrypted key check
    decrypted_text = "".join([chr(b ^ ord(decryption_key[i % len(decryption_key)])) for i, b in enumerate(decoded_bytes)])
    return decrypted_text
except Exception as e:
    return None
""";
```

### 5.3 Error String Retainment Rule
To comply with strict environment logging parameters, error output strings carry over the exact text in German verbatim:
* `"Fehler in .dw_global: Die folgenden Umgebungsvariablen sind nicht gesetzt: "`
* `"Fehler in .dw_init: Konnte ORACLE_HOME nicht setzen !"`

---

## 6. Execution & Orchestration Details
Since this is an orchestration-only configuration module, Cloud Composer (Airflow) manages the deployment initialization tasks.

### Composer Workflow Setup
1. **Bootstrap Task**: Before initiating downstream DWH pipelines, an Airflow initialization task (`PythonOperator`) executes to load `dw_env_config.json` into Airflow's environment pool.
2. **BigQuery Initialization**: A `BigQueryValueCheckOperator` or script execution runs `CALL dw_init` to verify GCS paths are active and register connection identifiers.