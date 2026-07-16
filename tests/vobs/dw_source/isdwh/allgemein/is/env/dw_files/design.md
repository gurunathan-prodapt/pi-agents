# MIGRATION DESIGN DOCUMENT

## 1. Executive Summary & Migration Pattern
This document details the target design for migrating the environment configurations (`shared_files`) of the Information Services Data Warehouse (`ISDWH`) system.
* **Source Path:** `vobs/dw_source/isdwh/allgemein/is/env/dw_files`
* **Source Files:** `.dw_ai`, `.dw_db`, `.dw_global`, and `.dw_init`
* **Prescribed Migration Pattern:** **UC4_ONLY / Cloud Composer Orchestration**.
* **Target Architecture:** Instead of using local UNIX shell variables and filesystem mappings, these parameters are migrated into Google Cloud Platform (GCP) configurations. 
  - Dynamic execution environments are modeled via **Airflow Variables** and **DAG parameters** in **Cloud Composer**.
  - Database connection variables map to **BigQuery Connection Objects** and **Google Cloud Secret Manager**.
  - Static configuration sets are persisted in a central metadata config table in **BigQuery**.

---

## 2. File Disposition Table

Every file from the pre-collected context is mapped to its target execution plan below:

| Source File Path | Target File / Execution Plan | Disposition | Explanation |
| :--- | :--- | :--- | :--- |
| `vobs/dw_source/isdwh/allgemein/is/env/dw_files/.dw_ai` | `dags/config/composer_env_config.json` | Merged into Global Composer Config | Configures global path and Ab Initio sandbox structures as Airflow metadata. |
| `vobs/dw_source/isdwh/allgemein/is/env/dw_files/.dw_db` | Cloud Secret Manager & BQ Connection Object | Merged into GCP IAM & Connection API | Credentials migrated to Secret Manager; Oracle connection endpoint migrated to a BigQuery Connection ID. |
| `vobs/dw_source/isdwh/allgemein/is/env/dw_files/.dw_global` | `gcp_composer/dags/sub_dags/sp_dw_global.sql` | Merged into BQ Stored Procedure & Airflow | Translated validation and NLS parameters into BigQuery SP validation and global variables. |
| `vobs/dw_source/isdwh/allgemein/is/env/dw_files/.dw_init` | `gcp_composer/dags/sub_dags/sp_dw_init.sql` | Merged into BQ Stored Procedure & Airflow | Sets up path hierarchies dynamically inside BigQuery or Cloud Storage URI paths. |

---

## 3. Environment Variable Classification (ENV Variable Policy)

To avoid prose placeholders and establish clean execution environments on GCP, legacy variables are mapped to either **GLOBAL** infrastructure properties or **JOB-SPECIFIC** configurations.

### 3.1 Global Variables (Environment-wide infrastructure)
These values are resolved at runtime via Airflow's Variable store (`Variable.get("NAME")`) or passed as parameters to BigQuery.

| Legacy Variable Name | GCP Target Equivalent | Retrieval Mechanism (Composer/SQL) |
| :--- | :--- | :--- |
| *System Config* | `GCP_PROJECT` | `Variable.get("GCP_PROJECT")` / `@gcp_project` |
| *System Config* | `GCP_REGION` | `Variable.get("GCP_REGION")` |
| `ETL_Host` | `ETL_HOST` | `Variable.get("ETL_HOST")` (Default: `dxcsa4.bn.detemobil.de`) |
| `ETL_Projekt` | `ETL_PROJEKT` | `Variable.get("ETL_PROJEKT")` (Default: `BHB`) |
| `NLS_LANG` | `NLS_LANG` | Script parameter / Constant `GERMAN_GERMANY.UTF8` |
| `NLS_DATE_FORMAT` | `NLS_DATE_FORMAT` | Query parameter / Constant `DD.MM.YY` |

### 3.2 Job-Specific Variables (Parameters unique to this job context)
These values are defined directly inside Airflow DAG `params` or inlined inside BigQuery logic.

| Legacy Variable Name | Target Value | Retrieval Mechanism |
| :--- | :--- | :--- |
| `DB_USER_DWH` | `meyreis` | Extracted from `Variable.get("job_db_user_dwh")` |
| `DB_TNS_NAME_DWH` | `projects/gcp-devlab-project/locations/europe-west3/connections/conn-edwh3-devlab` | BigQuery External Connection ID |
| `DB_PASSWD_DWH` | `projects/gcp-devlab-project/secrets/meyreis-dwh-password/versions/latest` | Google Cloud Secret Manager Secret Path |

---

## 4. Risks & Manual Actions

1. **SOURCE: NOT FOUND** — `.dw_lokal` — no candidate (Referenced by `.dw_init` as `. $HOME/.dw_lokal` but no source file exists).
2. **SOURCE: NOT FOUND** — `setpya.sh` — no candidate (Referenced by `.dw_global` as `. /appl/local/cognos/pya60207/setpya.sh` but no source file exists).
3. **UPSTREAM: NOT MIGRATED** — The downstream job chains (`DW.DWH_ABPZ_KKM_AIL_AGENT`, `r_ai_start`, and `vobs/dw_source/isdwh/abinitio/bin/r_ai_start`) are not yet migrated to Cloud Composer. The Composer DAG generated for these shared environment variables must be linked as a predecessor via cross-DAG execution sensors or event triggers once downstream files are migrated.
4. **CREDENTIAL MANAGEMENT** — The Oracle DB encrypted credentials mapped from `m_password` inside `.dw_db` must be manually imported into Google Cloud Secret Manager and associated with the service account running the Composer cluster.

---

## 5. Job Dependencies & Lineage Edges

* **Upstream Triggers:** Inherited/Scheduled or called initialization sequences. This job is a prerequisite for all DWH execution.
* **Downstream Consumers (Cross-Job Hand-off):**
  - `DW.DWH_ABPZ_KKM_AIL_AGENT` (Not Yet Migrated)
  - `r_ai_start` (Not Yet Migrated)
  - `vobs/dw_source/isdwh/abinitio/bin/r_ai_start` (Not Yet Migrated)
* **Target Connection Flow:** Downstream DAGs will sensor the completion of the Composer orchestration initialization pipeline before launching Ab Initio Spark workloads or BigQuery procedures.

---

## 6. Verbatim Source Translation & Target Logic (MCP Output)

Below is the verbatim functional translation of each environment config file to BigQuery procedural code and Python-based structures.

### 6.1 Translation of `.dw_ai`
```sql
-- Create a persistent configuration table to store ETL Environment variables if it doesn't exist
CREATE TABLE IF NOT EXISTS `PROJECT_DATASET.ETL_ENVIRONMENT_CONFIG` (
  variable_name STRING NOT NULL,
  variable_value STRING,
  description STRING,
  updated_timestamp TIMESTAMP
);

-- Merge the script's exported variables into the configuration table
MERGE `PROJECT_DATASET.ETL_ENVIRONMENT_CONFIG` AS target
USING (
  SELECT 'AB_HOME' AS variable_name, '/appl/local/abinitio/abinitio' AS variable_value, 'Ab Initio Home directory' AS description UNION ALL
  SELECT 'AB_AIR_ROOT', '/appl/local/abinitio/TMD_EME/eme_dev/repo', 'Enterprise Metadata Repository root path' UNION ALL
  SELECT 'AB_AIR_HOME', '/appl/local/abinitio/abinitio-V2-14', 'Ab Initio AIR Home' UNION ALL
  SELECT 'ETL_Host', 'dxcsa4.bn.detemobil.de', 'Target ETL Host Server' UNION ALL
  SELECT 'ETL_Projekt', 'BHB', 'DWH Project Identifier' UNION ALL
  SELECT 'AI_PRIV_SAND_ROOT', '~/abinitio', 'Private Sandbox Root Directory' UNION ALL
  SELECT 'AI_ENV_SAND_ROOT', '/appl/local/abinitio/sandboxes/DEV', 'Dev Sandbox Root Directory' UNION ALL
  SELECT 'AI_REPOSIT_TRACKING', 'FALSE', 'Repository Tracking deactivation flag'
) AS source
ON target.variable_name = source.variable_name
WHEN MATCHED THEN
  UPDATE SET 
    target.variable_value = source.variable_value, 
    target.updated_timestamp = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
  INSERT (variable_name, variable_value, description, updated_timestamp)
  VALUES (source.variable_name, source.variable_value, source.description, CURRENT_TIMESTAMP());

-- Example Procedure showing how to access and declare these variables in BigQuery Scripting
CREATE OR REPLACE PROCEDURE `PROJECT_DATASET.sp_get_etl_environment`(
  OUT out_ETL_Host STRING,
  OUT out_ETL_Projekt STRING,
  OUT out_AI_ENV_SAND_ROOT STRING
)
BEGIN
  -- Declare local session variables mapping to the Shell Script variables
  DECLARE var_AB_HOME STRING;
  DECLARE var_AB_AIR_ROOT STRING;
  DECLARE var_AB_AIR_HOME STRING;
  DECLARE var_ETL_Host STRING;
  DECLARE var_ETL_Projekt STRING;
  DECLARE var_AI_PRIV_SAND_ROOT STRING;
  DECLARE var_AI_ENV_SAND_ROOT STRING;
  DECLARE var_AI_REPOSIT_TRACKING BOOL;

  -- Fetch values dynamically from the configuration table
  SET var_AB_HOME = (SELECT variable_value FROM `PROJECT_DATASET.ETL_ENVIRONMENT_CONFIG` WHERE variable_name = 'AB_HOME');
  SET var_AB_AIR_ROOT = (SELECT variable_value FROM `PROJECT_DATASET.ETL_ENVIRONMENT_CONFIG` WHERE variable_name = 'AB_AIR_ROOT');
  SET var_AB_AIR_HOME = (SELECT variable_value FROM `PROJECT_DATASET.ETL_ENVIRONMENT_CONFIG` WHERE variable_name = 'AB_AIR_HOME');
  SET var_ETL_Host = (SELECT variable_value FROM `PROJECT_DATASET.ETL_ENVIRONMENT_CONFIG` WHERE variable_name = 'ETL_Host');
  SET var_ETL_Projekt = (SELECT variable_value FROM `PROJECT_DATASET.ETL_ENVIRONMENT_CONFIG` WHERE variable_name = 'ETL_Projekt');
  SET var_AI_PRIV_SAND_ROOT = (SELECT variable_value FROM `PROJECT_DATASET.ETL_ENVIRONMENT_CONFIG` WHERE variable_name = 'AI_PRIV_SAND_ROOT');
  SET var_AI_ENV_SAND_ROOT = (SELECT variable_value FROM `PROJECT_DATASET.ETL_ENVIRONMENT_CONFIG` WHERE variable_name = 'AI_ENV_SAND_ROOT');
  SET var_AI_REPOSIT_TRACKING = CAST((SELECT variable_value FROM `PROJECT_DATASET.ETL_ENVIRONMENT_CONFIG` WHERE variable_name = 'AI_REPOSIT_TRACKING') AS BOOL);

  -- Assign outputs for downstream script execution usage
  SET out_ETL_Host = var_ETL_Host;
  SET out_ETL_Projekt = var_ETL_Projekt;
  SET out_AI_ENV_SAND_ROOT = var_AI_ENV_SAND_ROOT;
END;
```

---

### 6.2 Translation of `.dw_db`
```sql
-- =================================================================================
-- BigQuery Scripting Translation of DB Environment Initialization
-- =================================================================================

-- Declare variables to mimic the exported shell environment variables
DECLARE NLS_LANG STRING;
DECLARE DB_CONNECTION_ID STRING;
DECLARE DB_USER STRING;
DECLARE DB_PASSWD_SECRET_PATH STRING;

-- Initialize environment configurations
-- Note: BigQuery operates natively on UTF-8. German locale-specific formatting
-- can be handled dynamically using FORMAT() or FORMAT_DATE() functions.
SET NLS_LANG = 'GERMAN_GERMANY.UTF8';

-- Map the TNS Name to a BigQuery External Connection ID or Project Resource Path
SET DB_CONNECTION_ID = 'projects/gcp-devlab-project/locations/europe-west3/connections/conn-edwh3-devlab';

-- Set DB Username session parameter
SET DB_USER = 'meyreis';

-- Instead of a raw or encrypted password string, reference the Google Cloud Secret Manager resource identifier
SET DB_PASSWD_SECRET_PATH = 'projects/gcp-devlab-project/secrets/meyreis-dwh-password/versions/latest';

-- Logging configuration initialization status (Simulating shell echo/logging)
SELECT 
  CURRENT_TIMESTAMP() AS log_timestamp,
  'SUCCESS' AS status,
  'Database session parameters initialized successfully.' AS message,
  NLS_LANG AS session_locale,
  DB_CONNECTION_ID AS targeted_connection,
  DB_USER AS database_user;
```

```python
# python_pseudocode.py
from google.cloud import secretmanager

def get_database_credentials(secret_id: str) -> str:
    """
    Replaces the shell-based decryption 'm_password' by securely pulling 
    the active password string from GCP Secret Manager.
    """
    try:
        # Create the Secret Manager client.
        client = secretmanager.SecretManagerServiceClient()
        
        # Access the secret version.
        response = client.access_secret_version(request={"name": secret_id})
        
        # Extract the payload and decode.
        password = response.payload.data.decode("UTF-8")
        return password
    except Exception as e:
        raise RuntimeError(f"Failed to retrieve database credentials: {str(e)}")
```

---

### 6.3 Translation of `.dw_global`
```sql
-- Create an Audit/Log table to capture environment check failures
CREATE TABLE IF NOT EXISTS `your_project.your_dataset.dw_environment_log` (
  log_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  log_level STRING,
  message STRING,
  missing_variable STRING
);

-- Stored Procedure to initialize and validate the global DWH environment variables
CREATE OR REPLACE PROCEDURE `your_project.your_dataset.sp_dw_global`(
  -- Input parameters representing the system environment variables
  IN p_DW_DIR_ROOT STRING,
  IN p_DW_DIR_PROT STRING,
  IN p_DW_DIR_CUBES STRING,
  IN p_DW_DIR_IMP_D1 STRING,
  IN p_DW_DIR_IMP_XTRA STRING,
  IN p_DW_DIR_IMP_CTEL STRING,
  IN p_DW_DIR_IMP_VO STRING,
  IN p_DW_DIR_IMP_RV STRING,
  IN p_DW_DIR_IMP_IF STRING,
  IN p_DW_DIR_IMP_NNV STRING,
  IN p_ORACLE_HOME STRING,
  -- Outputs returning standard session settings
  OUT out_NLS_LANG STRING,
  OUT out_NLS_DATE_FORMAT STRING,
  OUT out_NLS_DATE_LANGUAGE STRING,
  OUT out_LANG STRING
)
BEGIN
  -- Declarations for validation
  DECLARE v_fehler ARRAY<STRING>;
  DECLARE v_idx INT64 DEFAULT 0;
  DECLARE v_error_count INT64 DEFAULT 0;
  
  -- Initialize empty array for missing variables
  SET v_fehler = GENERATE_ARRAY(1, 0);

  -- Perform checks analogous to shell script conditional checks
  IF p_DW_DIR_ROOT IS NULL OR p_DW_DIR_ROOT = '' THEN
    SET v_fehler = ARRAY_CONCAT(v_fehler, ['DW_DIR_ROOT']);
  END IF;
  
  IF p_DW_DIR_PROT IS NULL OR p_DW_DIR_PROT = '' THEN
    SET v_fehler = ARRAY_CONCAT(v_fehler, ['DW_DIR_PROT']);
  END IF;

  IF p_DW_DIR_CUBES IS NULL OR p_DW_DIR_CUBES = '' THEN
    SET v_fehler = ARRAY_CONCAT(v_fehler, ['DW_DIR_CUBES']);
  END IF;

  IF p_DW_DIR_IMP_D1 IS NULL OR p_DW_DIR_IMP_D1 = '' THEN
    SET v_fehler = ARRAY_CONCAT(v_fehler, ['DW_DIR_IMP_D1']);
  END IF;

  IF p_DW_DIR_IMP_XTRA IS NULL OR p_DW_DIR_IMP_XTRA = '' THEN
    SET v_fehler = ARRAY_CONCAT(v_fehler, ['DW_DIR_IMP_XTRA']);
  END IF;

  IF p_DW_DIR_IMP_CTEL IS NULL OR p_DW_DIR_IMP_CTEL = '' THEN
    SET v_fehler = ARRAY_CONCAT(v_fehler, ['DW_DIR_IMP_CTEL']);
  END IF;

  IF p_DW_DIR_IMP_VO IS NULL OR p_DW_DIR_IMP_VO = '' THEN
    SET v_fehler = ARRAY_CONCAT(v_fehler, ['DW_DIR_IMP_VO']);
  END IF;

  IF p_DW_DIR_IMP_RV IS NULL OR p_DW_DIR_IMP_RV = '' THEN
    SET v_fehler = ARRAY_CONCAT(v_fehler, ['DW_DIR_IMP_RV']);
  END IF;

  IF p_DW_DIR_IMP_IF IS NULL OR p_DW_DIR_IMP_IF = '' THEN
    SET v_fehler = ARRAY_CONCAT(v_fehler, ['DW_DIR_IMP_IF']);
  END IF;

  IF p_DW_DIR_IMP_NNV IS NULL OR p_DW_DIR_IMP_NNV = '' THEN
    SET v_fehler = ARRAY_CONCAT(v_fehler, ['DW_DIR_IMP_NNV']);
  END IF;

  IF p_ORACLE_HOME IS NULL OR p_ORACLE_HOME = '' THEN
    SET v_fehler = ARRAY_CONCAT(v_fehler, ['ORACLE_HOME']);
  END IF;

  -- Verify if any missing variables were captured
  SET v_error_count = ARRAY_LENGTH(v_fehler);
  
  IF v_error_count > 0 THEN
    -- Loop through the errors and log each missing variable
    WHILE v_idx < v_error_count DO
      INSERT INTO `your_project.your_dataset.dw_environment_log` (log_level, message, missing_variable)
      VALUES ('ERROR', 'Umgebungsvariable ist nicht gesetzt !', v_fehler[OFFSET(v_idx)]);
      
      SET v_idx = v_idx + 1;
    END WHILE;
    
    -- Raise an execution error for missing dependencies
    ERROR CONCAT('Fehler in .dw_global: ', CAST(v_error_count AS STRING), ' required global environment variables are missing.');
  END IF;

  -- Define global/session mappings for downstream activities
  SET out_NLS_LANG = 'GERMAN_GERMANY.WE8ISO8859P1';
  SET out_NLS_DATE_FORMAT = 'DD.MM.YY';
  SET out_NLS_DATE_LANGUAGE = 'GERMAN_GERMANY.WE8ISO8859P1';
  SET out_LANG = 'de';

END;
```

---

### 6.4 Translation of `.dw_init`
```sql
CREATE OR REPLACE PROCEDURE `metadata.dw_init`(
  INOUT io_oracle_home STRING,
  IN io_oracle_sid STRING,
  IN i_home_dir STRING,
  -- Input flags to simulate filesystem checks (-d /appl/local/oracle/...)
  IN i_dir_oracle_12_exists BOOLEAN,
  IN i_dir_oracle_11_exists BOOLEAN
)
BEGIN
  -- Declaring environment path variables
  DECLARE DW_DIR_ROOT STRING;
  DECLARE DW_DIR_PROT STRING;
  DECLARE DW_DIR_CUBES STRING;
  DECLARE DW_DIR_IMP_D1 STRING;
  DECLARE DW_DIR_IMP_BWA STRING;
  DECLARE DW_DIR_IMP_XTRA STRING;
  DECLARE DW_DIR_IMP_CTEL STRING;
  DECLARE DW_DIR_IMP_VO STRING;
  DECLARE DW_DIR_IMP_RV STRING;
  DECLARE DW_DIR_IMP_IF STRING;
  DECLARE DW_DIR_IMP_NNV STRING;
  DECLARE DW_DIR_IMP_SIGMA STRING;
  DECLARE DW_DIR_EXP_SIGMA STRING;
  DECLARE DW_DIR_IMP_TRF STRING;
  DECLARE DW_DIR_IMP_AUF STRING;
  DECLARE DW_DIR_IMP_GUT STRING;
  DECLARE DW_DIR_IMP_KDG STRING;
  DECLARE DW_DIR_IMP_MP_KDG STRING;
  DECLARE DW_DIR_IMP_MP_TS STRING;
  DECLARE DW_DIR_IMP_MP_ZM STRING;
  DECLARE DW_DIR_IMP_TS STRING;
  DECLARE DW_DIR_IMP_ZM STRING;
  DECLARE DW_DIR_EXP STRING;
  DECLARE DW_DIR_IMP_BPM STRING;
  DECLARE DW_DIR_IMP_ZTS STRING;
  DECLARE DW_DIR_IMP_VRS STRING;
  DECLARE DW_DIR_IMP_BRUNET STRING;
  DECLARE DW_DIR_IMP_DWH STRING;
  DECLARE DW_DIR_IMP_PLATO STRING;
  DECLARE DW_DIR_IMP_CARMEN STRING;
  DECLARE DW_DIR_IMP_SAP STRING;
  DECLARE DW_DIR_IMP_SR_RV STRING;
  DECLARE DW_DIR_IMP_SAP_L STRING;
  DECLARE DW_DIR_IMP_L_MAHNSTYP_IST STRING;
  DECLARE DW_DIR_IMP_L_MAHNV_FI STRING;
  DECLARE DW_DIR_IMP_L_MAHNV_IST STRING;
  DECLARE DW_DIR_IMP_L_GUTGR STRING;
  DECLARE DW_DIR_IMP_L_LEIST STRING;
  DECLARE DW_DIR_IMP_L_PROD STRING;
  DECLARE DW_DIR_IMP_LKODE STRING;
  DECLARE DW_DIR_IMP_SUBSE STRING;
  DECLARE DW_DIR_SMS_PRG STRING;
  DECLARE DW_DIR_SMS_ADR STRING;
  DECLARE DW_DIR_SMS_TMP STRING;
  DECLARE DW_DIR_IMP_DPPS STRING;
  DECLARE DW_DIR_IMP_PLANF2 STRING;
  DECLARE DW_HOST_CUSTOMER STRING;
  DECLARE DW_DIR_UTL_FILE STRING;

  -- 1. Initialize environment directory paths
  SET DW_DIR_ROOT = CONCAT(i_home_dir, '/aktuell');
  SET DW_DIR_PROT = CONCAT(i_home_dir, '/daten/logfiles');
  SET DW_DIR_CUBES = CONCAT(i_home_dir, '/daten/cubes');

  SET DW_DIR_IMP_D1 = CONCAT(i_home_dir, '/daten/d1');
  SET DW_DIR_IMP_BWA = CONCAT(i_home_dir, '/daten/dpps/bwa');
  SET DW_DIR_IMP_XTRA = CONCAT(i_home_dir, '/daten/xtra');
  SET DW_DIR_IMP_CTEL = CONCAT(i_home_dir, '/daten/ctel');
  SET DW_DIR_IMP_VO = CONCAT(i_home_dir, '/daten/vo');
  SET DW_DIR_IMP_RV = CONCAT(i_home_dir, '/daten/rv');
  SET DW_DIR_IMP_IF = CONCAT(i_home_dir, '/daten/ees');
  SET DW_DIR_IMP_NNV = CONCAT(i_home_dir, '/daten/nnv');
  SET DW_DIR_IMP_SIGMA = CONCAT(i_home_dir, '/daten/gd/sigma');
  SET DW_DIR_EXP_SIGMA = CONCAT(i_home_dir, '/daten/gd/sigma/export');
  SET DW_DIR_IMP_TRF = CONCAT(i_home_dir, '/daten/trf');
  SET DW_DIR_IMP_AUF = CONCAT(i_home_dir, '/daten/sd/auf');
  SET DW_DIR_IMP_GUT = CONCAT(i_home_dir, '/daten/sd/gut');
  SET DW_DIR_IMP_KDG = CONCAT(i_home_dir, '/daten/sd/kdg');
  SET DW_DIR_IMP_MP_KDG = CONCAT(i_home_dir, '/daten/mp/kdg');
  SET DW_DIR_IMP_MP_TS = CONCAT(i_home_dir, '/daten/mp/ts');
  SET DW_DIR_IMP_MP_ZM = CONCAT(i_home_dir, '/daten/mp/zm');
  SET DW_DIR_IMP_TS = CONCAT(i_home_dir, '/daten/sd/ts');
  SET DW_DIR_IMP_ZM = CONCAT(i_home_dir, '/daten/sd/zm');
  SET DW_DIR_EXP = CONCAT(i_home_dir, '/daten/exporter');
  SET DW_DIR_IMP_BPM = CONCAT(i_home_dir, '/daten/bm');
  SET DW_DIR_IMP_ZTS = CONCAT(i_home_dir, '/daten/zts');
  SET DW_DIR_IMP_VRS = CONCAT(i_home_dir, '/daten/vrs');

  SET DW_DIR_IMP_BRUNET = CONCAT(i_home_dir, '/daten/brunet');
  SET DW_DIR_IMP_DWH = CONCAT(i_home_dir, '/daten/dwh');
  SET DW_DIR_IMP_PLATO = CONCAT(i_home_dir, '/daten/dwh/plato');
  SET DW_DIR_IMP_CARMEN = CONCAT(i_home_dir, '/daten/carmen');
  SET DW_DIR_IMP_SAP = CONCAT(i_home_dir, '/daten/sap');
  SET DW_DIR_IMP_SR_RV = CONCAT(i_home_dir, '/daten/sap/sr_rv_dpps');
  SET DW_DIR_IMP_SAP_L = CONCAT(i_home_dir, '/daten/sap/sap_l_gutgr');
  SET DW_DIR_IMP_L_MAHNSTYP_IST = CONCAT(i_home_dir, '/daten/sap/mahn');
  SET DW_DIR_IMP_L_MAHNV_FI = CONCAT(i_home_dir, '/daten/sap/mahn');
  SET DW_DIR_IMP_L_MAHNV_IST = CONCAT(i_home_dir, '/daten/sap/mahn');
  SET DW_DIR_IMP_L_GUTGR = CONCAT(i_home_dir, '/daten/sd/l_gutschr');
  SET DW_DIR_IMP_L_LEIST = CONCAT(i_home_dir, '/daten/sd/l_leist');
  SET DW_DIR_IMP_L_PROD = CONCAT(i_home_dir, '/daten/sd/l_prod');
  SET DW_DIR_IMP_LKODE = CONCAT(i_home_dir, '/daten/sd/lkode');

  SET DW_DIR_IMP_SUBSE = CONCAT(i_home_dir, '/daten/subse');

  SET DW_DIR_SMS_PRG = CONCAT(i_home_dir, '/aktuell/allgemein/is/util');
  SET DW_DIR_SMS_ADR = CONCAT(i_home_dir, '/daten/sms/adressen');
  SET DW_DIR_SMS_TMP = CONCAT(i_home_dir, '/daten/sms/tmp');

  SET DW_DIR_IMP_DPPS = CONCAT(i_home_dir, '/daten/dpps');
  SET DW_DIR_IMP_PLANF2 = CONCAT(i_home_dir, '/daten/planf2');

  -- Remote Host configuration
  SET DW_HOST_CUSTOMER = 'dxcst3.bn.detemobil.de';

  -- 2. Determine ORACLE_HOME dynamically based on system flags if not already provided
  IF io_oracle_home IS NULL OR io_oracle_home = '' THEN
    IF i_dir_oracle_12_exists THEN
      SET io_oracle_home = '/appl/local/oracle/12.2.0.1.0';
    ELSEIF i_dir_oracle_11_exists THEN
      SET io_oracle_home = '/appl/local/oracle/11.2.0';
    ELSE
      -- Log configuration failure
      SELECT ERROR('Fehler in .dw_init: Konnte ORACLE_HOME nicht setzen !');
    END IF;
  END IF;

  -- 3. Execute Global and Local Configurations (Sourced Scripts)
  -- Note: sp_dw_lokal placeholder is called simulating the sourcing pattern.
  CALL `metadata.sp_dw_global`(
    DW_DIR_ROOT, DW_DIR_PROT, DW_DIR_CUBES, DW_DIR_IMP_D1, DW_DIR_IMP_XTRA, 
    DW_DIR_IMP_CTEL, DW_DIR_IMP_VO, DW_DIR_IMP_RV, DW_DIR_IMP_IF, DW_DIR_IMP_NNV, 
    io_oracle_home, _, _, _, _
  );
  -- CALL `metadata.sp_dw_lokal`();

  -- 4. Calculate final derived variable path
  SET DW_DIR_UTL_FILE = CONCAT('/appl/local/oracle/admin/', io_oracle_sid, '/utl_file');

END;
```