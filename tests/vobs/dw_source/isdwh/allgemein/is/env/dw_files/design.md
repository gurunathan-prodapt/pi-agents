An implementation-ready **MIGRATION DESIGN DOCUMENT** for the assembled job `Shared Files — vobs/dw_source/isdwh/allgemein/is/env/dw_files` is presented below.

---

# MIGRATION DESIGN DOCUMENT

## 1. Executive Summary
* **Seed Name:** Shared Files — vobs/dw_source/isdwh/allgemein/is/env/dw_files
* **Seed Type:** shared_files
* **Source Path:** `/home/gurunathan_t/folder1_uc4_ksh_abinitio/vobs/dw_source/isdwh/allgemein/is/env/dw_files/.dw_ai`
* **Target Platform:** Google Cloud Platform (BigQuery / Cloud Composer / Horizon Python)
* **Pre-Classification Pattern:** `UC4_ONLY` / Cloud Composer (High Confidence).
* **Deviation Note:** Although classified as `UC4_ONLY` at the orchestrator layer, this specific shared file (`.dw_ai`) is a shell script environment setup. Because it configures framework path variables, we have chosen the `shellscript_to_horizon_python_design` tool to generate a Python-compatible representation of these variables. This allows Python-based Airflow DAGs and downstream processes to reference them natively.

---

## 2. File Disposition Table

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `vobs/dw_source/isdwh/allgemein/is/env/dw_files/.dw_ai` | `vobs/dw_source/isdwh/allgemein/is/env/dw_files/dw_ai.py` | Migrates the static Ab Initio environment setup variables into a Python config/execution block to support transition-phase compatibility for downstream processes. |

---

## 3. Folder Integrity Rule compliance
* The target file is generated at `vobs/dw_source/isdwh/allgemein/is/env/dw_files/dw_ai.py`, directly mirroring the legacy directory structure.
* No merging of files from separate folders occurs.

---

## 4. MCP VERBATIM OUTPUT

Below is the verbatim design output produced by the conversion engine for `.dw_ai`:

=== Result for vobs/dw_source/isdwh/allgemein/is/env/dw_files/.dw_ai ===
### Step 1: Shell Script Analysis

1.  **Sources**: No data sources (such as databases, Hive tables, or flat files) are defined or read in this script.
2.  **Processing Flow**: The script simply sets environment variables relating to an Ab Initio installation (`AB_HOME`, `AB_AIR_ROOT`, `AB_AIR_HOME`), updates the system path (`PATH`), and sets Ab Initio Sandbox environment variables (`ETL_Host`, `ETL_Projekt`, `AI_PRIV_SAND_ROOT`, `AI_ENV_SAND_ROOT`). There are no data transformations, loops, conditionals, or execution logic.
3.  **Targets**: No target tables or destination files are specified.
4.  **External Dependencies**: The script defines paths and environments pointing to local Ab Initio directories (`/appl/local/abinitio/...`). These do not exist in a Cloud-native BigQuery/Horizon Python environment.
5.  **Parameters and Variables**:
    *   `AB_HOME` = `/appl/local/abinitio/abinitio`
    *   `AB_AIR_ROOT` = `/appl/local/abinitio/TMD_EME/eme_dev/repo`
    *   `AB_AIR_HOME` = `/appl/local/abinitio/abinitio-V2-14`
    *   `ETL_Host` = `dxcsa4.bn.detemobil.de`
    *   `ETL_Projekt` = `BHB`
    *   `AI_PRIV_SAND_ROOT` = `$HOME/abinitio`
    *   `AI_ENV_SAND_ROOT` = `/appl/local/abinitio/sandboxes/DEV`
6.  **Undeclared Variables**:
    *   `$HOME` is an implicit system environment variable representing the user's home directory.
7.  **Hive Queries**: No Hive queries are present in this script.

---

### Step 2: Decompose the Logic

1.  **Environment Initialization**: The shell script is purely configuration-based, establishing environment paths for a legacy Ab Initio ETL system.
2.  **Horizon Python Equivalent**: Since the destination environment is BigQuery/Horizon, legacy Ab Initio configuration paths are obsolete. However, to preserve operational metadata or equivalents, these environment variables can be represented as a Python dictionary or loaded into Python's `os.environ` if downstream processes depend on them.

---

### Step 3: Map Shell Commands to Horizon Python

1.  **File Handling**: None present.
2.  **Sources and Targets**: None present.
3.  **Transformations**: None present.
4.  **Hive to BQSQL**: None present.

---

### Design Document

#### Shell Script Analysis
The input script is a environment-setup script wrapper written in Korn Shell (`ksh_dwh`). It sets up the system environment variables for executing Ab Initio graphs and tracking repositories. It does not load, modify, or output any data.

#### Assumptions and Additional Notes
*   **Ab Initio Migration**: Ab Initio frameworks are typically migrated to native Python execution or Cloud Orchestration tools (like Google Cloud Composer/Airflow). The environment variables declared in this script (`AB_HOME`, `AI_ENV_SAND_ROOT`, etc.) are legacy-specific and have no functional impact on Google Cloud BigQuery or Horizon.
*   **Variables Preservation**: We will store these variables in a Python dictionary and inject them into `os.environ` to ensure compatibility if any legacy wrapper scripts reference them during the transition phase.

---

### Python Pseudocode

```python
import os
import sys

# Ensure library path for Horizon is included
sys.path.append(os.getenv('DIR_LIB_PY', ''))

# Import the Horizon framework core execution module
from framework.core.lib import script

def main():
    """
    Main migration task to replicate environment configuration in Horizon Python.
    """
    # Define legacy environment variables mapping
    legacy_env_config = {
        "AB_HOME": "/appl/local/abinitio/abinitio",
        "AB_AIR_ROOT": "/appl/local/abinitio/TMD_EME/eme_dev/repo",
        "AB_AIR_HOME": "/appl/local/abinitio/abinitio-V2-14",
        "ETL_Host": "dxcsa4.bn.detemobil.de",
        "ETL_Projekt": "BHB",
        "AI_PRIV_SAND_ROOT": f"{os.getenv('HOME', '/home/user')}/abinitio",
        "AI_ENV_SAND_ROOT": "/appl/local/abinitio/sandboxes/DEV"
    }

    # Set the variables into the current execution environment (os.environ)
    for key, val in legacy_env_config.items():
        os.environ[key] = val
        print(f"[INFO] Configured Environment Variable: {key} = {val}")

    # Append AB_HOME/bin to System PATH
    additional_path = f"{legacy_env_config['AB_HOME']}/bin"
    os.environ["PATH"] = f"{os.environ.get('PATH', '')}:{additional_path}"
    print(f"[INFO] PATH updated with legacy Ab Initio binary path.")

if __name__ == "__main__":
    main()
```

---

## 5. Context, Dependencies, and Scheduling

### Upstream/Downstream Job Interfacing
The environment variables specified in `.dw_ai` are shared components consumed downstream. To maintain architectural consistency in Google Cloud Composer:

* **Downstream Jobs (Consumers):**
  * `DW.DWH_ABPZ_KKM_AIL_AGENT` — *not yet migrated*
  * `r_ai_start` — *not yet migrated*
  * `vobs/dw_source/isdwh/abinitio/bin/r_ai_start` — *not yet migrated*

**Wiring Strategy:**
Since the downstream consumers are "not yet migrated", their ultimate Google Cloud composer orchestration or native execution must load this pythonized `dw_ai.py` helper to populate environment flags. Once those downstream processes are fully migrated to native BigQuery SQL/PySpark, the variables corresponding to local paths (`/appl/local/...`) will become completely obsolete.

### Schedule & Variables — Retention
* **Trigger:** This environment script is loaded ad-hoc or as a setup-task prior to executing dependent wrapper jobs.
* **Variable Transport:** For the GCP target, these configurations should be converted into Airflow Variables or passed as a `dict` via an Airflow DAG's task arguments.

---

## 6. Environment-Specific Values Classification

Per the migration policy, legacy variables are mapped to GCP standards based on their role:

### 1. Global (Environment-Wide Infrastructure)
* **ETL_Host** (`dxcsa4.bn.detemobil.de`) $\rightarrow$ Map to `GCP_PROJECT` or `GCS_BUCKET` endpoint parameters in production, as this host is replaced by Google Cloud Composer/BigQuery execution environments.

### 2. Job-Specific
* **ETL_Projekt** (`BHB`) $\rightarrow$ Sourced at runtime as an Airflow DAG param: `Variable.get("etl_projekt", default_var="BHB")`.
* **AB_HOME**, **AB_AIR_ROOT**, **AB_AIR_HOME**, **AI_PRIV_SAND_ROOT**, **AI_ENV_SAND_ROOT** $\rightarrow$ These local file-system paths are retained inside `dw_ai.py` (via `JOB_CONFIG` variables) exclusively for legacy-compatibility checks in the staging container. If the downstream ETL pipelines are converted entirely to BigQuery SQL, these variables can be safely retired.

---

## 7. Risks & Manual Actions
* **RISK:** Downstream dependencies (`DW.DWH_ABPZ_KKM_AIL_AGENT` and `r_ai_start`) are not yet migrated. The exact target deployment strategy for these components must map back to `dw_ai.py` to ensure environment variables are correctly inherited.
* **MANUAL ACTION:** Once the entire Ab Initio catalog is migrated to PySpark/BigQuery, a code cleanup task must be scheduled to retire this configuration module entirely and use standard Airflow configuration variables.

---

An elegant, implementation-ready migration design for the job `Shared Files — vobs/dw_source/isdwh/allgemein/is/env/dw_files` has been prepared. This job is classified under the `UC4_ONLY` orchestration pattern (Confidence: High) and serves to transition legacy Oracle environment variables (`.dw_db`) into Google Cloud Platform (GCP) configurations.

Below is the complete, verbatim analysis and design provided by the specialized Migration Design Engine, coupled with the critical target architecture mapping, dependencies, scheduling linkages, and file dispositions.

---

### VERBATIM DESIGN ENGINE OUTPUT
```markdown
# Database Environment Migration Design Document
**Oracle `.dw_db` Local Environment File to GCP (BigQuery & Google Secret Manager)**

---

## 1. Objective

### 1.1 Objective of the Migration
The primary objective of this design is to migrate a legacy Oracle local database environment configuration file (`.dw_db`) to a secure, modern, cloud-native architecture on Google Cloud Platform (GCP). The migrated architecture replaces local plain-text/obfuscated files with **Google Secret Manager** for credential rotation and retrieval, and utilizes **BigQuery** as the target enterprise data warehouse.

### 1.2 Problem Statement & Context
Legacy systems often rely on local configurations (such as `.dw_db` shell-sourced files) to define:
*   Oracle System Identifiers (`ORACLE_SID`, `TWO_TASK`)
*   Localization settings (`NLS_LANG`, `NLS_DATE_FORMAT`)
*   Local encrypted or plain-text file credentials (`.passwd` files, obfuscated strings)

This approach poses severe security risks (hardcoded keys, local credential access, lack of audit trails) and lacks scalability. In the target GCP environment, local Oracle database instances are replaced by serverless **BigQuery** datasets. System operations must transition from host-level environment configuration to modern Identity and Access Management (IAM), OAuth 2.0/Service Account structures, and runtime secret retrieval.

---

## 2. Functional Overview

```
+---------------------------------------------------------------------------------+
|                                 LEGACY SYSTEM                                   |
|  +--------------------+      +--------------------+      +--------------------+ |
|  |   .dw_db Config    | ---> |  Oracle Client Env | ---> | Oracle Database DB | |
|  |  (NLS_LANG, SIDs)  |      |   (Sqlplus/Shell)  |      |   (Local Instance) | |
|  +--------------------+      +--------------------+      +--------------------+ |
+---------------------------------------------------------------------------------+
                                        │
                                        ▼ [MIGRATION PATH]
+---------------------------------------------------------------------------------+
|                                   GCP SYSTEM                                    |
|  +--------------------+      +--------------------+      +--------------------+ |
|  | Secret Manager     | ---> | Python ETL Engine  | ---> | BigQuery DW        | |
|  | (API Key, Passwords|      | (Runtime Decrypt & |      | (Dataset & Tables) | |
|  |  Service Account)  |      | BigQuery Client API|      |                    | |
|  +--------------------+      +--------------------+      +--------------------+ |
+---------------------------------------------------------------------------------+
```

### 2.1 Logical Migration Steps
1.  **Secret Retrieval:** The runtime environment authenticates with GCP using Application Default Credentials (ADC) or an assigned IAM service account role.
2.  **Secret Resolution:** The migration execution script queries **Google Secret Manager** API to dynamically fetch sensitive credentials (database passwords, connection parameters).
3.  **Localization/NLS Handling:** Converts legacy Oracle `NLS` configuration parameters into BigQuery-compatible SQL properties, regional dataset configurations, or Python formatting operations (such as converting `NLS_DATE_FORMAT` patterns to Standard ISO 8601 formats).
4.  **Client Initialization:** Establishes a connection to Google BigQuery using the standard Google Cloud Client Library.
5.  **Execution & Write:** Data transformation jobs execute queries directly inside BigQuery, utilizing schema configurations mapped from legacy DDL structures.

### 2.2 Detailed Operation Workflow
*   **Decoupled Authentication:** The system discards hardcoded passwords inside environmental profiles. Instead, the runtime relies on GCP IAM role bindings (`roles/secretmanager.secretAccessor` and `roles/bigquery.dataEditor`).
*   **Config Translation:** 
    *   Legacy `NLS_DATE_FORMAT="YYYY-MM-DD HH24:MI:SS"` transitions to BigQuery standard `DATETIME`/`TIMESTAMP` types which inherently parse standard formats, removing environment-level formatting locks.
    *   `NLS_LANG` transitions to native UTF-8 processing in Google BigQuery.

---

## 3. Inputs and Outputs

### 3.1 Input Mapping
| Parameter / Configuration Key | Legacy Source (`.dw_db`) | Target Cloud Parameter | Format / Type | Purpose |
| :--- | :--- | :--- | :--- | :--- |
| **Database Server Reference** | `ORACLE_SID` / `TWO_TASK` | `GCP_PROJECT_ID` / `DATASET_ID` | String | Target processing scope in BigQuery |
| **NLS Settings** | `NLS_LANG="AMERICAN_AMERICA.AL32UTF8"` | Standard GCP Region / UTF-8 | String | System locale and encoding structure |
| **Date Formatting** | `NLS_DATE_FORMAT="YYYY-MM-DD"` | ISO Standard Parsing | String (Standard) | Output date format for presentation |
| **Credentials** | Hardcoded/Obfuscated files | Secret Manager Resource Path | String URI | GCP Resource reference: `projects/*/secrets/*/versions/*` |

### 3.2 List of Tables Being Created
The target schema created in Google BigQuery consists of the following core data-warehouse administration tracking tables:
1.  **`gcp_migration_audit_log`**: Tracks execution times, statuses, and performance metadata of migration jobs.
2.  **`dw_environment_configurations`**: Holds non-sensitive, environment-level configuration variables and GCP mappings.

### 3.3 Output Specification
*   **BigQuery Integration:** Results are directly committed to specific target tables in Google BigQuery.
*   **Standard Return Codes:** Process returns exit code `0` on successful runtime configuration resolution and database operation, and non-zero on execution failures.

---

## 4. I/O Operations

### 4.1 Cloud Infrastructure Interactions
*   **Google Secret Manager (API v1):** 
    *   **Protocol:** gRPC / HTTPS.
    *   **Query Structure:** Fetch payload of secret resources path `projects/{project_id}/secrets/{secret_name}/versions/latest`.
*   **Google BigQuery API:**
    *   **Protocol:** REST API over HTTPS.
    *   **Operations:** `INSERT`, `MERGE` (Upsert), and structured query executions.

---

## 5. External Dependencies

### 5.1 Libraries and Frameworks
*   `google-cloud-secret-manager` (v2.x+): Python client library for accessing Google Secret Manager API.
*   `google-cloud-bigquery` (v3.x+): Python client library for Google BigQuery execution and data loading.
*   `google-auth` (v2.x+): Handles credentials resolution (Application Default Credentials).

---

## 6. Business Rules Extraction

### 6.1 Extracted Rules and Behaviors
*   **Rule 1: No Plaintext Secrets on Storage.** Plaintext credentials cannot reside in environment files (`.dw_db` / `.env` or system profiles) on virtual machines or workspace directories. Access must use token-based ephemeral requests.
*   **Rule 2: NLS Encoding Standarization.** All legacy systems character sets (e.g., `AL32UTF8`, `WE8MSWIN1252`) are standardly mapped and forced to Standard UTF-8 within BigQuery.
*   **Rule 3: Decoupled Project Scoping.** Environments (Development, QA, Production) must be strictly isolated via different GCP Projects rather than using Oracle Scheme qualifiers (`system.user` vs `system_dev.user`).

---

## 7. Security Considerations

### 7.1 Sensitive Information and Authorization Mechanisms
*   **Least Privilege Access Model:**
    *   Runners (Compute Engine, Cloud Run, GKE, or Airflow/Cloud Composer tasks) must assume a **Service Account** bounded solely to `roles/secretmanager.secretAccessor` on the specific secret and `roles/bigquery.jobUser` on the target project.
*   **Data in Transit & Rest:** 
    *   Secrets in transit are wrapped in TLS 1.3 encryption. Secret payloads are encrypted at rest with Google-managed encryption keys (or customer-managed encryption keys, CMEK).
*   **No local caching:** Secret payloads must reside strictly in execution-context memory (RAM) and must never write to log files or persistent local disks.

---

## 8. Error Handling Strategies

### 8.1 Potential Error Scenarios
*   `SecretVersionResolutionError`: Occurs when Secret Manager is unreachable or the secret path reference is invalid.
*   `GoogleAuthError`: Occurs when application fails to resolve Application Default Credentials.
*   `BigQueryJobError`: Schema mismatch, type conversion errors, or insufficient IAM permissions during query write-backs.

### 8.2 Recommended Error Mitigation
*   **Retry Backoff Policy:** Implement exponential backoff for Secret Manager lookup requests to manage transient API network failures.
*   **Fail-fast Validation:** On initialization, test connection validations immediately before executing complex processing tasks.

---

## 9. Monitoring and Logging

### 9.1 Existing and Target Capabilities
*   **Cloud Logging (Stackdriver):** Every API request made to Secret Manager and BigQuery produces an entry in GCP Cloud Trail / Audit Logs.
*   **Alerting Policies:** Set alerts on IAM access failures (`PermissionDenied`) on Secrets to flag unauthorized reconnaissance attempts.

### 9.2 Enhancements
*   Inject process metrics such as configuration retrieval latency and BigQuery job execution runtimes into custom Cloud Monitoring dashboards.

---

## 10. Abstract Syntax Tree (AST)

The tree below models the structural flow of the modern Python migration client resolving environment parameters from Secret Manager and interfacing with BigQuery.

```
[MigrationProgram]
  │
  ├── [Imports]
  │     ├── google.cloud.secretmanager
  │     └── google.cloud.bigquery
  │
  ├── [ConfigurationClass: EnvConfig]
  │     ├── Properties: project_id, dataset_id, secret_uri
  │     └── Method: get_resolved_secrets()
  │
  ├── [ExecutionEngine]
  │     ├── Step 1: Initialize Google Clients (Secret Manager & BigQuery)
  │     ├── Step 2: Fetch and Parse DB Secret Payload
  │     ├── Step 3: Map Legacy NLS to Target ISO Formats
  │     └── Step 4: Execute BigQuery Transactions / Logs
  │
  └── [Error Handler]
        ├── Catch: DefaultCredentialsError
        ├── Catch: SecretManagerAccessDenied
        └── Catch: BigQueryExecutionException
```

---

## 11. SQL Table Creation Statements

These DDL scripts outline the structural tables created in the Google BigQuery target environment to track dynamic migrations.

### 11.1 Table: `dw_environment_configurations`
```sql
CREATE OR REPLACE TABLE `your_project_id.migration_metadata.dw_environment_configurations` (
  config_id STRING NOT NULL OPTIONS(description="Unique configuration UUID"),
  legacy_sid STRING OPTIONS(description="Legacy Oracle System Identifier mapping"),
  gcp_project_id STRING NOT NULL OPTIONS(description="Target GCP Project Identifier"),
  bq_dataset_id STRING NOT NULL OPTIONS(description="Target BigQuery Dataset Name"),
  nls_lang_mapped STRING NOT NULL DEFAULT 'UTF-8' OPTIONS(description="Character encoding"),
  secret_manager_path STRING NOT NULL OPTIONS(description="Full resource identifier path of Secret Manager secret"),
  updated_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);
```

### 11.2 Table: `gcp_migration_audit_log`
```sql
CREATE OR REPLACE TABLE `your_project_id.migration_metadata.gcp_migration_audit_log` (
  log_id STRING NOT NULL OPTIONS(description="Unique ID of execution run"),
  job_name STRING NOT NULL OPTIONS(description="Name of the migrated ETL job or run"),
  execution_status STRING NOT NULL OPTIONS(description="Status of the task (SUCCESS, FAILED, RUNNING)"),
  records_processed INT64 OPTIONS(description="Total records updated or written during run"),
  error_message STRING OPTIONS(description="Captured exceptions if runtime failed"),
  started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  finished_at TIMESTAMP
);
```

---

## 12. Pseudo-Code

The pseudo-code below represents the programmatic flow of the migration runtime module:

```python
# Pseudo-code for Database Configuration Resolution and BigQuery Initiation

IMPORT google.cloud.secretmanager AS secretmanager
IMPORT google.cloud.bigquery AS bigquery
IMPORT os, sys, json

CLASS MigrationEngine:
    CONSTRUCTOR(project_id, dataset_id, secret_name):
        SET self.project_id = project_id
        SET self.dataset_id = dataset_id
        SET self.secret_name = secret_name
        SET self.secret_client = NEW secretmanager.SecretManagerServiceClient()
        SET self.bq_client = NEW bigquery.Client(project=self.project_id)

    METHOD retrieve_db_credentials():
        TRY:
            # Build GCP Secret Access Resource Path
            secret_path = f"projects/{self.project_id}/secrets/{self.secret_name}/versions/latest"
            
            # Retrieve payload
            response = self.secret_client.access_secret_version(request={"name": secret_path})
            secret_payload = response.payload.data.decode("UTF-8")
            
            # Parse dynamic secret json structure (e.g. host port mappings, API Keys)
            RETURN json.loads(secret_payload)
        EXCEPT AccessDeniedException AS e:
            LOG "CRITICAL: IAM credentials do not permit access to Secret resource: " + e.message
            RAISE e
        EXCEPT Exception AS e:
            LOG "ERROR: Could not resolve configurations: " + e.message
            RAISE e

    METHOD run_bigquery_load(target_table, data_payload):
        TRY:
            # Ensure runtime mappings conform to legacy target NLS representation (e.g., UTF-8 encoding)
            # UTF-8 and ISO configurations are naturally preserved in BQ data types
            table_ref = f"{self.project_id}.{self.dataset_id}.{target_table}"
            
            LOG f"Writing payload to BigQuery table: {table_ref}"
            write_job = self.bq_client.insert_rows_json(table_ref, data_payload)
            
            IF write_job is empty:
                LOG "Data successfully integrated into Google BigQuery."
                RETURN True
            ELSE:
                LOG "Row insertion failed: " + str(write_job)
                RETURN False
        EXCEPT Exception AS e:
            LOG "FATAL: Target database operation failed: " + e.message
            RETURN False

# Application Entrypoint Execution Flow
FUNCTION main():
    # Load foundational non-sensitive system environment profiles
    gcp_project = os.get_env("GCP_PROJECT_ID")
    target_dataset = os.get_env("BQ_DATASET_ID")
    secret_key_ref = "dw-database-access-credentials"

    migration_runtime = NEW MigrationEngine(gcp_project, target_dataset, secret_key_ref)
    
    # Secure runtime environment resolution (Decoupled from local '.dw_db' files)
    credentials = migration_runtime.retrieve_db_credentials()
    
    # Execute loading process
    test_payload = [{"config_id": "c100", "legacy_sid": "DWPROD", "gcp_project_id": gcp_project, "bq_dataset_id": target_dataset, "secret_manager_path": secret_key_ref}]
    success = migration_runtime.run_bigquery_load("dw_environment_configurations", test_payload)
    
    IF success IS True:
        EXIT 0
    ELSE:
        EXIT 1
```
```

---

### MIGRATION & OPERATIONAL CONTEXT

#### 1. Job Dependencies
This shared configuration setup propagates downstream to key execution and data pipeline layers.
* **Downstream Consumers (Cross-Job Hand-offs):**
  * `DW.DWH_ABPZ_KKM_AIL_AGENT` — *not yet migrated*
  * `r_ai_start` — *not yet migrated*
  * `vobs/dw_source/isdwh/abinitio/bin/r_ai_start` — *not yet migrated*
* **Wiring on Target Platform:**
  * Since this file (`.dw_db`) configures global and job-specific environment characteristics, on Google Cloud Platform, these variables are injected into **Cloud Composer (Airflow)** as environmental properties or stored dynamically within **Airflow Variables**.
  * Downstream PySpark and BigQuery ETL DAG processes will retrieve these parameters using a unified initialization helper or tasks sensing state, maintaining synchronous environmental settings.

#### 2. Execution Order & Scheduling
* **Scheduling Linkage:** The orchestration layers do not run `.dw_db` as a standalone scheduled job; it is a structural environment initialization file. The target scheduling mechanism relies on Cloud Composer (Airflow) importing these variables dynamically during runtime initialization.
* **Preserving Variables:**
  * Legacy system variables (`NLS_LANG=GERMAN_GERMANY.WE8ISO8859P1`) are preserved natively inside Airflow environment configuration mapping, or by standardizing Airflow’s connection parameters to handle target localization.

#### 3. Environmental Values
To align with the **Env Variable Policy**, legacy items are mapped as follows:

* **GLOBAL (Environment-Wide Infrastructure Constants):**
  * `DB_TNS_NAME_DWH="@eDWH3.devlab.de.tmo"` $\rightarrow$ Maps to target BigQuery/GCP Project configurations: **`GCP_PROJECT`**, **`GCS_BUCKET`**, and **`BQ_DATASET`**.
  * `NLS_LANG` $\rightarrow$ Standardized globally to standard UTF-8 parsing inside GCP data streams.

* **JOB-SPECIFIC (Target Credentials & Parameters):**
  * `DB_USER_DWH="meyreis"` $\rightarrow$ Configured as a BigQuery target execution Service Account (e.g., `sa-dwh-meyreis@<GCP_PROJECT>.iam.gserviceaccount.com`).
  * `DB_PASSWD_DWH` (encrypted Oracle password) $\rightarrow$ Securely handled by **Google Secret Manager** (`projects/${GCP_PROJECT_ID}/secrets/dw-database-access-credentials/versions/latest`).

#### 4. File Disposition Table
Every file provided in the pre-collected context is fully accounted for:

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `vobs/dw_source/isdwh/allgemein/is/env/dw_files/.dw_db` | `vobs/dw_source/isdwh/allgemein/is/env/dw_files/dw_db.py` (or integrated into Cloud Composer variables) | Replaces local environment and encrypted Oracle logins with GCP Secret Manager credential lookups and BigQuery environmental mappings. |

#### 5. Folder Integrity Rule
* The target environment-resolution module and associated YAML configurations are stored in the mirrored directory: `vobs/dw_source/isdwh/allgemein/is/env/dw_files/` ensuring absolute path structural integrity across the repository.

#### 6. Risks & Manual Actions
* **SOURCE: NOT YET MIGRATED** — `DW.DWH_ABPZ_KKM_AIL_AGENT` — Cross-job downstream mapping cannot be finalized until this job is configured on GCP.
* **SOURCE: NOT YET MIGRATED** — `r_ai_start` — Execution entry point setup must wait for the Ab Initio start script migration.
* **SOURCE: NOT YET MIGRATED** — `vobs/dw_source/isdwh/abinitio/bin/r_ai_start` — Orchestration linkage to PySpark start execution cannot be resolved until this path is migrated.
* **Manual Step:** Ensure the service account running the Cloud Composer DAGs/Airflow tasks is explicitly granted the `Secret Manager Secret Accessor` role for the `dw-database-access-credentials` secret in Google Secret Manager.

---

# MIGRATION DESIGN DOCUMENT

## 1. PRESCRIBED MIGRATION PATTERN & TARGET ARCHITECTURE
* **Prescribed Pattern**: `UC4_ONLY` (High Confidence)
* **Target Architecture**: **Cloud Composer (Airflow)**
* **Approach**: This job is a shell-based environment configuration file (`.dw_global`). Since the prescribed migration target is Cloud Composer, we will migrate these environment-wide variables and connection configurations into Cloud Composer's environment-wide runtime configuration (Airflow Variables and Airflow Connections), and provide a central environment initialization module to replace the legacy `dwh_init` / `.dw_global` pattern.
* **Purpose**: This configuration establishes the shared environment variables, database connection parameter defaults, and global directory markers required for the downstream Ab Initio and shell processes.

---

## 2. FILE DISPOSITION TABLE
Every file in the source workspace for this job is mapped below:

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `vobs/dw_source/isdwh/allgemein/is/env/dw_files/.dw_global` | `dags/allgemein/is/env/dw_files/dw_global_config.py` | Migrates env-validation logic and outputs configured global values using Airflow Variables. |

---

## 3. UNRESOLVED COMPONENTS & RISKS
The pre-collected lineage edges identified references to files that do not exist in the scanned codebase. These are managed as follows:

* **Risks & Manual Actions**:
  * **SOURCE: NOT FOUND — SETPYA.SH — no candidate**
    * *Description*: The legacy code sources `/appl/local/cognos/pya60207/setpya.sh` to configure Cognos PowerPlay variables. This Cognos tool is retired or must be integrated via custom GCP configurations if still active.
  * **SOURCE: NOT FOUND — dwh_init — no candidate**
    * *Description*: The legacy documentation states `.dw_global` is called exclusively by `dwh_init`. The parent initializing script `dwh_init` is missing from the scanned context.
  * **WIRING FOR DOWNSTREAM JOBS NOT YET MIGRATED**:
    * Downstream jobs (`DW.DWH_ABPZ_KKM_AIL_AGENT`, `r_ai_start`, and `vobs/dw_source/isdwh/abinitio/bin/r_ai_start`) are not yet migrated. The connection settings generated here will need to be referenced by their Airflow DAGs once migrated.

---

## 4. VERBATIM MCP REVERSE-ENGINEERED DESIGN

=== Result for vobs/dw_source/isdwh/allgemein/is/env/dw_files/.dw_global ===
Here is the system design document generated from the provided KornShell (ksh) script code.

# DESIGN DOCUMENT: `.dw_global`

---

## 1. Objective
The script `.dw_global` is designed to set and configure global environment variables for a Data Warehouse (DWH) environment based on initial entry points. It validates the presence of essential root directories and Oracle system variables. If valid, it configures national language support (NLS) settings for database connections and sources external environment configurations for third-party BI tools (Cognos PowerPlay).

This script is intended to be called exclusively by `dwh_init`.

---

## 2. Inputs and Outputs

### Inputs
*   **Environment Variables**:
    *   `DW_DIR_ROOT` (String): The root directory path of the DWH.
    *   `DW_DIR_PROT` (String): The protocol/log directory path.
    *   `DW_DIR_CUBES` (String): The directory path for OLAP cubes.
    *   `DW_DIR_IMP_D1` (String): The import directory path for "D1".
    *   `DW_DIR_IMP_XTRA` (String): The import directory path for "XTRA".
    *   `DW_DIR_IMP_CTEL` (String): The import directory path for "CTEL".
    *   `DW_DIR_IMP_VO` (String): The import directory path for "VO".
    *   `DW_DIR_IMP_RV` (String): The import directory path for "RV".
    *   `DW_DIR_IMP_IF` (String): The import directory path for "IF".
    *   `DW_DIR_IMP_NNV` (String): The import directory path for "NNV".
    *   `ORACLE_HOME` (String): The home directory path of the Oracle Database installation.
*   **External Files**:
    *   `/appl/local/cognos/pya60207/setpya.sh` (Shell script): External script evaluated to set Cognos PowerPlay variables.

### Outputs
*   **Environment Variables Exported**:
    *   `NLS_LANG` = `"GERMAN_GERMANY.WE8ISO8859P1"`
    *   `NLS_DATE_FORMAT` = `"DD.MM.YY"`
    *   `NLS_DATE_LANGUAGE` = `"GERMAN_GERMANY.WE8ISO8859P1"`
    *   `PYA_USR` = `""` (Empty string)
    *   `LANG` = `"de"`
*   **Standard Output (stdout)**: Error messages printed to the console if mandatory environment variables are missing.

---

## 3. Dependencies
*   **Shell Interpreter**: KornShell (`/bin/ksh`).
*   **Parent Execution Script**: `dwh_init` (must call this script).
*   **File System Dependency**: Access to file `/appl/local/cognos/pya60207/setpya.sh`.

---

## 4. Error Scenarios
*   **Missing Environment Variables**: If any of the audited input environment variables are unassigned/empty, the script accumulates their names into a list variable `fehler`. It then outputs a validation block listing each unassigned variable:
    ```text
    Fehler in .dw_global:
       Umgebungsvariable <VARIABLE_NAME> ist nicht gesetzt !
    ```
    *Note: The script prints these warnings but does not halt execution or exit with a non-zero code.*

---

## 5. Abstract Syntax Tree (AST)

```text
Program
├── VariableAssignment (fehler = "")
├── Sequence (Environment Validation Checks)
│   ├── IfCondition [Is DW_DIR_ROOT empty?]
│   │   └── True: VariableAssignment (fehler = fehler + " DW_DIR_ROOT ")
│   ├── IfCondition [Is DW_DIR_PROT empty?]
│   │   └── True: VariableAssignment (fehler = fehler + " DW_DIR_PROT ")
│   ├── IfCondition [Is DW_DIR_CUBES empty?]
│   │   └── True: VariableAssignment (fehler = fehler + " DW_DIR_CUBES ")
│   ├── IfCondition [Is DW_DIR_IMP_D1 empty?]
│   │   └── True: VariableAssignment (fehler = fehler + " DW_DIR_IMP_D1 ")
│   ├── IfCondition [Is DW_DIR_IMP_XTRA empty?]
│   │   └── True: VariableAssignment (fehler = fehler + " DW_DIR_IMP_XTRA ")
│   ├── IfCondition [Is DW_DIR_IMP_CTEL empty?]
│   │   └── True: VariableAssignment (fehler = fehler + " DW_DIR_IMP_CTEL ")
│   ├── IfCondition [Is DW_DIR_IMP_VO empty?]
│   │   └── True: VariableAssignment (fehler = fehler + " DW_DIR_IMP_VO ")
│   ├── IfCondition [Is DW_DIR_IMP_RV empty?]
│   │   └── True: VariableAssignment (fehler = fehler + " DW_DIR_IMP_RV ")
│   ├── IfCondition [Is DW_DIR_IMP_IF empty?]
│   │   └── True: VariableAssignment (fehler = fehler + " DW_DIR_IMP_IF ")
│   ├── IfCondition [Is DW_DIR_IMP_NNV empty?]
│   │   └── True: VariableAssignment (fehler = fehler + " DW_DIR_IMP_NNV ")
│   └── IfCondition [Is ORACLE_HOME empty?]
│       └── True: VariableAssignment (fehler = fehler + " ORACLE_HOME ")
├── IfCondition [Is fehler NOT empty?]
│   └── True: 
│       ├── CommandExecution (echo "Fehler in .dw_global:")
│       └── ForLoop (varname in fehler)
│           └── CommandExecution (echo "   Umgebungsvariable varname ist nicht gesetzt !")
├── Sequence (Set DB parameters)
│   ├── VariableAssignment (NLS_LANG = "GERMAN_GERMANY.WE8ISO8859P1") -> Export
│   ├── VariableAssignment (NLS_DATE_FORMAT = "DD.MM.YY") -> Export
│   └── VariableAssignment (NLS_DATE_LANGUAGE = "GERMAN_GERMANY.WE8ISO8859P1") -> Export
├── Sequence (Set Cognos PowerPlay parameters)
│   ├── VariableAssignment (PYA_USR = "") -> Export
│   ├── IfCondition [Does file /appl/local/cognos/pya60207/setpya.sh exist?]
│   │   └── True: SourceCommand (. /appl/local/cognos/pya60207/setpya.sh)
│   └── VariableAssignment (LANG = "de") -> Export
```

---

## 6. Pseudo Code

```text
Initialize fehler as empty string ""

IF DW_DIR_ROOT is empty THEN fehler = fehler + " DW_DIR_ROOT " ENDIF
IF DW_DIR_PROT is empty THEN fehler = fehler + " DW_DIR_PROT " ENDIF
IF DW_DIR_CUBES is empty THEN fehler = fehler + " DW_DIR_CUBES " ENDIF
IF DW_DIR_IMP_D1 is empty THEN fehler = fehler + " DW_DIR_IMP_D1 " ENDIF
IF DW_DIR_IMP_XTRA is empty THEN fehler = fehler + " DW_DIR_IMP_XTRA " ENDIF
IF DW_DIR_IMP_CTEL is empty THEN fehler = fehler + " DW_DIR_IMP_CTEL " ENDIF
IF DW_DIR_IMP_VO is empty THEN fehler = fehler + " DW_DIR_IMP_VO " ENDIF
IF DW_DIR_IMP_RV is empty THEN fehler = fehler + " DW_DIR_IMP_RV " ENDIF
IF DW_DIR_IMP_IF is empty THEN fehler = fehler + " DW_DIR_IMP_IF " ENDIF
IF DW_DIR_IMP_NNV is empty THEN fehler = fehler + " DW_DIR_IMP_NNV " ENDIF
IF ORACLE_HOME is empty THEN fehler = fehler + " ORACLE_HOME " ENDIF

IF fehler is not empty THEN
    PRINT "Fehler in .dw_global:"
    FOR EACH varname IN fehler DO
        PRINT "   Umgebungsvariable " + varname + " ist nicht gesetzt !"
    DONE
ENDIF

SET NLS_LANG = "GERMAN_GERMANY.WE8ISO8859P1"
EXPORT NLS_LANG

SET NLS_DATE_FORMAT = "DD.MM.YY"
EXPORT NLS_DATE_FORMAT

SET NLS_DATE_LANGUAGE = "GERMAN_GERMANY.WE8ISO8859P1"
EXPORT NLS_DATE_LANGUAGE

SET PYA_USR = ""
EXPORT PYA_USR

IF file "/appl/local/cognos/pya60207/setpya.sh" exists THEN
    SOURCE "/appl/local/cognos/pya60207/setpya.sh"
ENDIF

SET LANG = "de"
EXPORT LANG
```

---

## 7. Security Implementations
"Not implemented".

---

## 8. Monitoring Implementations
*   **Standard Error Logging**: Error validation results are output to standard output when environment variables are detected missing during execution. No internal log files, audit systems, or external monitoring system triggers are implemented in the code.

---

## 5. ENVIRONMENT VALUES & CONFIGURATION
Applying the **Environment Variable Policy**:

### GLOBAL VARIABLES (Environment-wide infrastructure configs)
These values are defined at the Airflow environment level using standard GCP/Composer Variable patterns:
* **GCP_PROJECT**: The target GCP project running the BigQuery instance.
* **GCS_BUCKET**: The replacement path for `DW_DIR_ROOT` (migrating from local directories to GCS storage paths).
* **GCP_REGION**: Cloud location (e.g. `europe-west3`).

### JOB-SPECIFIC / CONTEXT-SPECIFIC VARIABLES
The configuration directory inputs must be mapped to their runtime GCS or local directory configurations inside an Airflow variable structure (e.g., loaded from a JSON configuration block `dw_env_config`):

* `DW_DIR_ROOT` (GCS Bucket path fallback: `f"gs://{GCS_BUCKET}/dwh_root"`)
* `DW_DIR_PROT` (GCS path fallback: `f"gs://{GCS_BUCKET}/dwh_prot"`)
* `DW_DIR_CUBES` (GCS path fallback: `f"gs://{GCS_BUCKET}/dwh_cubes"`)
* `DW_DIR_IMP_D1` (GCS path fallback: `f"gs://{GCS_BUCKET}/imports/d1"`)
* `DW_DIR_IMP_XTRA` (GCS path fallback: `f"gs://{GCS_BUCKET}/imports/xtra"`)
* `DW_DIR_IMP_CTEL` (GCS path fallback: `f"gs://{GCS_BUCKET}/imports/ctel"`)
* `DW_DIR_IMP_VO` (GCS path fallback: `f"gs://{GCS_BUCKET}/imports/vo"`)
* `DW_DIR_IMP_RV` (GCS path fallback: `f"gs://{GCS_BUCKET}/imports/rv"`)
* `DW_DIR_IMP_IF` (GCS path fallback: `f"gs://{GCS_BUCKET}/imports/if"`)
* `DW_DIR_IMP_NNV` (GCS path fallback: `f"gs://{GCS_BUCKET}/imports/nnv"`)

---

## 6. TARGET FILE PLAN & TARGET PYTHON MODULE
Below is the python execution logic to be saved at `dags/allgemein/is/env/dw_files/dw_global_config.py`. It implements the verification loop, respects **OUTPUT/PRINT LITERAL RULE** by preserving exact German print messages, and provides a clean dictionary retrieval mechanism for downstream DAG execution.

```python
import os
import logging
from airflow.models import Variable

# OUTPUT/PRINT LITERAL RULE: Verbatim German log prints retained from original .dw_global
def validate_and_get_dw_env():
    # 1. Fetch Global GCP Configurations
    gcs_bucket = Variable.get("GCS_BUCKET", default_var="prod-dw-file-landing")
    
    # Define environment directory mapping
    dw_env = {
        "DW_DIR_ROOT": os.environ.get("DW_DIR_ROOT") or Variable.get("DW_DIR_ROOT", default_var=f"gs://{gcs_bucket}/dwh_root"),
        "DW_DIR_PROT": os.environ.get("DW_DIR_PROT") or Variable.get("DW_DIR_PROT", default_var=f"gs://{gcs_bucket}/dwh_prot"),
        "DW_DIR_CUBES": os.environ.get("DW_DIR_CUBES") or Variable.get("DW_DIR_CUBES", default_var=f"gs://{gcs_bucket}/dwh_cubes"),
        "DW_DIR_IMP_D1": os.environ.get("DW_DIR_IMP_D1") or Variable.get("DW_DIR_IMP_D1", default_var=f"gs://{gcs_bucket}/imports/d1"),
        "DW_DIR_IMP_XTRA": os.environ.get("DW_DIR_IMP_XTRA") or Variable.get("DW_DIR_IMP_XTRA", default_var=f"gs://{gcs_bucket}/imports/xtra"),
        "DW_DIR_IMP_CTEL": os.environ.get("DW_DIR_IMP_CTEL") or Variable.get("DW_DIR_IMP_CTEL", default_var=f"gs://{gcs_bucket}/imports/ctel"),
        "DW_DIR_IMP_VO": os.environ.get("DW_DIR_IMP_VO") or Variable.get("DW_DIR_IMP_VO", default_var=f"gs://{gcs_bucket}/imports/vo"),
        "DW_DIR_IMP_RV": os.environ.get("DW_DIR_IMP_RV") or Variable.get("DW_DIR_IMP_RV", default_var=f"gs://{gcs_bucket}/imports/rv"),
        "DW_DIR_IMP_IF": os.environ.get("DW_DIR_IMP_IF") or Variable.get("DW_DIR_IMP_IF", default_var=f"gs://{gcs_bucket}/imports/if"),
        "DW_DIR_IMP_NNV": os.environ.get("DW_DIR_IMP_NNV") or Variable.get("DW_DIR_IMP_NNV", default_var=f"gs://{gcs_bucket}/imports/nnv"),
        "ORACLE_HOME": os.environ.get("ORACLE_HOME") or Variable.get("ORACLE_HOME", default_var="/opt/oracle/instantclient")
    }

    # Perform Validation Check
    missing_vars = []
    for var_name, var_value in dw_env.items():
        if not var_value:
            missing_vars.append(var_name)

    if missing_vars:
        # EXACT German output formats preserved from source
        print("Fehler in .dw_global:")
        for varname in missing_vars:
            print(f"   Umgebungsvariable {varname} ist nicht gesetzt !")
            
    # Set default execution/session variables analogous to original imports
    global_exports = {
        "NLS_LANG": "GERMAN_GERMANY.WE8ISO8859P1",
        "NLS_DATE_FORMAT": "DD.MM.YY",
        "NLS_DATE_LANGUAGE": "GERMAN_GERMANY.WE8ISO8859P1",
        "PYA_USR": "",
        "LANG": "de"
    }
    
    # Check for legacy script stub (Cognos setpya)
    # SOURCE: NOT FOUND — SETPYA.SH
    pya_script = "/appl/local/cognos/pya60207/setpya.sh"
    if os.path.exists(pya_script):
        # In case the file is mounted in the Airflow environment, keep trace execution
        logging.info(f"Sourcing legacy script {pya_script}")
    else:
        logging.warning("TODO: no source found - /appl/local/cognos/pya60207/setpya.sh")

    dw_env.update(global_exports)
    return dw_env
```

---

Below is the complete, implementation-ready Migration Design Document for the assembled job `Shared Files — vobs/dw_source/isdwh/allgemein/is/env/dw_files`.

***

# MIGRATION DESIGN DOCUMENT
**Job Name:** Shared Files — vobs/dw_source/isdwh/allgemein/is/env/dw_files  
**Target Platform:** Google Cloud Platform (BigQuery / Cloud Composer / GCS)

---

### DE CLASSIFICATION & PATTERN DECISION
*   **Prescribed Pattern:** UC4_ONLY
*   **Target:** Cloud Composer (Apache Airflow) / GCS Configuration Files
*   **Approach:** Establish shared environment variables and database configuration parameters on Cloud Composer/Airflow variables instead of maintaining local shell-based environments.
*   **Deviating from Prescribed Pattern Statement:** Sourcing shell-based properties (`.dw_init`) must be structured into an Airflow environment initialization setup, or made accessible via standard runtime parameters to ensure compatibility with downstream Python or PySpark execution steps.

---

### FILE DISPOSITION TABLE
Every file identified in the pre-collected context is accounted for below. No files from different directories are merged incorrectly.

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `vobs/dw_source/isdwh/allgemein/is/env/dw_files/.dw_init` | `gcs/config/.dw_init.py` (or shared Airflow Variables) | Replicates directory structural logic, variables, and path initialization in Python for Cloud Composer environments. |
| `vobs/dw_source/isdwh/allgemein/is/env/dw_files/.dw_global` | `gcs/config/.dw_global.json` / Variables | Sourced global parameters referenced inside `.dw_init`. |
| `.DW_LOKAL` (UNRESOLVED) | Risk (Stubbed/Referenced) | Unresolved local parameter file; documented under Risks & Manual Actions. |

---

### VERBATIM MCP TOOL MIGRATION DESIGN & TARGET PSEUDOCODE
Below is the output from the migration translation tool mapping the source environment script into a pythonic structure.

```python
#!/usr/bin/env python3
"""
Horizon Python Migration of Environment and Variable Initialization Script.
Targeted for BigQuery/GCP environment execution.
"""

import os
import sys

# Ensure custom framework libraries are accessible
DIR_LIB_PY = os.getenv('DIR_LIB_PY', '')
if DIR_LIB_PY:
    sys.path.append(DIR_LIB_PY)

# Framework Execution Import (Placeholder for potential downstream BQ actions)
try:
    from framework.core.lib import script
except ImportError:
    # Fallback/mock if execution outside of framework environment during testing
    class MockScript:
        def func_execute_bq(self, query, pass_file, col_delim, row_delim):
            pass
    script = MockScript()

def load_sourced_env_file(filepath):
    """
    Simulates sourcing a shell script by parsing key=value exports 
    or running custom logic.
    """
    if os.path.exists(filepath):
        with open(filepath, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    # Remove 'export ' if present
                    if line.startswith('export '):
                        line = line[7:]
                    if '=' in line:
                        key, val = line.split('=', 1)
                        # Clean up quotes and resolve self-references
                        val = val.strip('"\'')
                        if '$HOME' in val:
                            val = val.replace('$HOME', os.getenv('HOME', ''))
                        os.environ[key.strip()] = val.strip()

def main():
    # 1. Base Variables Setup
    home_dir = os.getenv('HOME', '/home/user')
    gcs_bucket = os.getenv('GCS_BUCKET_NAME', 'your_project_id_bucket')
    
    # Establish base cloud directories mapping (using GCS targets or relative local storage)
    env_vars = {
        "DW_DIR_ROOT": f"{home_dir}/aktuell",
        "DW_DIR_PROT": f"{home_dir}/daten/logfiles",
        "DW_DIR_CUBES": f"{home_dir}/daten/cubes",
        "DW_DIR_IMP_D1": f"{home_dir}/daten/d1",
        "DW_DIR_IMP_BWA": f"{home_dir}/daten/dpps/bwa",
        "DW_DIR_IMP_XTRA": f"{home_dir}/daten/xtra",
        "DW_DIR_IMP_CTEL": f"{home_dir}/daten/ctel",
        "DW_DIR_IMP_VO": f"{home_dir}/daten/vo",
        "DW_DIR_IMP_RV": f"{home_dir}/daten/rv",
        "DW_DIR_IMP_IF": f"{home_dir}/daten/ees",
        "DW_DIR_IMP_NNV": f"{home_dir}/daten/nnv",
        "DW_DIR_IMP_SIGMA": f"{home_dir}/daten/gd/sigma",
        "DW_DIR_EXP_SIGMA": f"{home_dir}/daten/gd/sigma/export",
        "DW_DIR_IMP_TRF": f"{home_dir}/daten/trf",
        "DW_DIR_IMP_AUF": f"{home_dir}/daten/sd/auf",
        "DW_DIR_IMP_GUT": f"{home_dir}/daten/sd/gut",
        "DW_DIR_IMP_KDG": f"{home_dir}/daten/sd/kdg",
        "DW_DIR_IMP_MP_KDG": f"{home_dir}/daten/mp/kdg",
        "DW_DIR_IMP_MP_TS": f"{home_dir}/daten/mp/ts",
        "DW_DIR_IMP_MP_ZM": f"{home_dir}/daten/mp/zm",
        "DW_DIR_IMP_TS": f"{home_dir}/daten/sd/ts",
        "DW_DIR_IMP_ZM": f"{home_dir}/daten/sd/zm",
        "DW_DIR_EXP": f"{home_dir}/daten/exporter",
        "DW_DIR_IMP_BPM": f"{home_dir}/daten/bm",
        "DW_DIR_IMP_ZTS": f"{home_dir}/daten/zts",
        "DW_DIR_IMP_VRS": f"{home_dir}/daten/vrs",
        "DW_DIR_IMP_BRUNET": f"{home_dir}/daten/brunet",
        "DW_DIR_IMP_DWH": f"{home_dir}/daten/dwh",
        "DW_DIR_IMP_PLATO": f"{home_dir}/daten/dwh/plato",
        "DW_DIR_IMP_CARMEN": f"{home_dir}/daten/carmen",
        "DW_DIR_IMP_SAP": f"{home_dir}/daten/sap",
        "DW_DIR_IMP_SR_RV": f"{home_dir}/daten/sap/sr_rv_dpps",
        "DW_DIR_IMP_SAP_L_GUTGR": f"{home_dir}/daten/sap/sap_l_gutgr",
        "DW_DIR_IMP_L_MAHNSTYP_IST": f"{home_dir}/daten/sap/mahn",
        "DW_DIR_IMP_L_MAHNV_FI": f"{home_dir}/daten/sap/mahn",
        "DW_DIR_IMP_L_MAHNV_IST": f"{home_dir}/daten/sap/mahn",
        "DW_DIR_IMP_L_GUTGR": f"{home_dir}/daten/sd/l_gutschr",
        "DW_DIR_IMP_L_LEIST": f"{home_dir}/daten/sd/l_leist",
        "DW_DIR_IMP_L_PROD": f"{home_dir}/daten/sd/l_prod",
        "DW_DIR_IMP_LKODE": f"{home_dir}/daten/sd/lkode",
        "DW_DIR_IMP_SUBSE": f"{home_dir}/daten/subse",
        "DW_DIR_SMS_PRG": f"{home_dir}/aktuell/allgemein/is/util",
        "DW_DIR_SMS_ADR": f"{home_dir}/daten/sms/adressen",
        "DW_DIR_SMS_TMP": f"{home_dir}/daten/sms/tmp",
        "DW_DIR_IMP_DPPS": f"{home_dir}/daten/dpps",
        "DW_DIR_IMP_PLANF2": f"{home_dir}/daten/planf2",
        "DW_HOST_CUSTOMER": "dxcst3.bn.detemobil.de"
    }

    # Set cloud path variables to OS environment
    for var, path in env_vars.items():
        os.environ[var] = path

    # 2. Oracle Home Detection Logic
    oracle_home = os.getenv("ORACLE_HOME")
    if not oracle_home:
        path_v1 = "/appl/local/oracle/12.2.0.1.0"
        path_v2 = "/appl/local/oracle/11.2.0"
        
        if os.path.isdir(path_v1):
            os.environ["ORACLE_HOME"] = path_v1
        elif os.path.isdir(path_v2):
            os.environ["ORACLE_HOME"] = path_v2
        else:
            print("Fehler in .dw_init:", file=sys.stderr)
            print("   Konnte ORACLE_HOME nicht setzen !", file=sys.stderr)

    # 3. Sourcing Global and Local Config Scripts
    global_cfg_path = os.path.join(home_dir, ".dw_global")
    local_cfg_path = os.path.join(home_dir, ".dw_lokal")
    
    load_sourced_env_file(global_cfg_path)
    load_sourced_env_file(local_cfg_path)

    # 4. Finalizing Environment Properties
    oracle_sid = os.getenv("ORACLE_SID", "DEFAULT_SID")
    os.environ["DW_DIR_UTL_FILE"] = f"/appl/local/oracle/admin/{oracle_sid}/utl_file"

if __name__ == "__main__":
    main()
```

---

### JOB DEPENDENCIES, SCHEDULING, & LINEAGE

#### Job Dependencies
*   **Upstream:** None explicitly discovered in scheduling boundaries, but reads config files `.dw_global` and `.dw_lokal`.
*   **Downstream (Consume outputs):**
    *   `DW.DWH_ABPZ_KKM_AIL_AGENT` — *Not yet migrated* (Wiring cannot be finalized until this job is created).
    *   `r_ai_start` — *Not yet migrated* (Wiring cannot be finalized).
    *   `vobs/dw_source/isdwh/abinitio/bin/r_ai_start` — *Not yet migrated* (Wiring cannot be finalized).

#### Execution Order & Scheduling
*   **Execution Sequence:** `.dw_init` must be sourced or processed as an initialization step *prior* to launching any associated Python scripts, PySpark actions, or Ab Initio pipeline scripts (e.g., `r_ai_start`).
*   **Scheduling:** This environment configuration job operates on-demand via direct inclusion/sourcing inside subsequent executables. In BigQuery/Composer, it is mapped to a shared Airflow variable configuration load task or imported dynamically within downstream task wrappers.

---

### ENVIRONMENT-SPECIFIC VALUES

Following the strict environment policy, here is the classification of variables used in `.dw_init`:

#### 1. GLOBAL (Environment-Wide Infrastructure Constants)
*   **GCP_PROJECT** — Sourced dynamically inside Airflow or runtime: `os.environ.get("GCP_PROJECT")`
*   **GCS_BUCKET** — Sourced via Airflow config/variables: `Variable.get("GCS_BUCKET")`

#### 2. JOB-SPECIFIC (Unique to this workload environment mapping)
*   **DW_HOST_CUSTOMER** — Host endpoint value: `"dxcst3.bn.detemobil.de"`
*   **ORACLE_SID** — Set dynamically based on targeted database configuration parameters in Airflow.
*   **DW_DIR_*** directories — Formulated dynamically relative to Cloud Composer local workspaces or targeted logical paths in Google Cloud Storage (`gs://{gcs_bucket}/...`).

---

### RISKS & MANUAL ACTIONS
The following manual steps and gaps must be resolved prior to deploying this job:

1.  **SOURCE: NOT FOUND** — `.dw_lokal` — No source file found in codebase. An equivalent representation must be manually recreated as a JSON/YAML parameter store or Airflow Variable.
2.  **Downstream Wiring Risk** — Downstream components (`DW.DWH_ABPZ_KKM_AIL_AGENT`, `r_ai_start`) are not yet migrated. The exact method of passing these environment parameters must be aligned when those systems are ported.
3.  **Oracle Client Paths Validation** — The local Oracle paths `/appl/local/oracle/*` do not exist in GCP serverless environments. If database connectivity requires native client drivers, they must be bundled within the custom Airflow worker container image (Cloud Composer), or replaced by modern JDBC/ODBC connection hooks.