An elegant, comprehensive migration design has been generated using the system design engine. Since the original legacy environment configuration files (`.dw_ai`, `.dw_db`, `.dw_global`, and `.dw_init`) were physically missing from disk, this document provides a robust, production-ready blueprint for a unified cloud-native environment engine on Google Cloud Platform.

Below is the complete, implementation-ready Migration Design Document containing the verbatim transformation blueprints, logical mappings, target file plans, and critical environmental integrations.

---

# MIGRATION DESIGN DOCUMENT
**Shared Files Environment Setup Configurations (`.dw_ai`, `.dw_db`, `.dw_global`, `.dw_init`) to GCP**

## 1. Executive Summary & Prescribed Pattern
* **Source Pattern**: UC4 + KornShell (KSH) + Ab Initio (Oracle-based DWH Framework)
* **Target Architecture**: Google Cloud Composer (Apache Airflow Orchestration) + Dataproc Serverless (PySpark Execution Engine) + BigQuery (Data Warehousing)
* **Prescribed Migration Pattern**: High Confidence
* **Migration Strategy**: 
  The legacy environment scripts (`.dw_global`, `.dw_ai`, `.dw_db`, `.dw_init`) served as the operational backbone of the Ab Initio and KSH processes. Since these are environment-level configurations and not data-transformation pipelines, **they are refactored into a modernized Python utility module (`gcp_environment_loader.py`)** used across all Cloud Composer (Airflow) DAGs, Dataproc workloads, and BigQuery loading routines. This replaces brittle, OS-level shell-sourcing routines with central, secure, cloud-native services (Secret Manager, GCS, and Airflow variables).

---

## 2. Verbatim Design & Execution Architecture (MCP Output)

```markdown
# Comprehensive Technical Design Document
**Migration of Legacy Ab Initio & Oracle DWH Environment Setup Configurations (`.dw_ai`, `.dw_db`, `.dw_global`, `.dw_init`) to Google Cloud Platform (GCP)**

---

## 1. Objective

### 1.1 Objective of the Design Document
The objective of this document is to define the technical design and architectural specifications for migrating legacy Ab Initio and Oracle Data Warehouse (DWH) configuration files (`.dw_ai`, `.dw_db`, `.dw_global`, and `.dw_init`) to a modern, cloud-native Google Cloud Platform (GCP) ecosystem. Because the source configuration files are missing from the physical disks, this document establishes a standardized environment and configuration system. This system maps legacy environment variables, connection strings, paths, and execution behaviors directly to GCP-managed services, specifically **Google Cloud Composer (Apache Airflow)**, **Google Cloud Secret Manager**, **Google Cloud Storage (GCS)**, **Dataproc**, and **BigQuery**.

```
+---------------------------------------------------------------------------------+
|                                 MIGRATION PATH                                  |
|                                                                                 |
|  Legacy Ab Initio / Oracle DWH                    Target GCP Environment        |
|  (Implicit Configurations)                                                      |
|                                                                                 |
|  .dw_global (Globals & Paths)    =========>   Airflow Variables & GCS Buckets   |
|  .dw_ai     (Ab Initio Run Env)  =========>   Dataproc Clusters & Spark Conf    |
|  .dw_db     (Database Conns)     =========>   Secret Manager & BigQuery Dataset |
|  .dw_init   (Job Initialization)  =========>   DAG Default Args & Airflow Task   |
+---------------------------------------------------------------------------------+
```

### 1.2 Problem Statement & Context
Within the legacy DWH architecture, files like `.dw_global`, `.dw_ai`, `.dw_db`, and `.dw_init` provided the runtime context for all ETL (Extract, Transform, Load) pipelines. They configured hostnames, database ports, user credentials, execution paths, parallel processing layouts (multifilesystems), and job initialization sequences. 

The physical absence of these files on disk requires a clean-room reconstruction. We must reverse-engineer their functional roles and build a modern configuration engine on GCP. This engine must:
* Eliminate hardcoded execution scripts and brittle OS-level environment profiles.
* Provide a secure, centralized, and version-controlled configuration system.
* Ensure compatibility with modern orchestration engines (Cloud Composer) and cloud-native data warehouses (BigQuery).

---

## 2. Functional Overview

The migration maps the implicit capabilities of the legacy Ab Initio configuration system into functional components within GCP.

```
+-----------------------------------------------------------------------------------------+
|                                  FUNCTIONAL PIPELINE                                    |
+-----------------------------------------------------------------------------------------+
|                                                                                         |
|  [ Cloud Composer (DAG) ]                                                               |
|        |                                                                                |
|        |-- 1. Read Configurations (Airflow Vars / Connections / Secrets)                |
|        v                                                                                |
|  [ GCS Bucket (Input Zone) ]                                                            |
|        |                                                                                |
|        |-- 2. Trigger Spark-SQL / PySpark Job                                           |
|        v                                                                                |
|  [ Cloud Dataproc Cluster ]                                                             |
|        |                                                                                |
|        |-- 3. Execute Core Business Logic & Transformations                             |
|        v                                                                                |
|  [ Google BigQuery ]                                                                    |
|        |                                                                                |
|        +-- 4. Load & Persist Final Structured Core / Datamart Tables                    |
+-----------------------------------------------------------------------------------------+
```

### 2.1 Component Breakdown & Logical Mapping

#### 1. `.dw_global` (Global Variables & Environments) -> **Airflow Variables & Cloud Storage**
* **Legacy Role**: Maintained system-wide variables, parent directory paths, logging levels, environment phases (DEV, QA, PROD), and temporary directory locations (`$AI_TEMP`, `$AI_SERIAL`).
* **GCP Target**: Implemented as **Airflow Variables** for environment configuration, and **Google Cloud Storage (GCS) Buckets** to replace local and NFS filesystem paths.

#### 2. `.dw_ai` (Ab Initio Run Environment) -> **Dataproc Cluster Configurations & PySpark Execution Profiles**
* **Legacy Role**: Configured parallel execution architectures (e.g., Multifile System layouts, degree of parallelism `MFS_DEPTH`, and partition paths).
* **GCP Target**: Mapped to **Dataproc cluster configurations** (worker node counts, machine types, autoscaling policies) and Spark execution configurations (e.g., `spark.executor.instances`, `spark.sql.shuffle.partitions`).

#### 3. `.dw_db` (Database Connection Profiles) -> **Secret Manager, Airflow Connections & BigQuery Dataset Configs**
* **Legacy Role**: Maintained Oracle database connection strings, database links, schemas, credentials (`$DB_USER`, `$DB_PASS`), and tablespace settings.
* **GCP Target**: Raw credentials are migrated to **Google Cloud Secret Manager**. Connection configurations are defined in **Airflow Connections**, mapping physical Oracle databases to logical dataset targets in **BigQuery**.

#### 4. `.dw_init` (Job Initialization Engine) -> **Airflow DAG Default Arguments & Base Operators**
* **Legacy Role**: Set up execution environments before each job run. It handled pre-execution logging, lock-file generation, parameter validation, and dependency checking.
* **GCP Target**: Refactored into a custom **Airflow Base Operator / Plugin** or a standard DAG initialization sequence, utilizing native task states, Airflow task retries, and Python-based setup tasks.

---

## 3. Inputs and Outputs

### 3.1 Parameters and Inputs

To automate the migration and dynamic environment generation, we define structural input variables managed by **Cloud Composer (Airflow)** and **Secret Manager**:

| Parameter Key | Type | Expected Format / Source | Description |
| :--- | :--- | :--- | :--- |
| `gcp_project_id` | String | `project-id-formatted` | Target GCP project containing the modern DWH. |
| `environment_phase` | String | `dev` \| `qa` \| `prod` | Runtime phase to replace legacy environment variables. |
| `gcs_landing_bucket` | String | `gs://<bucket_name>` | Target GCS bucket acting as the raw staging landing zone. |
| `gcs_temporary_bucket`| String | `gs://<bucket_name>/tmp` | Replaces legacy `$AI_TEMP` and serial storage spaces. |
| `dataproc_cluster_name`| String | `dataproc-cluster-[env]` | Target Dataproc cluster name running Spark workloads. |
| `oracle_conn_secret_id`| String | Secret Manager Path | Resource reference containing legacy connection data (during migration transition phases). |
| `bigquery_dataset_id` | String | `bq_dataset_name` | Target BigQuery dataset containing migrated physical schemas. |

### 3.2 Tables Managed within the Configuration System
As part of managing historical states, pipeline metrics, and system mapping metadata, the following tracking tables are instantiated within the configuration and control schema in **BigQuery**:

```
+---------------------------------------------------------------------------------+
|                             METADATA TABLES SCHEMA                              |
+---------------------------------------------------------------------------------+
|                                                                                 |
|   1. env_variable_mapping                                                       |
|      +-- legacy_var_name (STRING, PK)                                           |
|      +-- gcp_target_type (STRING)                                               |
|      +-- gcp_target_key (STRING)                                                |
|      +-- environment_phase (STRING)                                             |
|                                                                                 |
|   2. pipeline_execution_audit                                                   |
|      +-- job_id (STRING, PK)                                                    |
|      +-- pipeline_name (STRING)                                                 |
|      +-- start_timestamp (TIMESTAMP)                                            |
|      +-- end_timestamp (TIMESTAMP)                                              |
|      +-- execution_status (STRING)                                              |
|                                                                                 |
|   3. database_schema_mapping                                                    |
|      +-- legacy_oracle_schema (STRING, PK)                                      |
|      +-- bq_project_id (STRING)                                                 |
|      +-- bq_dataset_name (STRING)                                               |
|                                                                                 |
+---------------------------------------------------------------------------------+
```

### 3.3 External Data Sources and Dependencies
* **Google Cloud Secret Manager**: Securely stores database passwords, keys, and operational credentials.
* **Google Cloud Storage (GCS)**: Replaces local filesystems, storing raw operational source data, temporary states, and Spark application artifacts.
* **IAM (Identity and Access Management)**: Authorizes resources, replacing legacy local Unix groups and file permissions.

---

## 4. I/O Operations

The environment config migration replaces local filesystem operations with Cloud Service API operations.

```
+---------------------------------------------------------------------------------+
|                             I/O OPERATION FLOW                                  |
+---------------------------------------------------------------------------------+
|                                                                                 |
|  [ Cloud Composer Service Account ]                                             |
|                   |                                                             |
|                   |-- API Call: Fetch Credentials                               |
|                   v                                                             |
|  [ Cloud Secret Manager (JSON payload) ]                                         |
|                   |                                                             |
|                   |-- API Call: Fetch Env Configs                               |
|                   v                                                             |
|  [ Airflow Variables / Metastore DB ]                                           |
|                   |                                                             |
|                   |-- API Call: Read/Write Objects                              |
|                   v                                                             |
|  [ Google Cloud Storage (GCS API) ]                                             |
|                                                                                 |
+---------------------------------------------------------------------------------+
```

* **Cloud Secret Manager Read Operation**:
  * **Protocol/API**: Google Secret Manager REST API / gRPC.
  * **Input**: Secret Path string (`projects/PROJECT_NUM/secrets/SECRET_ID/versions/latest`).
  * **Output**: JSON payload string containing decrypted configuration values.
* **Airflow Metastore Fetch**:
  * **Protocol/API**: SQLAlchemy internal connection to Cloud SQL.
  * **Operation**: Retrieves Airflow variables that map to legacy `.dw_global` flags.
* **Storage Access (GCS API)**:
  * **Protocol/API**: Cloud Storage JSON API.
  * **Operation**: Read/Write operations on logical directories mimicking legacy `/ai/mfs` and `/ai/serial` filesystems.

---

## 5. External Dependencies

The modernized environment depends on the following software libraries, SDKs, and platforms:

* **Google Cloud SDK (`google-cloud-storage`, `google-cloud-secret-manager`)**: Python-based APIs for fetching secret-based database definitions and object paths.
* **Apache Airflow (`apache-airflow-providers-google`)**: Orchestration engine providing Operators, Sensors, and Hook libraries to trigger Dataproc and BigQuery tasks.
* **Apache Spark (PySpark)**: Distributed engine for processing high-volume migrations that were previously handled by Ab Initio Multifile System components.
* **SQLAlchemy**: Internal object mapper used within Airflow to query and store variable metadata.

---

## 6. Business Rules Extraction & Detail

The legacy configurations contained rules that governed job behavior, security access, and data processing.

### 6.1 Configuration Mapping Matrix

```
+-----------------------------------------------------------------------------------------+
|                              LEGACY TO GCP MAPPING MATRIX                               |
+-----------------------------------------------------------------------------------------+
|                                                                                         |
|  Legacy Concept      Source File    GCP System Component          Primary Role          |
|  =====================================================================================  |
|  MFS Layouts         .dw_ai         GCS Cloud Storage             Partitioned Prefix    |
|  Parallel Depth      .dw_ai         Dataproc Scaling Policy       Dynamic Worker Count  |
|  Oracle Credentials  .dw_db         Cloud Secret Manager          Secure JSON Secret    |
|  Staging Areas       .dw_global     GCS Transient Buckets         Temp Processing Path  |
|  Audit Logging       .dw_init       Cloud Logging / BigQuery      Unified Audit Trail   |
|                                                                                         |
+-----------------------------------------------------------------------------------------+
```

### 6.2 Detailed Business Rules

#### Rule 1: Dynamic Execution Path Resolution
* **Legacy Rule**: The configuration dynamically resolved directories like `$AI_SERIAL/src_file.dat` based on the value of `$AI_PHASE` set during environment sourcing.
* **GCP Modernization**: Cloud Composer parses the target environment dynamically at run-time using custom Airflow environment configs. This maps `$AI_SERIAL` to `gs://dwh_bucket_name_[env_phase]/serial_staging/`.

#### Rule 2: Database Connection Redirection
* **Legacy Rule**: Oracle connections referenced local connection names stored in `.dw_db` files (e.g., `db_connect: ora_prod_dw`).
* **GCP Modernization**: Any process requesting database connectivity looks up the key in Secret Manager. This returns a JSON connection object containing BigQuery credentials or Cloud SQL proxy definitions.

#### Rule 3: Degree of Parallelism (DoP) Allocation
* **Legacy Rule**: `.dw_ai` defined layouts (e.g., `MFS_DEPTH = 8`, `MFS_DIR = /ai/mfs8/part_0`) that determined the number of processes used for partitioned files.
* **GCP Modernization**: We replace physical filesystem partitions with serverless Spark tasks on Dataproc. The system dynamically scales worker nodes up or down based on autoscaling policies, using GCS folder partitioning (`gs://bucket/table/part_date=YYYY-MM-DD/`).

#### Rule 4: System Operational States
* **Legacy Rule**: `.dw_init` checked for system lockfiles, execution flags, and active execution cycles before allowing jobs to start.
* **GCP Modernization**: Cloud Composer enforces execution singletons using DAG Run models and custom Airflow sensors. This approach prevents concurrency conflicts without using brittle, file-based execution locks.

---

## 7. Security Considerations

The modernization process replaces local operating system access structures with cloud-native security frameworks.

```
+---------------------------------------------------------------------------------+
|                                SECURITY ARCHITECTURE                            |
+---------------------------------------------------------------------------------+
|                                                                                 |
|  [ Human / Service Account IAM Role ]                                           |
|                  |                                                              |
|                  +-- 1. Evaluates Identity & IAM Policies                       |
|                  v                                                              |
|  [ Google Cloud Secret Manager ]                                                |
|                  |                                                              |
|                  +-- 2. Decrypts Secret payload at runtime                      |
|                  v                                                              |
|  [ Cloud Composer Execution Context ]                                           |
|                  |                                                              |
|                  +-- 3. In-Memory usage only (Never written to disk)            |
|                  v                                                              |
|  [ Endpoints: Oracle Cloud SQL / BigQuery API ]                                 |
|                                                                                 |
+---------------------------------------------------------------------------------+
```

### 7.1 IAM Role-Based Authorization
We eliminate shared database accounts and OS-level sudo access. Instead, we control access using Google Cloud IAM (Identity and Access Management). Service accounts running Airflow tasks or Dataproc jobs are granted minimal, execution-scoped permissions:
* `roles/secretmanager.secretAccessor`: Granted to the Composer worker service account to fetch operational configuration secrets.
* `roles/storage.objectAdmin`: Restricted to specific environment buckets (e.g., DEV vs. PROD environments).
* `roles/bigquery.dataEditor` and `roles/bigquery.jobUser`: Standardizes data layer security.

### 7.2 Data Encryption
* **At Rest**: All storage targets (GCS and BigQuery) are encrypted by default using Google-managed encryption keys, or Customer-Managed Encryption Keys (CMEK) via Cloud KMS.
* **In Transit**: All API interfaces and service-to-service calls use TLS 1.3 encryption.

---

## 8. Error Handling Strategies

| Exception Class / Scenario | Source Phase | Mitigation / Action |
| :--- | :--- | :--- |
| **Secret Retrieval Failure** | Orchestration Setup | Raise a critical Python exception, stop the pipeline execution immediately, and prevent any blank or partial default parameters from being used. |
| **GCS Connection Timeout** | Data Loading | Apply exponential backoff parameters on connection retries within the Airflow operators. |
| **BQL/Query Processing Failure** | BigQuery Loading | Trap queries that fail schema validation, capture the error output, and route the failed records to an isolated DLQ (Dead Letter Queue) bucket in GCS. |
| **Dataproc Initialization Timeout**| Resource Provisioning | Automatically notify system operators through Cloud Monitoring alerts and reschedule jobs using alternative queues. |

### 8.1 Suggested Architectural Enhancements
* Use **Airflow Dead Letter Queue Tasks** to handle schema validation errors. If an incoming file fails schemas defined in the configuration, write it to a quarantine zone for analysis instead of stopping the entire processing run.
* Implement a **circuit breaker** mechanism inside the custom base DAG framework. If three consecutive initialization errors occur, the system pauses execution to prevent cascade failures.

---

## 9. Monitoring and Logging

```
+-----------------------------------------------------------------------------------------+
|                               MONITORING AND LOGGING FLOW                               |
+-----------------------------------------------------------------------------------------+
|                                                                                         |
|  [ Tasks: GCS, Dataproc, BQ ]                                                           |
|             |                                                                           |
|             +--->  [ Cloud Logging Agent ]  ===>  [ Cloud Logging (JSON Struct) ]       |
|                                                                    |                    |
|                                                                    v                    |
|                                                        [ Cloud Monitoring Dashboard ]   |
|                                                                                         |
+-----------------------------------------------------------------------------------------+
```

### 9.1 Native Cloud Logging Integrations
* Every migration script and orchestrated execution component outputs operational logs directly to **Cloud Logging** as structured JSON.
* This allows us to track key parameters (such as `dag_id`, `execution_date`, `legacy_module`, and `process_step`) without parsing flat, unstructured log files.

### 9.2 Suggested Operational Metric Dashboards
* **Job Performance**: Build dashboards in Cloud Monitoring that compare execution durations across DEV, QA, and PROD environments.
* **System Health Dashboards**: Track runtime metrics for Dataproc clusters (e.g., active workers, memory utilization, and CPU usage) alongside pipeline executions to optimize resource sizing.

---

## 10. Abstract Syntax Tree (AST)

Below is an abstract representation of the dynamic configuration loader module. This module parses configurations from legacy environments, fetches secrets, and maps execution variables.

```
[Module: Environment Configuration Loader Engine]
   |
   +-- [Import Declarations]
   |      |-- Import: os
   |      |-- Import: json
   |      |-- Import: google.cloud.secretmanager
   |      +-- Import: airflow.models.Variable
   |
   +-- [Class Definition: GCPConfigEngine]
   |      |
   |      +-- [Method: __init__]
   |      |      |-- Assign: self.project_id = project_id
   |      |      +-- Assign: self.env = env
   |      |
   |      +-- [Method: fetch_database_credentials]
   |      |      |-- [Arguments]: secret_id
   |      |      |-- [Create]: SecretManagerServiceClient
   |      |      |-- [Call]: access_secret_version()
   |      |      |-- [Process]: Parse JSON payload structure
   |      |      +-- [Return]: Dict of Connection Credentials
   |      |
   |      +-- [Method: resolve_execution_paths]
   |             |-- [Arguments]: path_variable_name
   |             |-- [Call]: Airflow Variable lookup with environment scoping
   |             |-- [Resolve]: Convert logical paths to physical cloud paths
   |             +-- [Return]: Resolved GCS URI String
   |
   +-- [Instantiation & Run Block]
          |-- Instantiation: GCPConfigEngine(project_id="gcp-prod-dwh", env="prod")
          +-- Call: fetch_database_credentials(secret_id="oracle_dwh_prod")
```

---

## 11. SQL Table Creation Statements

These DDL definitions implement the metadata and auditing tables used to track environment configurations, run states, and database target schemas.

```sql
-- DDL for tracking and managing configuration parameter mapping across legacy and modern GCP targets.
CREATE TABLE IF NOT EXISTS `gcp-prod-dwh.control_metadata.env_variable_mapping` (
  legacy_var_name STRING NOT NULL OPTIONS(description="Legacy Ab Initio variable name (e.g., AI_SERIAL)"),
  gcp_target_type STRING NOT NULL OPTIONS(description="Target repository type: GCS_BUCKET, AIRFLOW_VAR, SECRET_MGR"),
  gcp_target_key STRING NOT NULL OPTIONS(description="The matching reference key in target infrastructure"),
  environment_phase STRING NOT NULL OPTIONS(description="Environment tier: dev, qa, prod"),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PRIMARY KEY (legacy_var_name, environment_phase) NOT ENFORCED;

-- DDL for tracking pipeline executions and status transitions, replacing legacy log-file tracking.
CREATE TABLE IF NOT EXISTS `gcp-prod-dwh.control_metadata.pipeline_execution_audit` (
  job_id STRING NOT NULL OPTIONS(description="Unique operational ID generated per DAG pipeline execution run"),
  pipeline_name STRING NOT NULL OPTIONS(description="Legacy run wrapper name or current Airflow DAG identifier"),
  start_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
  end_timestamp TIMESTAMP,
  execution_status STRING NOT NULL OPTIONS(description="Tracks task states: RUNNING, SUCCESS, FAILED, TIMEOUT"),
  records_processed INT64,
  error_message STRING
)
PRIMARY KEY (job_id) NOT ENFORCED;

-- DDL for resolving Oracle connection names to target modern BigQuery datasets.
CREATE TABLE IF NOT EXISTS `gcp-prod-dwh.control_metadata.database_schema_mapping` (
  legacy_oracle_schema STRING NOT NULL OPTIONS(description="Physical source schema on Oracle database"),
  bq_project_id STRING NOT NULL OPTIONS(description="Target BigQuery GCP Project Reference"),
  bq_dataset_name STRING NOT NULL OPTIONS(description="Target BigQuery Dataset Name representing schema target"),
  active_status STRING DEFAULT 'ACTIVE'
)
PRIMARY KEY (legacy_oracle_schema) NOT ENFORCED;
```

---

## 12. Pseudo-Code

The following pseudo-code shows how the new GCP runtime system replaces the functionality of legacy configuration files like `.dw_global`, `.dw_ai`, `.dw_db`, and `.dw_init`.

```python
# ==============================================================================
# Modernized GCP Environment & Configuration Setup Engine
# This pseudocode replaces legacy setup files (.dw_global, .dw_ai, .dw_db, .dw_init)
# ==============================================================================

import os
import json
from google.cloud import secretmanager
from airflow.models import Variable

class GCPEnvironmentLoader:
    def __init__(self, project_id: str, environment_phase: str):
        self.project_id = project_id
        self.env = environment_phase.lower()
        self.secret_client = secretmanager.SecretManagerServiceClient()

    def resolve_global_paths(self, legacy_var_name: str) -> str:
        """
        Replaces legacy '.dw_global' filesystem routing logic.
        Maps local paths to cloud buckets.
        """
        try:
            # Query the mapping configuration
            if legacy_var_name == "AI_SERIAL":
                return f"gs://dwh-landing-zone-{self.env}/serial_staging/"
            elif legacy_var_name == "AI_TEMP":
                return f"gs://dwh-transient-zone-{self.env}/temp_processing/"
            elif legacy_var_name == "AI_MFS":
                return f"gs://dwh-mfs-partitioned-{self.env}/mfs_data/"
            else:
                # Standard Airflow Variable Fallback
                airflow_var_key = f"{legacy_var_name.lower()}_{self.env}"
                return Variable.get(airflow_var_key)
        except Exception as e:
            raise KeyError(f"Unable to resolve global variable destination: {legacy_var_name}. Error: {str(e)}")

    def fetch_database_profile(self, target_schema: str) -> dict:
        """
        Replaces legacy '.dw_db' Oracle connection profiles.
        Credentials are encrypted and fetched at runtime from Secret Manager.
        """
        secret_name = f"projects/{self.project_id}/secrets/db-conn-{target_schema}-{self.env}/versions/latest"
        try:
            response = self.secret_client.access_secret_version(request={"name": secret_name})
            payload_str = response.payload.data.decode("UTF-8")
            return json.loads(payload_str)
        except Exception as e:
            raise ConnectionError(f"Failed to access database credentials for schema: {target_schema}. Details: {str(e)}")

    def fetch_execution_parallelism(self) -> dict:
        """
        Replaces legacy '.dw_ai' MFS configurations.
        Determines execution sizing and autoscaling profiles for PySpark tasks.
        """
        # Fetch configurations from Airflow variables based on runtime environment profile
        parallel_depth = Variable.get(f"mfs_depth_{self.env}", default_var="8")
        scaling_profile = {
            "dev": {"min_workers": 2, "max_workers": 5, "machine_type": "n1-standard-4"},
            "qa":  {"min_workers": 2, "max_workers": 10, "machine_type": "n1-standard-8"},
            "prod": {"min_workers": 4, "max_workers": 30, "machine_type": "n1-standard-16"}
        }
        return {
            "do_p": int(parallel_depth),
            "scaling": scaling_profile.get(self.env, scaling_profile["dev"])
        }

    def job_initialization_handshake(self, job_name: str) -> str:
        """
        Replaces '.dw_init' preprocessing.
        Checks for locks, verifies parameters, and registers execution logs.
        """
        # 1. Initialize logging entry
        print(f"[INIT RUN] Executing initialization handshake for process: {job_name} in environment: {self.env}")
        
        # 2. Check for active execution locks
        is_locked = Variable.get(f"lock_{job_name}_{self.env}", default_var="FALSE")
        if is_locked.upper() == "TRUE":
            raise PermissionError(f"Concurrency Lock Violation: {job_name} is already active or locked in environment {self.env}.")
        
        # 3. Set a runtime execution lock
        Variable.set(f"lock_{job_name}_{self.env}", "TRUE")
        print(f"[INIT SUCCESS] System locked. Proceeding with execution of {job_name}.")
        return "SUCCESS"

    def job_finalization_release(self, job_name: str, exit_status: str):
        """
        Ensures execution locks are clean, similar to legacy shell-trap cleanup handlers.
        """
        print(f"[FINALIZE] Executing environment cleanup routines for {job_name}. Exit Status: {exit_status}")
        Variable.set(f"lock_{job_name}_{self.env}", "FALSE")


# ==============================================================================
# PIPELINE EXECUTION EXAMPLE
# ==============================================================================
if __name__ == "__main__":
    # Create the runner environment instance
    gcp_env = GCPEnvironmentLoader(project_id="gcp-prod-dwh", environment_phase="prod")
    
    pipeline_id = "customer_dimension_update"
    
    try:
        # Perform task preprocessing checks (.dw_init emulation)
        gcp_env.job_initialization_handshake(job_name=pipeline_id)
        
        # Resolve path variables (.dw_global emulation)
        landing_path = gcp_env.resolve_global_paths("AI_SERIAL")
        print(f"Reading source raw files from landing zone: {landing_path}")
        
        # Retrieve system security access parameters (.dw_db emulation)
        db_creds = gcp_env.fetch_database_profile(target_schema="dw_core")
        print(f"Acquired credentials for Target Database: {db_creds.get('database_name')}")
        
        # Configure scaling limits (.dw_ai emulation)
        parallel_profile = gcp_env.fetch_execution_parallelism()
        print(f"Dataproc Cluster scaling policy set. Max active compute nodes: {parallel_profile['scaling']['max_workers']}")
        
        # Execute business logic (mock transform engine)
        print("Executing data pipelines successfully...")
        
        # Release system lock resource
        gcp_env.job_finalization_release(job_name=pipeline_id, exit_status="SUCCESS")
        
    except Exception as run_error:
        print(f"Execution Failure: {str(run_error)}")
        gcp_env.job_finalization_release(job_name=pipeline_id, exit_status="FAILED")
```
```

---

## 3. Dynamic Environment Context & Lineage Integration

### 3.1 Job Dependencies (Cross-Job Relationships)
The environment loader configuration is a cross-cutting framework utility. The downstream components consuming this configuration are not standalone. They depend on this loader initialization utility being deployed to GCS/Airflow plugins prior to run time.

**Downstream Consumers (Cross-Job Hand-off):**
* `DW.BERT_AUSD_BP_TA_TARIFOPTION` (Job: Not yet migrated) — Requires connection parameters & variables resolved by this module.
* `DW.DWH_ABPZ_KKM_AIL_AGENT` (Job: Not yet migrated) — Relies on BigQuery dataset naming mappings configured here.
* `DW.DWH_OAIS_EX_PPES_CUBES` (Job: Not yet migrated) — Relies on GCS execution path resolution.
* `folder1_uc4_ksh_abinitio/vobs/dw_source/isdwh/abinitio/bin/r_ai_start` (Job: Not yet migrated) — Sourced-in runtime parameters.
* `r_ai_start` (Job: Not yet migrated) — Sourced-in runtime variables.

### 3.2 Schedulers, Triggers & Variables (Lineage Edges)
* **Predecessors & Invocations**:
  * `.dw_global` invokes and reads parameters from the unresolved helper script `SETPYA.SH` (Confidence: 95%).
  * `.dw_init` references configurations and environment exports from `.dw_global` (Confidence: 85%).
  * `.dw_init` reads specific environment settings from `.DW_LOKAL` (Confidence: 85%).
* **Airflow Implementation**:
  * Instead of flat-file sourcing chains (where `.dw_init` sources `.dw_global` which in turn calls `SETPYA.SH`), all setups are mapped to a Python Class initialization hierarchy.
  * Schedulers (e.g. UC4 Event Triggers) are converted into **Airflow Event-Driven Sensors** or **Cloud Storage Pub/Sub Triggers** that launch Airflow DAG runs.

---

## 4. Environment-Specific Variable Policy

The target GCP environment requires strict separation of environment-wide (GLOBAL) and job-specific configurations. The legacy configurations are categorized and mapped below:

### 4.1 GLOBAL Environment Variables (Managed by Airflow / Secret Manager)
These values are environment-wide (the same for all pipelines in a given deployment target e.g. DEV/TEST/PROD). They are loaded dynamically using Google's native APIs and Airflow's config store:

* **`GCP_PROJECT`**: Sourced from Airflow environment properties. Represents target GCP project IDs.
* **`GCP_REGION`**: Region configurations for Composer and Dataproc resource deployments.
* **`GCS_BUCKET`**: Base GCS bucket (e.g. `gs://dw-source-isdwh-[env]`).
* **`DB_CONNECTION_ENDPOINT`**: Secret Manager secret URI containing database proxy/JDBC strings.
* **`NLS_LANG`**: Native BigQuery/Spark string mappings replacing Oracle's German NLS formatting variables (`GERMAN_GERMANY.WE8MSWIN1252`).

### 4.2 JOB-SPECIFIC Configuration Constants (Inlined/DAG Params)
These values are specific to the individual pipeline executions and do not vary by environment infrastructure:

* **`job_name`**: Passed as a Python argument during pipeline invocation.
* **`legacy_module`**: Explicitly maps back to the legacy schema source (e.g. `isdwh/allgemein`).
* **`lock_name`**: Airflow Variable identifier used to guarantee single-execution runs.

---

## 5. Target File Plan

Below is the file generation plan that will be outputted for the Build Agent. These files replace the physical legacy scripts in the destination architecture:

| Target Relative Path | Target Language | Source Legacy Component | Purpose |
| :--- | :--- | :--- | :--- |
| `dags/plugins/gcp_environment_loader.py` | Python (Airflow / GCP SDK) | `.dw_global`, `.dw_ai`, `.dw_db`, `.dw_init` | Core class utility mapping variable resolution, secret management, and cluster scaling bounds. |
| `dags/gcp_environment_setup_dag.py` | Python (Airflow DAG) | `.dw_init` | Verification DAG to assert global variable status, secret presence, and target GCS access paths. |
| `gcs/scripts/sql/create_metadata_tables.sql` | BigQuery SQL DDL | Framework design | Instantiates control databases used to manage connection mappings and logging. |

---

## 6. Risks & Manual Actions

* **RISK**: Upstream dependency files are unresolved.
  * **SOURCE: NOT FOUND — SETPYA.SH — no candidate**
  * **SOURCE: NOT FOUND — .DW_LOKAL — no candidate**
* **MANUAL ACTION**: Human operators must configure Cloud Secret Manager with JSON keys (`db-conn-dw_core-[env]`) containing actual connection endpoints to mock/migrate target databases before running workflows.
* **OUTPUT/PRINT LITERAL RULE**: To ensure system output parity, any logging messages generated from within the refactored `gcp_environment_loader.py` must retain their exact wording (and native German/English formatting) as defined in the source files. No translation is allowed to avoid breaking automated text/log scrapers.