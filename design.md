# Migration Design — customer/crm_lineage_tracker.py

## 1. Purpose & Scope

The `crm_lineage_tracker.py` script serves as a critical metadata and lineage reporting tool within the legacy environment. Its primary function is to track and document source-to-target data lineage across various business domains (sales, finance, customer) by querying Oracle's internal system catalogs (`DBA_DEPENDENCIES`, `ALL_TAB_PRIVS`) and a custom `ETL_JOB_AUDIT` table. The script then compiles this information into a JSON output, which is consumed by the DataStreak Discovery Engine (referencing requirement REQ-WG-02).

The scope of this migration involves re-implementing the core functionality of `crm_lineage_tracker.py` to seamlessly operate within the Google Cloud Platform, specifically leveraging BigQuery for its data warehousing capabilities and metadata. This entails a significant adaptation of the data extraction logic, transitioning from Oracle-specific system catalogs and metadata structures to BigQuery's equivalent metadata sources (e.g., Information Schema, Data Catalog, audit logs). Furthermore, the hard-coded lineage chain definitions, which are currently tied to the legacy environment, will need to be re-evaluated and redefined to reflect the BigQuery/GCP data processing landscape. The final output, a JSON lineage graph, should either maintain its existing format or be adapted to meet the requirements of the DataStreak Discovery Engine in the new BigQuery ecosystem.

## 2. Source Inventory

*   **File Name:** `customer/crm_lineage_tracker.py`
*   **Technology:** Python (utilizing the `cx_Oracle` library for Oracle database interaction)
*   **Category:** `python`
*   **Tool (Primary Type):** `Python`
*   **Summary:** This script is designed to track and document source-to-target data lineage across sales, finance, and customer domains. It achieves this by querying Oracle system catalogs and cross-schema objects to construct an end-to-end lineage graph, which is then outputted as JSON.
*   **Purpose:** Lineage tracking and metadata reporting.
*   **Complexity Tier:** Undetermined (data from `file_complexity` was not available). However, given the nature of the script requiring a complete rewrite of database interaction logic and hard-coded lineage definitions, it is anticipated to be at least "medium" to "complex".
*   **Migration Flags:** Undetermined (data from `file_complexity` was not available).
*   **Automation Bucket:** `manual` (B3)

**Key Observations from Source Analysis:**
*   The script configures Oracle database connections using environment variables for parameters such as host, port, SID, user, and password.
*   It executes specialized SQL queries against Oracle's internal system views to infer object dependencies (`DBA_DEPENDENCIES`) and cross-schema data access via grants (`ALL_TAB_PRIVS`).
*   A custom `ETL_JOB_AUDIT` table is queried to obtain job execution audit records.
*   A significant portion of the lineage graph construction relies on a hard-coded list of `lineage_chains`, which describe specific ETL patterns and component mappings within the legacy Oracle environment (e.g., UC4 Trigger -> ksh Script -> Oracle Staging -> PL/SQL Job).
*   The output is a JSON file written to a local file system directory.

## 3. Target Architecture

The migrated lineage tracking job will be implemented as a Python application running within the Google Cloud Platform. The choice of compute environment (Cloud Function, Cloud Run, or a scheduled Dataflow/Composer job) will depend on the required execution frequency, latency, and resource scalability.

*   **Compute:** A Python 3.x execution environment on GCP, such as:
    *   **Cloud Functions:** For event-driven or scheduled execution (e.g., via Cloud Scheduler).
    *   **Cloud Run:** For a containerized, serverless deployment, offering more control over the environment.
    *   **Cloud Composer (Apache Airflow):** For more complex scheduling, dependency management, and integration with other GCP services as part of a larger data orchestration pipeline.
*   **Metadata Source:** The Oracle system catalogs will be replaced by:
    *   **BigQuery Information Schema:** For discovering datasets, tables, views, stored procedures, and their definitions/dependencies (e.g., `INFORMATION_SCHEMA.TABLE_OPTIONS`, `INFORMATION_SCHEMA.VIEWS`, `INFORMATION_SCHEMA.ROUTINES`, `INFORMATION_SCHEMA.JOBS`).
    *   **Google Cloud Data Catalog:** For richer, unified metadata management and lineage tracking features across GCP services.
    *   **BigQuery IAM Policies:** For inferring cross-project/cross-dataset data access and "grants."
    *   **Stackdriver Logging (BigQuery Audit Logs):** For gathering operational metrics and audit records equivalent to the `ETL_JOB_AUDIT` table.
*   **Output Storage:** The generated JSON lineage files will be stored in a Google Cloud Storage (GCS) bucket, providing scalable and durable object storage.
*   **Secret Management:** Google Cloud Secret Manager will be used to securely store and access any sensitive configuration, replacing environment variables for database credentials.

**High-Level Target Design:**
The refactored Python application will:
1.  Utilize the `google-cloud-bigquery` client library (and potentially other GCP client libraries for Data Catalog, IAM, Storage) to interact with GCP services.
2.  Query BigQuery's Information Schema and potentially Data Catalog APIs to extract metadata about tables, views, and routines, and infer their dependencies.
3.  Query GCP IAM policies and BigQuery access controls to understand data sharing and cross-dataset/project access patterns.
4.  Optionally, process BigQuery audit logs or query a dedicated BigQuery audit table to gather ETL job metrics.
5.  Re-evaluate and update the currently hard-coded `lineage_chains` to reflect BigQuery-native ETL patterns (e.g., Dataflow jobs, dbt models, BigQuery stored procedures, Composer DAGs). This is a major redesign component.
6.  Generate the lineage JSON output and store it in a specified Cloud Storage bucket.

## 4. Data Flow & Lineage

The original `crm_lineage_tracker.py` script *infers* lineage from Oracle metadata. The migrated version will continue this role, inferring lineage from BigQuery/GCP metadata.

**Legacy Inferred Lineage (as documented in source code):**
UC4 Trigger → DIL Source → ksh Script → Oracle Staging Table → PL/SQL Job → Target Analytical Table

**Migrated System Lineage Flow:**

1.  **Configuration Inputs:** The Python application receives configuration parameters (e.g., `run_date`, `workflow`, target GCP projects/datasets, output GCS bucket path) via command-line arguments, environment variables, or a configuration file.
2.  **Metadata Extraction (Python Application):**
    *   The application connects to BigQuery and other GCP services using appropriate client libraries.
    *   It executes queries against BigQuery's `INFORMATION_SCHEMA` (e.g., `TABLES`, `VIEWS`, `ROUTINES`, `JOBS`) to retrieve metadata.
    *   It leverages GCP IAM APIs to understand access permissions across BigQuery resources.
    *   (Optional) It processes relevant BigQuery audit logs from Stackdriver Logging (e.g., job creation, modification, execution) to extract operational lineage information.
    *   It ingests the redefined, BigQuery-centric `lineage_chains` configuration.
3.  **Lineage Graph Construction (Python Application):** The extracted metadata and defined lineage patterns are processed by the application's logic (similar to `build_lineage_graph`) to construct a comprehensive lineage graph. This graph will now reflect dependencies and data flows within the BigQuery and broader GCP ecosystem.
4.  **Output Generation & Storage:** The application serializes the constructed lineage graph into a JSON format. This JSON file is then uploaded to a designated Google Cloud Storage bucket (e.g., `gs://<output-bucket-name>/lineage/crm/lineage_CRM_WEEKLY_WORKFLOW_YYYYMMDD.json`).
5.  **DataStreak Discovery Engine Consumption:** The DataStreak Discovery Engine retrieves and ingests the JSON lineage report directly from the Cloud Storage bucket for its analysis and visualization purposes.

## 5. Transformation Logic

The primary transformation involves converting Oracle-specific database interactions and metadata queries to their BigQuery/GCP equivalents.

*   **Database Connectivity:**
    *   **Legacy:** Uses `cx_Oracle.connect()` to establish a connection to an Oracle database, relying on DSN and environment variables for credentials.
    *   **Target:** `google.cloud.bigquery.Client()` will be used to connect to BigQuery. Authentication will leverage Application Default Credentials or a specified Service Account, enhancing security and integration within GCP.
*   **Table/View Dependency Extraction (`extract_table_dependencies`):**
    *   **Legacy:** SQL queries target `DBA_DEPENDENCIES` (with fallback to `ALL_DEPENDENCIES`) to find PL/SQL object dependencies on tables, views, and synonyms.
    *   **Target:** This function will be re-implemented to query BigQuery's `INFORMATION_SCHEMA` views (`TABLES`, `VIEWS`, `ROUTINES`, `JOBS`) to identify table and view definitions, as well as dependencies. Parsing of SQL within `view_definition` and routine bodies will be crucial for inferring explicit dependencies. Integration with Google Cloud Data Catalog APIs may provide a more robust and native lineage tracking solution.
*   **Cross-Schema Grants (`extract_cross_schema_grants`):**
    *   **Legacy:** Queries `ALL_TAB_PRIVS` to identify cross-schema `SELECT`, `INSERT`, `UPDATE`, `DELETE` grants.
    *   **Target:** This logic will be redesigned to analyze GCP IAM policies at the project and dataset levels, and BigQuery dataset/table access control lists. The concept of "cross-schema grants" will be mapped to cross-project or cross-dataset data access permissions granted to users or service accounts. This is a conceptual mapping and will likely involve using `google-cloud-iam` or `google-cloud-resource-manager` client libraries.
*   **ETL Job Audit (`extract_etl_job_audit`):**
    *   **Legacy:** Queries a custom `ETL_JOB_AUDIT` table in Oracle for job execution metrics based on `run_date`.
    *   **Target:** Two primary approaches:
        *   **Option A (Custom BigQuery Audit Table):** A dedicated `ETL_JOB_AUDIT` table will be created in BigQuery, mirroring the schema of its Oracle predecessor. All migrated ETL jobs will be updated to write their audit records to this BigQuery table. The Python function will then query this BigQuery table.
        *   **Option B (BigQuery Audit Logs):** Develop logic to parse and extract relevant job audit information from BigQuery's native audit logs, accessible via Stackdriver Logging (potentially exported to BigQuery for easier querying). This approach avoids maintaining a custom audit table but requires careful log parsing.
*   **Lineage Chain Definitions (`build_lineage_graph` - `lineage_chains`):**
    *   **Legacy:** A static, hard-coded list of dictionaries describing specific, predefined lineage patterns linked to legacy components (UC4, ksh, Ab Initio, PL/SQL).
    *   **Target (Redesign):** This is a critical redesign area. The `lineage_chains` must be redefined to reflect the actual data flow and job orchestration within the GCP environment. This could involve:
        *   Replacing legacy component names (e.g., UC4 job, ksh script) with GCP equivalents (e.g., Cloud Composer DAG IDs, Dataflow job names, BigQuery Stored Procedure names, dbt model names).
        *   Moving these definitions into an external, configurable format (e.g., YAML file stored in GCS, or a dedicated BigQuery configuration table) to decouple them from the application code.
*   **Schema to Domain Mapping (`_schema_to_domain`):**
    *   **Legacy:** Maps Oracle schema names (`CRM_SCHEMA`, `DW_OWNER`, `FINANCE_SCHEMA`) to business domains (`customer`, `sales`, `finance`).
    *   **Target:** This mapping will be updated to use BigQuery project IDs, dataset IDs, or other logical grouping mechanisms within GCP to align with the new data organization.

## 6. External Dependencies

*   **Oracle Database:**
    *   **Legacy Role:** The central source for all lineage-related metadata and `ETL_JOB_AUDIT` records. The `cx_Oracle` library facilitates interaction.
    *   **Replacement in GCP:** BigQuery's native metadata services (Information Schema, Data Catalog), BigQuery IAM, and Stackdriver Logging will collectively replace Oracle as the source of metadata. The `cx_Oracle` library will be removed from the Python application's dependencies.
*   **ETL_JOB_AUDIT Table:**
    *   **Legacy Role:** A custom Oracle table used to store detailed audit records of ETL job executions.
    *   **Replacement in GCP:** This will be replaced by either a newly created, custom `ETL_JOB_AUDIT` table within BigQuery (populated by other migrated ETL jobs) or by extracting relevant job execution details directly from BigQuery's own audit logs available via Stackdriver Logging.
*   **Local File System (`/opt/etl/lineage/crm`):**
    *   **Legacy Role:** The local directory where the generated JSON lineage reports are stored.
    *   **Replacement in GCP:** A Google Cloud Storage (GCS) bucket will be used as the target for the JSON output files. The application will use the `google-cloud-storage` client library for file upload operations.
*   **Legacy Orchestration & Data Processing Components (UC4, DIL, ksh, SQLPlus, Ab Initio, Spark Scala):**
    *   **Legacy Role:** These are not direct dependencies of `crm_lineage_tracker.py` itself but are components whose lineage `crm_lineage_tracker.py` is designed to track.
    *   **Replacement in GCP:** In the migrated environment, these will be replaced by their GCP-native counterparts. For example, UC4 triggers might become Cloud Composer DAGs or Cloud Scheduler jobs; ksh/SQLPlus scripts might be BigQuery stored procedures or Dataflow/Dataproc jobs; Ab Initio/Spark Scala jobs would likely translate to Dataflow or Dataproc jobs. The lineage tracker will then need to understand the metadata and relationships of these new GCP components.

## 7. Unresolved / Risks

*   **Missing `file_complexity` data:** The lack of complexity tier and migration flags for `crm_lineage_tracker.py` is a data gap. Given the `manual` migration bucket, we assess this as a "complex" migration due to the bespoke nature of the Oracle metadata extraction.
*   **`DBA_DEPENDENCIES` Fallback Logic:** The Oracle script's fallback to `ALL_DEPENDENCIES` suggests potential permission issues or varied access levels in the source. This highlights a need to carefully manage IAM permissions for the BigQuery service account that will execute the migrated lineage tracker to ensure it has comprehensive read access to all necessary metadata.
*   **Completeness of BigQuery Metadata:** While BigQuery's Information Schema is rich, it may not perfectly map all intricate dependency types or "grants" directly comparable to Oracle's system views. A thorough analysis of BigQuery's metadata capabilities versus the current script's needs is crucial. Google Cloud Data Catalog may be required for a more complete picture.
*   **Hard-coded Lineage Chains (`lineage_chains` array):**
    *   **CRITICAL REDESIGN ITEM (B4):** This is the highest risk and requires a full redesign. These chains are inherently tied to the legacy environment's specific job topology. A direct migration is not feasible. The new design must:
        *   Identify the equivalent GCP components for each legacy step (e.g., which BigQuery job/procedure replaces a PL/SQL package).
        *   Either dynamically infer these chains from GCP orchestration metadata (e.g., parsing Cloud Composer DAGs) or manage them via an external, configurable artifact specific to the GCP environment.
*   **`ETL_JOB_AUDIT` Table Reliance:** The script's dependency on a custom `ETL_JOB_AUDIT` table poses a risk if other ETL jobs are not simultaneously migrated to populate a BigQuery equivalent. This requires cross-job coordination.
*   **Dynamic SQL Parsing:** The Oracle version benefits from the database's internal dependency resolution. In BigQuery, inferring lineage from SQL definitions (e.g., in views or stored procedures) often requires implementing SQL parsing logic within the Python application, which can be complex and error-prone.
*   **Credential Management:** The transition from environment variables for database credentials to GCP's Secret Manager and service accounts requires careful implementation to maintain security best practices.

## 8. Build Plan

The migration of `crm_lineage_tracker.py` is a re-implementation effort, focusing on adapting its metadata extraction and lineage reporting logic to the BigQuery/GCP ecosystem.

**Phase 1: Foundational Setup & Connectivity (Python)**
1.  **Project Setup:** Create a dedicated GCP project or configure existing projects/datasets for the lineage tracking solution.
2.  **Authentication & Authorization:**
    *   Establish a dedicated GCP Service Account for the lineage tracker with read-only permissions to BigQuery Information Schema, Data Catalog (if used), IAM policies, and write access to the target GCS bucket.
    *   Configure access to BigQuery resources via `google-cloud-bigquery` using the Service Account.
3.  **Refactor `get_connection()`:**
    *   Remove `cx_Oracle` library and replace with `google.cloud.bigquery` client library initialization (`bigquery.Client()`).
    *   Update credential handling to use GCP best practices (e.g., implicit credentials via Service Account).
4.  **Configuration Management:**
    *   Adapt argument parsing (`parse_args()`) to accept GCP project IDs and dataset IDs instead of Oracle schema names.
    *   Implement mechanisms to retrieve configurations (e.g., GCS output bucket path) from environment variables or Secret Manager.

**Phase 2: BigQuery Metadata Extraction Logic (Python)**
1.  **Re-implement `extract_table_dependencies()`:**
    *   Develop functions to query `INFORMATION_SCHEMA.TABLES`, `INFORMATION_SCHEMA.VIEWS`, and `INFORMATION_SCHEMA.ROUTINES` across the configured GCP projects and datasets.
    *   Implement logic to parse `view_definition` and routine bodies (if available) to infer dependencies between BigQuery objects.
    *   (Optional but Recommended) Integrate with Google Cloud Data Catalog APIs to enrich metadata and leverage its native lineage capabilities.
2.  **Re-implement `extract_cross_schema_grants()`:**
    *   Develop functions to query GCP IAM policies (`get_iam_policy`) for relevant BigQuery projects and datasets.
    *   Analyze dataset and table access control lists within BigQuery to identify cross-project/cross-dataset data sharing, translating the concept of Oracle grants to BigQuery's access model.
3.  **Re-implement `extract_etl_job_audit()`:**
    *   **Option A (Recommended):** If the custom audit table is critical, design and create an `ETL_JOB_AUDIT` table in BigQuery. Update all other migrated ETL jobs to write their audit records to this new BigQuery table. The `extract_etl_job_audit` function will then query this BigQuery table.
    *   **Option B:** Develop functions to query `INFORMATION_SCHEMA.JOBS` for BigQuery job history or process BigQuery audit logs from Stackdriver Logging (exported to BigQuery) to derive equivalent operational audit information.

**Phase 3: Lineage Graph Construction & Output (Python)**
1.  **Redesign `lineage_chains` Configuration:**
    *   **CRITICAL REDESIGN:** Collaborate with data engineers and architects to define the new, BigQuery-native `lineage_chains`. This involves mapping legacy ETL patterns (UC4, ksh, PL/SQL) to their GCP counterparts (Composer DAGs, Dataflow jobs, BigQuery Stored Procedures, dbt models).
    *   Store these redesigned lineage patterns in a separate, easily configurable format (e.g., YAML file in GCS, or a dedicated BigQuery configuration table) to ensure flexibility and maintainability.
2.  **Refactor `build_lineage_graph()`:**
    *   Update the logic to process the BigQuery metadata retrieved in Phase 2 and integrate it with the redesigned `lineage_chains`.
    *   Ensure the output JSON structure remains consistent with the DataStreak Discovery Engine's requirements, making any necessary adjustments for BigQuery-specific metadata.
3.  **Output to Cloud Storage:**
    *   Replace local file system write operations (`os.makedirs`, `open()`) with calls to the `google.cloud.storage` client library to upload the generated JSON lineage reports to the configured GCS bucket.

**Phase 4: Deployment & Orchestration**
1.  **Deployment:** Deploy the refactored Python application to the chosen GCP compute platform (Cloud Function, Cloud Run, or VM).
2.  **Scheduling:** Configure a scheduler (e.g., Cloud Scheduler, Cloud Composer DAG) to trigger the execution of the lineage tracker at predefined intervals (e.g., weekly, daily, hourly), ensuring timely updates to the lineage graph.

**Language:** Python (re-implemented using GCP client libraries)