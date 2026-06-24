```markdown
# MIGRATION_NOTES.md: customer/crm_lineage_tracker.py

## 1. Summary

The `crm_lineage_tracker.py` script, originally an Oracle-dependent Python application for tracking data lineage, has been re-implemented to operate within the Google Cloud Platform (GCP) ecosystem. The migration involved transitioning its core functionality from querying Oracle system catalogs and a custom `ETL_JOB_AUDIT` table to leveraging Google BigQuery's Information Schema, BigQuery Access Controls, and a new BigQuery-based `ETL_JOB_AUDIT` table. The output, a JSON lineage graph, is now stored in Google Cloud Storage (GCS) for consumption by the DataStreak Discovery Engine.

## 2. Generated Artifacts

*   **`customer/crm_lineage_tracker.py`**:
    *   **Role**: This is the re-implemented Python script. It connects to BigQuery, extracts metadata from the Information Schema, queries a BigQuery `etl_job_audit` table, constructs a lineage graph based on predefined GCP-centric patterns, and uploads the resulting JSON to GCS. It replaces the legacy Oracle-dependent script entirely.
*   **`requirements.txt` (Implicit)**:
    *   **Role**: Although not explicitly generated, a `requirements.txt` file would be necessary to list the Python dependencies for the new script, including `google-cloud-bigquery`, `google-cloud-storage`, and potentially `google-cloud-iam-admin`.
*   **JSON Lineage Report (in GCS)**:
    *   **Role**: The final output of the script, a JSON file containing the structured lineage graph (nodes and edges), stored in a designated GCS bucket. This file is intended for consumption by the DataStreak Discovery Engine. Example filename: `gs://<output-bucket-name>/lineage/crm/lineage_CRM_WORKFLOW_YYYYMMDD_TIMESTAMP.json`.

## 3. Key Design Decisions

*   **Platform Transition to GCP**: The entire lineage tracking process has been moved from an on-premise Oracle environment to GCP. This aligns with the broader migration strategy and leverages GCP's managed services.
*   **BigQuery Information Schema as Primary Metadata Source**:
    *   **Rationale**: Replaces Oracle's `DBA_DEPENDENCIES` and `ALL_TAB_PRIVS`. BigQuery's `INFORMATION_SCHEMA` provides native, structured access to metadata about tables, views, and routines within BigQuery, making it the logical successor for dependency inference.
    *   **Trade-off**: While rich, BigQuery's Information Schema might not capture all the nuanced dependency types or "grants" that Oracle's internal system views did. Simple string parsing of `view_definition` and `routine_definition` is used for dependency inference, which is less robust than a full SQL parser.
*   **Google Cloud Storage for Output**:
    *   **Rationale**: Replaces local filesystem storage. GCS offers scalable, durable, and highly available object storage, seamlessly integrating with other GCP services and providing a reliable endpoint for the DataStreak Discovery Engine.
*   **`google-cloud-bigquery` and `google-cloud-storage` Client Libraries**:
    *   **Rationale**: Replaces the `cx_Oracle` library. These are the official and idiomatic Python client libraries for interacting with BigQuery and GCS, ensuring secure, efficient, and well-supported integration within GCP.
*   **Dedicated BigQuery `ETL_JOB_AUDIT` Table**:
    *   **Rationale**: Replaces the custom Oracle `ETL_JOB_AUDIT` table. This provides a consistent mechanism for tracking ETL job executions within the BigQuery environment.
    *   **Trade-off**: Requires all migrated ETL jobs to be updated to write their audit records to this new BigQuery table, necessitating cross-team coordination. An alternative (processing BigQuery audit logs via Stackdriver) was considered but deemed more complex for initial implementation.
*   **Redesign of `lineage_chains`**:
    *   **Rationale**: The legacy `lineage_chains` were hard-coded and specific to Oracle-based ETL patterns (e.g., UC4, ksh, PL/SQL). These are irrelevant in GCP. The new design introduces GCP-native patterns (e.g., Composer DAGs, Dataflow jobs, BigQuery Stored Procedures, dbt models).
    *   **Trade-off**: This is a **critical redesign item (B4)**. The current implementation hardcodes these chains within the script and provides only illustrative examples. A comprehensive definition requires significant manual effort and collaboration with data engineers to accurately map legacy patterns to their GCP equivalents. Externalizing this configuration (e.g., to a YAML file in GCS or a BigQuery table) is a recommended follow-up.
*   **Simplified Cross-Dataset Access Inference**:
    *   **Rationale**: Replaces Oracle's `ALL_TAB_PRIVS`. The script infers cross-dataset access by examining BigQuery dataset access controls (`dataset.access_entries`).
    *   **Trade-off**: This is a simplified approach. A truly comprehensive analysis of cross-project/cross-dataset permissions would involve deeper integration with GCP IAM policies at various resource levels using `google-cloud-iam-admin` and `google-cloud-resource-manager` APIs, which is more complex.
*   **Python Execution Environment (Cloud Functions/Run/Composer)**:
    *   **Rationale**: The design document outlines flexibility in deployment (Cloud Functions for event-driven, Cloud Run for containerized, Cloud Composer for orchestration). This allows choosing the best fit based on execution frequency, complexity, and integration needs. The generated script is designed to be portable across these environments.

## 4. Manual Steps Before Go-Live

The following manual steps are required to prepare the GCP environment for the `crm_lineage_tracker.py` script:

1.  **GCP Project Setup**:
    *   Ensure the target GCP project (`DEFAULT_GCP_PROJECT` in the script, e.g., `your-gcp-project-id`) exists and is correctly configured.
    *   Identify or create the BigQuery datasets (`DEFAULT_BIGQUERY_DATASETS`, e.g., `your_dataset_1`, `your_dataset_2`) that the lineage tracker needs to scan.
    *   Identify or create the BigQuery dataset (`DEFAULT_AUDIT_DATASET`, e.g., `your_audit_dataset`) where the `etl_job_audit` table will reside.
2.  **IAM & Permissions**:
    *   Create a dedicated GCP Service Account for the lineage tracker.
    *   Grant this Service Account the following IAM roles:
        *   `BigQuery Data Viewer` (or custom role with `bigquery.tables.get`, `bigquery.tables.list`, `bigquery.routines.get`, `bigquery.routines.list`, `bigquery.views.get`, `bigquery.views.list`, `bigquery.datasets.getIamPolicy`, `bigquery.jobs.list` permissions) on all relevant BigQuery projects/datasets.
        *   `BigQuery Data Editor` (or custom role with `bigquery.tables.getData`, `bigquery.tables.list`, `bigquery.jobs.create` permissions) on the `DEFAULT_AUDIT_DATASET` to query the `etl_job_audit` table.
        *   `Storage Object Creator` and `Storage Object Viewer` on the target GCS bucket (`DEFAULT_OUTPUT_GCS_BUCKET`).
        *   If using Data Catalog, `Data Catalog Viewer` role.
3.  **BigQuery `etl_job_audit` Table Creation**:
    *   Create the `etl_job_audit` table in BigQuery within the `DEFAULT_AUDIT_DATASET`. The schema should be designed to capture relevant ETL job execution details (e.g., `job_id`, `job_name`, `start_time`, `end_time`, `status`, `source_table`, `target_table`, `workflow_name`).
    *   **Crucially**: Ensure all other migrated ETL jobs are updated to write their audit records to this new BigQuery `etl_job_audit` table.
4.  **GCS Bucket Creation**:
    *   Create the Google Cloud Storage bucket (`DEFAULT_OUTPUT_GCS_BUCKET`, e.g., `your-lineage-output-bucket`) where the generated JSON lineage reports will be stored.
5.  **Configuration Updates**:
    *   Replace all placeholder values in the `customer/crm_lineage_tracker.py` script (e.g., `DEFAULT_GCP_PROJECT`, `DEFAULT_BIGQUERY_DATASETS`, `DEFAULT_AUDIT_DATASET`, `DEFAULT_OUTPUT_GCS_BUCKET`) with actual GCP resource IDs.
    *   **CRITICAL**: Redefine the `gcp_lineage_chains` within the `build_lineage_graph` function to accurately reflect the actual GCP-native ETL patterns and component names in your environment. Consider externalizing this configuration to a YAML file in GCS or a dedicated BigQuery table for easier management.
6.  **Deployment and Scheduling**:
    *   Deploy the `customer/crm_lineage_tracker.py` script to the chosen GCP compute environment (e.g., Cloud Function, Cloud Run, or a VM managed by Cloud Composer).
    *   Configure a scheduler (e.g., Cloud Scheduler, Cloud Composer DAG) to trigger the execution of the deployed script at the desired frequency (e.g., weekly, daily).

## 5. Known Gaps & Unresolved References

*   **`lineage_chains` Redesign (B4 Item)**: The `gcp_lineage_chains` are currently hardcoded and illustrative. This is the most significant unresolved item. A comprehensive, accurate, and maintainable definition of these chains, reflecting the actual GCP ETL landscape, is required. This will likely involve externalizing them into a configurable format (e.g., YAML in GCS) and a dedicated effort to define them.
*   **Robust SQL Parsing for Dependencies**: The current `extract_metadata_from_bigquery` function uses simple string matching to infer dependencies from `view_definition` and `routine_definition`. This is prone to errors and may miss complex dependencies. A more robust solution would involve a dedicated SQL parser, or leveraging Google Cloud Data Catalog's native lineage capabilities (if available and configured).
*   **Comprehensive IAM Policy Analysis**: The `cross_dataset_access` inference is simplified, relying on BigQuery dataset access entries. A full understanding of cross-project/cross-dataset data access would require deeper integration with GCP IAM APIs (`google-cloud-iam-admin_v1`) to analyze policies at various resource hierarchies.
*   **`ETL_JOB_AUDIT` Table Population**: The script relies on a BigQuery `etl_job_audit` table. This table needs to be actively populated by all other migrated ETL jobs. If these jobs are not yet migrated or updated, the audit data will be incomplete, impacting the lineage graph. This is a cross-team dependency.
*   **DataStreak Discovery Engine Integration**: The assumption is that the DataStreak Discovery Engine can consume the generated JSON format from GCS. This needs to be explicitly verified with the DataStreak team to ensure compatibility and successful ingestion.
*   **Compute Environment Finalization**: The specific GCP compute environment (Cloud Function, Cloud Run, Cloud Composer) for deployment is not yet finalized. The choice will impact deployment specifics and scheduling mechanisms.

## 6. Validation

To validate the successful migration and operation of the `crm_lineage_tracker.py` script:

1.  **Local Execution (for initial testing)**:
    *   Ensure `gcloud auth application-default login` has been run to authenticate your local environment.
    *   Install dependencies: `pip install -r requirements.txt` (assuming `requirements.txt` is created).
    *   Run the script from the command line, providing necessary arguments:
        ```bash
        python customer/crm_lineage_tracker.py \
            --gcp_projects your-gcp-project-id \
            --bigquery_datasets your_dataset_1 your_dataset_2 \
            --run_date 2023-10-26 \
            --output_gcs_bucket your-lineage-output-bucket
        ```
    *   **Passing Criteria**: The script should execute without errors, print "Lineage tracking complete," and a JSON file should appear in the specified GCS bucket.

2.  **Deployment and Scheduled Execution (for production validation)**:
    *   Deploy the script to the chosen GCP compute environment (e.g., Cloud Function, Cloud Run).
    *   Trigger the execution manually or via its configured scheduler (e.g., Cloud Scheduler, Cloud Composer).
    *   **Passing Criteria**:
        *   The execution completes successfully in the GCP environment (check logs in Cloud Logging).
        *   A new JSON lineage report file is generated in the configured GCS bucket at the expected path.
        *   The JSON file contains a `nodes` array with entries for BigQuery tables, views, routines, and ETL jobs, and an `edges` array showing dependencies.
        *   The content of the JSON file accurately reflects the current state of BigQuery metadata and the defined `gcp_lineage_chains`.
        *   **Ultimate Passing Criteria**: The DataStreak Discovery Engine successfully ingests the generated JSON report from GCS and displays the lineage correctly within its interface, reflecting the GCP data flows.

## 7. Rollback Procedure

In case of issues or unexpected behavior with the migrated `crm_lineage_tracker.py` script, the following rollback procedure can be followed:

1.  **Immediate Action**:
    *   Stop any scheduled executions of the new `crm_lineage_tracker.py` script in GCP (e.g., disable Cloud Scheduler job, pause Cloud Composer DAG).
    *   Delete any erroneous or incomplete JSON lineage reports generated by the new script from the GCS output bucket.
2.  **Revert to Legacy System**:
    *   Ensure the legacy Oracle environment and the original `crm_lineage_tracker.py` script are still operational.
    *   Resume the execution of the legacy `crm_lineage_tracker.py` script on its original schedule. This will ensure that the DataStreak Discovery Engine continues to receive lineage updates from the legacy source.
3.  **Clean Up GCP Resources (if necessary)**:
    *   Delete the deployed GCP compute resource (e.g., Cloud Function, Cloud Run service) associated with the new script.
    *   If the BigQuery `etl_job_audit` table was created specifically for this migration and is not used by other systems, consider dropping it.
    *   If the GCS output bucket was created solely for this migration, consider deleting it.
    *   Revert any IAM permissions granted to the Service Account used by the new script.
4.  **Investigation and Redesign**:
    *   Analyze the root cause of the issues encountered with the migrated script.
    *   Address the identified gaps (e.g., refine `lineage_chains`, improve SQL parsing, enhance IAM analysis).
    *   Redeploy and re-validate once fixes are implemented.

**Note**: A successful rollback relies on the continued availability and functionality of the legacy Oracle environment and its associated lineage tracking processes during the migration transition period.
```