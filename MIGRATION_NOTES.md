# MIGRATION_NOTES for EXIS

## 1. Summary

The EXIS job, originally an Oracle Data Warehouse export process orchestrated by UC4 and implemented with KornShell scripts and Oracle PL/SQL, has been migrated to Google Cloud Platform (GCP). Its primary function is to extract various master data sets (telephone system data, stock data, discount data) into gzipped CSV files and distribute them to external target systems via SFTP.

The migrated solution leverages:
*   **Orchestration:** Apache Airflow on Cloud Composer.
*   **Data Processing:** Google BigQuery for SQL transformations and Python for data extraction, post-processing, and file handling.
*   **File Storage:** Google Cloud Storage (GCS) for intermediate and archival storage.
*   **External Data Transfer:** Python with `paramiko` for SFTP distribution.

## 2. Generated Artifacts

The migration produced the following files:

*   **`sql/bq_d_exis_apt_bestandsdaten.sql`**:
    *   **Role:** BigQuery SQL script for extracting stock data. This is a direct translation of the original Oracle PL/SQL script (`d_exis_apt_bestandsdaten.sql`) to BigQuery SQL syntax, reading from BigQuery raw layer tables.
*   **`sql/bq_d_exis_apt_nna_daten.sql`**:
    *   **Role:** BigQuery SQL script for extracting telephone system master data. This is a direct translation of the original Oracle PL/SQL script (`d_exis_apt_nna_daten.sql`) to BigQuery SQL syntax, reading from BigQuery raw layer tables and accepting a `FROM_YYYYMM` parameter.
*   **`sql/bq_d_exis_apt_nna_voice.sql`**:
    *   **Role:** BigQuery SQL script for extracting telephone system voice data. This is a direct translation of the original Oracle PL/SQL script (`d_exis_apt_nna_voice.sql`) to BigQuery SQL syntax, reading from BigQuery raw layer tables and accepting a `FROM_YYYYMM` parameter.
*   **`sql/bq_d_exis_apt_rabattdaten.sql`**:
    *   **Role:** BigQuery SQL script for extracting discount data. This is a direct translation of the original Oracle PL/SQL script (`d_exis_apt_rabattdaten.sql`) to BigQuery SQL syntax, reading from BigQuery raw layer tables.
*   **`exis_exporter.py`**:
    *   **Role:** A core Python application that encapsulates the logic previously found in the `r_exis_v2` KornShell script. It handles:
        *   Connecting to BigQuery and executing SQL queries.
        *   Fetching query results.
        *   Post-processing data (e.g., adding footers, similar to `nawk`).
        *   Gzip compression.
        *   Uploading files to GCS.
        *   Distributing files via SFTP using `paramiko`.
        *   It is designed to be configurable via a JSON dictionary, allowing it to be reused by different EXIS export jobs.
*   **`dags/exis_nna_data_dag.py`**:
    *   **Role:** An Apache Airflow DAG responsible for orchestrating the `EXIS_SD_APT_NNA_DATA` export. It defines a PythonOperator task that calls the `exis_exporter.py` with specific configuration for the NNA Data export, including the SQL file path, output naming, query parameters, footer configuration, and SFTP details.
*   **`dags/exis_nna_voice_dag.py`**:
    *   **Role:** An Apache Airflow DAG responsible for orchestrating the `EXIS_SD_APT_NNA_VOIC` export. Similar to the NNA Data DAG, it configures and invokes `exis_exporter.py` for the NNA Voice data.
*   **`dags/exis_bestands_dag.py`**:
    *   **Role:** An Apache Airflow DAG responsible for orchestrating the `EXIS_SD_APT_BESTANDS` export. It configures and invokes `exis_exporter.py` for the stock data.
*   **`dags/exis_rabatt_dag.py`**:
    *   **Role:** An Apache Airflow DAG responsible for orchestrating the `EXIS_SD_APT_RABATT` export. It configures and invokes `exis_exporter.py` for the discount data.

## 3. Key Design Decisions

*   **Orchestration with Multiple Airflow DAGs:**
    *   **Decision:** Instead of a single monolithic DAG, four independent Airflow DAGs were created, each corresponding to one of the original UC4 `JOBS_UNIX` objects.
    *   **Rationale:** This approach promotes modularity, independent scheduling, and clearer separation of concerns. Each export job can be managed, monitored, and retried individually, reducing the blast radius of failures. It aligns with Airflow's best practices for managing distinct workflows.
    *   **Trade-offs:** Requires managing four separate DAGs and their schedules. If there were complex inter-dependencies between these jobs in UC4 (not evident from the provided XMLs), a higher-level orchestrating DAG might be needed, which is a known gap.

*   **BigQuery for SQL Transformations, Python for Orchestration and File Handling:**
    *   **Decision:** Oracle PL/SQL scripts were translated directly to BigQuery SQL for data extraction, while the complex shell script logic (`r_exis_v2`) was re-implemented in a Python application (`exis_exporter.py`).
    *   **Rationale:** BigQuery is optimized for large-scale analytical SQL queries, providing performance and scalability. Python offers a robust, readable, and maintainable environment for orchestration, data manipulation (like `nawk` equivalents), compression, and interacting with GCP services (GCS, BigQuery client libraries) and external systems (SFTP). This leverages the strengths of both technologies.
    *   **Trade-offs:** Requires careful translation of Oracle-specific SQL constructs and functions to BigQuery. Introduces a new programming language (Python) into the data processing pipeline.

*   **Configuration Management:**
    *   **Decision:** Original `.var` and `.cfg` files were abstracted into Python dictionaries within the Airflow DAGs, with sensitive information (like SFTP credentials) intended for Google Secret Manager or Airflow Connections.
    *   **Rationale:** Centralizes configuration within the Airflow environment, making it dynamic and manageable. Using Secret Manager enhances security for sensitive credentials.
    *   **Trade-offs:** Requires manual migration of configuration parameters and careful mapping to Python dictionary keys.

*   **SFTP Distribution with Python `paramiko`:**
    *   **Decision:** SFTP distribution is handled by a Python function within `exis_exporter.py` using the `paramiko` library.
    *   **Rationale:** Provides direct control over the SFTP process and allows for flexible error handling and logging. It's a common and well-supported method for programmatic SFTP.
    *   **Trade-offs:** Requires managing SFTP credentials securely (e.g., Secret Manager). An alternative, Cloud Storage Transfer Service, was considered but might not be suitable if the external system cannot pull from GCS or if specific SFTP features are required.

*   **Google Cloud Storage (GCS) for Filesystem Operations:**
    *   **Decision:** All local filesystem operations (temporary files, archival) were replaced with GCS operations.
    *   **Rationale:** GCS offers highly durable, scalable, and cost-effective object storage, seamlessly integrated with other GCP services. It eliminates the need for managing local disk space on compute instances.
    *   **Trade-offs:** Requires adapting file paths and operations to GCS object storage semantics.

*   **Direct SQL Translation:**
    *   **Decision:** Oracle SQL scripts were translated as directly as possible to BigQuery SQL.
    *   **Rationale:** Minimizes functional changes and reduces the risk of introducing new bugs. It leverages BigQuery's powerful SQL engine.
    *   **Trade-offs:** Requires careful handling of data type conversions, function equivalences, and removal of Oracle-specific hints. Performance characteristics might differ and require BigQuery-specific tuning.

*   **`r_exis_v2` Redesign into `exis_exporter.py`:**
    *   **Decision:** The complex KornShell script was completely redesigned into a modular Python application.
    *   **Rationale:** KornShell is a legacy technology with limited maintainability, scalability, and integration capabilities in a cloud-native environment. Python offers a modern, highly readable, and extensible language with rich libraries for data processing, cloud interaction, and SFTP. This significantly improves the maintainability and future extensibility of the export logic.
    *   **Trade-offs:** Requires a complete rewrite and thorough testing to ensure functional equivalence.

## 4. Manual Steps Before Go-Live

The following manual steps must be completed before the migrated EXIS jobs can go live:

1.  **GCP Project Setup:**
    *   Confirm or create the target GCP Project ID (e.g., `your-gcp-project-id`).
    *   Enable necessary APIs: BigQuery API, Cloud Storage API, Cloud Composer API, Secret Manager API.

2.  **BigQuery Schema/Dataset Creation:**
    *   Ensure the `dwh_raw_layer` dataset exists within `your-gcp-project-id`.
    *   Verify that all source Oracle tables (`RPT$TA_S_D1_VERTRAG`, `SOF$TA_BPR_OPTIONEN`, `SOF$VI_L_OPTIONZUORDNUNG`, `DWH$VI_L_MAP_FA_TARIF`, `BL_D_TARIF`, `DWH$VI_C_VERTRAG`, `DWH$TA_F_NNV_GPRS`, `DWH$VI_F_NNV_TVD_12_MONATE`, `DWH$VI_L_TVD_LEISTUNGSKLASSE`, `RPT$TA_S_D1_DISCOUNT_RR`) have been successfully ingested into corresponding tables within the `your-gcp-project-id.dwh_raw_layer` dataset. This is a critical prerequisite.

3.  **GCS Bucket Creation:**
    *   Create the GCS bucket named `your-exis-exports-bucket` (or the chosen name) for storing exported files and archives.
    *   Ensure appropriate lifecycle policies are configured for the bucket (e.g., for data retention, archival, or deletion).

4.  **IAM & Permissions:**
    *   **Airflow Service Account:** The service account associated with your Cloud Composer environment (Airflow) must have:
        *   `BigQuery Data Editor` or `BigQuery User` role to run queries.
        *   `Storage Object Admin` or `Storage Object Creator` role for `your-exis-exports-bucket` to upload and manage files.
        *   `Secret Manager Secret Accessor` role to retrieve SFTP credentials.
    *   **BigQuery Service Account (if different):** If BigQuery operations are performed by a separate service account, ensure it has the necessary permissions.

5.  **Connection Strings & Secrets:**
    *   **SFTP Credentials:** Store the SFTP host, username, password, and port for each external target system in Google Secret Manager.
        *   The DAGs currently retrieve these from environment variables (`EXIS_SFTP_HOST`, `EXIS_SFTP_USER`, `EXIS_SFTP_PASSWORD`, `EXIS_SFTP_PORT`). These environment variables should be set in the Airflow environment, ideally by pulling from Secret Manager using an Airflow Secret Backend or by manually configuring them in the Composer environment variables.
        *   **Crucially, replace placeholder SFTP credentials in the DAGs (`your-sftp-host.example.com`, `exis_sftp_user`, `your-sftp-password-nna-data`, etc.) with actual values or references to Secret Manager.**
    *   **GCP Project ID:** Update `your-gcp-project-id` placeholders in the SQL files and DAGs with the actual GCP Project ID.

6.  **Airflow Environment Setup:**
    *   **`exis_exporter.py` deployment:** Ensure `exis_exporter.py` is deployed to a location accessible by the Airflow worker pods. This typically means placing it in the DAGs folder or a designated `plugins` folder, or packaging it as a Python library and installing it in the Composer environment.
    *   **Python Libraries:** Ensure `paramiko` is installed in the Cloud Composer environment. This can be done via the Composer environment configuration (PyPI packages).

7.  **Scheduling Confirmation:**
    *   **`schedule_interval`:** Review and confirm the `schedule_interval` for each DAG (`@monthly` for NNA Data/Voice, `@daily` for Bestands/Rabatt) with business stakeholders to ensure it matches the original UC4 scheduling requirements. The `MONATS_ID` parameter in NNA jobs suggests monthly execution.

8.  **SFTP Target Paths:**
    *   Confirm the exact remote SFTP directory paths (`/exis/sftp/out/nna_data`, `/exis/sftp/out/nna_voice`, `/exis/sftp/out/bestandsdaten`, `/exis/sftp/out/rabattdaten`) with the external system owners.

## 5. Known Gaps & Unresolved References

The following items were identified as gaps or require further follow-up:

1.  **Undeclared Variables in `r_exis_v2`:**
    *   The original `r_exis_v2` shell script used numerous variables (e.g., `DW_DIR_ROOT`, `DW_ORAUSER`, `LogDatei`, `SEPARATOR`, `SYSDATE`, `FROM`, `TO`, `SQLENGINE`, `SQLSPLIT`, `FILE_PARTITION`, `JOBID`, `CODEPAGE`, SFTP/SCP/MAIL credentials) whose exact origin and definition were not fully determined from the provided source inventory.
    *   **Follow-up:** Further analysis with source system owners or by reviewing environment configuration scripts is required to identify how these variables are set and used. They have been mapped to Airflow parameters or environment variables in the generated code, but their precise values and derivation logic need confirmation.

2.  **UC4 Workflow (JOBP/EVNT_TIME):**
    *   The absence of `JOBP` (workflow definition) and `EVNT_TIME` (scheduling) files means that the higher-level orchestration and scheduling dependencies between the four export jobs (and potentially other UC4 jobs) are currently unknown.
    *   **Follow-up:** Consult UC4 system administrators or business users to understand the full workflow and scheduling requirements. A master Airflow DAG might be needed to orchestrate these four individual export DAGs, or they may run independently as currently designed. The `schedule_interval` in the DAGs is a best guess and needs business confirmation.

3.  **SFTP Credential Management:**
    *   While `paramiko` is used for SFTP, the secure management of credentials (host, user, password) is critical. The generated DAGs use environment variables as placeholders.
    *   **Follow-up:** Implement a robust solution using Google Secret Manager to store and retrieve SFTP credentials securely within the Airflow environment. This involves configuring an Airflow Secret Backend or explicitly fetching secrets within the PythonOperator.

4.  **Oracle Source Data Availability:**
    *   The successful migration is entirely dependent on the Oracle source data being fully and accurately ingested into BigQuery.
    *   **Follow-up:** Establish robust data validation and reconciliation processes between Oracle and BigQuery after initial data migration to ensure data integrity and completeness.

5.  **Performance Tuning:**
    *   Oracle queries with `/*+ parallel(...)*/` hints were removed during translation. While BigQuery has automatic optimization, performance characteristics might differ.
    *   **Follow-up:** Monitor BigQuery query performance post-migration. Be prepared for manual tuning (e.g., partitioning, clustering, optimal join strategies) if performance issues arise.

6.  **Custom Shell Libraries:**
    *   The original `r_exis_v2` script likely relied on custom shell libraries like `.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parser.ksh`, etc. While core logic was re-implemented in `exis_exporter.py`, any remaining specific functionalities (e.g., advanced error reporting, specific date calculations) might need further review.
    *   **Follow-up:** Verify that all essential functionalities from these helper scripts have been adequately replaced or are no longer required in the GCP context.

## 6. Validation

Validation involves ensuring that the migrated EXIS jobs produce the correct output files with the expected data and are distributed successfully.

1.  **Data Ingestion Validation (Pre-requisite):**
    *   **Method:** Before running the EXIS DAGs, perform a data validation check to ensure that the Oracle source tables have been fully and accurately ingested into their corresponding BigQuery `dwh_raw_layer` tables. This can involve row counts, checksums, and sample data comparisons.
    *   **Passing Criteria:** All required Oracle tables are present in BigQuery, and their data matches the source system within acceptable tolerances.

2.  **Individual SQL Query Validation:**
    *   **Method:** Manually execute each `bq_d_exis_apt_*.sql` query in the BigQuery console, providing appropriate parameter values (e.g., `FROM_YYYYMM`). Compare the results with the output of the original Oracle SQL queries run against the source data.
    *   **Passing Criteria:** The BigQuery query results match the Oracle query results in terms of row count, column values, and data types.

3.  **Airflow DAG Execution & Output Validation:**
    *   **Method:**
        *   **Manual Trigger:** Trigger each EXIS DAG (`exis_nna_data_export_dag`, `exis_nna_voice_export_dag`, `exis_bestands_export_dag`, `exis_rabatt_export_dag`) manually from the Airflow UI.
        *   **Log Review:** Monitor Airflow task logs for successful completion, BigQuery query execution details, GCS upload messages, and SFTP transfer confirmations.
        *   **GCS Verification:** Check the `your-exis-exports-bucket` in GCS for the presence of the generated `.csv.gz` files in their respective archive paths (e.g., `exis_exports/EXIS_SD_APT_NNA_DATA/archive/`).
        *   **File Content Verification:** Download a sample `.csv.gz` file from GCS, decompress it, and inspect its content.
            *   Verify the CSV header and data rows.
            *   Verify the appended footer line (e.g., `X|<DESTINATION_FILE>|<FROM YYYYMMDD>|NR|V_F_NNA_Daten|<SYSDATE YYYYMMDD>`) is correctly formatted and contains accurate record counts.
            *   Compare the data content with historical output files from the original system or with data extracted directly from BigQuery.
        *   **SFTP Verification:** Confirm with the external target system owners that the files were successfully received on their SFTP server at the expected remote paths.
    *   **Passing Criteria:**
        *   All Airflow tasks complete successfully without errors.
        *   Gzipped CSV files are generated in the correct GCS paths.
        *   File names, content, and footer match the expected format and data from the original system.
        *   Files are successfully transferred to the external SFTP target.
        *   Data integrity checks (e.g., row counts, specific value checks) pass between source (BigQuery) and target (exported CSV).

## 7. Rollback Procedure

In case of critical issues or unexpected behavior after go-live, the following rollback procedure can be initiated:

1.  **Disable New Airflow DAGs:**
    *   Immediately disable all four migrated EXIS Airflow DAGs (`exis_nna_data_export_dag`, `exis_nna_voice_export_dag`, `exis_bestands_export_dag`, `exis_rabatt_export_dag`) in the Airflow UI. This will prevent any further execution of the new jobs.

2.  **Re-enable Original UC4 Jobs:**
    *   Contact the UC4 administrators to re-enable the original EXIS jobs (`DW.DWH_EXIS_SD_APT_NNA_DATA`, `DW.DWH_EXIS_SD_APT_NNA_VOIC`, `DW.DWH_EXIS_SD_APT_BESTANDS`, `DW.DWH_EXIS_SD_APT_RABATT`) in the legacy environment.
    *   Ensure their original schedules and dependencies are restored.

3.  **Verify Legacy System Operation:**
    *   Monitor the re-enabled UC4 jobs to confirm they are running as expected and producing output files to the original SFTP targets.
    *   Verify with external stakeholders that they are receiving the expected files from the legacy system.

4.  **Clean Up (Optional, if necessary):**
    *   If any erroneous files were generated and sent to external systems by the new GCP jobs, coordinate with the external teams for their removal or correction.
    *   Remove any partial or incorrect files from the GCS `exis_exports` bucket that were generated by the failed GCP jobs.

5.  **Root Cause Analysis & Remediation:**
    *   Once the legacy system is operational, conduct a thorough root cause analysis of the issues encountered with the migrated jobs.
    *   Implement necessary fixes, re-test in a staging environment, and plan for a re-deployment.