# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_apn_ve.ksh

## 1. Purpose & Scope
This shell script, `k_ausd_v_ta_apn_ve.ksh`, serves as an orchestration wrapper for a data loading process. Its primary purpose is to:
- Handle parameter parsing (job identifier `p_JobKennung` and entry number `p_EintragsNr`).
- Set up the environment by sourcing various utility scripts.
- Perform error checking and validation of input parameters.
- Orchestrate the execution of an SQL script, `d_ausd_v_ta_apn_ve.sql`, which populates the `ta_apn_ve` table.
- Manage job status by interacting with a job table.
- Ignore or deactivate active jobs as part of its control flow.

The script's core business logic is embedded within the invoked SQL script, while `k_ausd_v_ta_apn_ve.ksh` manages the execution flow and error handling.

## 2. Source Inventory

| File Path                                                       | Technology  | Tier   | Automation Bucket | Summary                                                                                                                                                                                                                                           |
|:----------------------------------------------------------------|:------------|:-------|:------------------|:--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_apn_ve.ksh` | KornShell | medium | semi_auto         | This ksh script acts as a control script for a data loading process. It handles parameter parsing, environment setup, error checking, and orchestrates the execution of an SQL script to populate the 'ta_apn_ve' table, managing job status. |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_apn_ve.sql` | SQL         | N/A    | N/A               | This SQL script is executed by the ksh wrapper. It reads from `DWTK_MELDUNGEN` and `PDS$TA_PDP_CONTEXT_ASSOC`, and writes to `SOF$TA_APN_VE` and `VIA`. It also utilizes the `DWPA_UTIL_SKRIPT` package. (Details to be confirmed upon SQL analysis). |
| Utility Scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`) | KornShell | N/A    | N/A               | These are generic helper scripts providing functionalities like error messaging, date handling, parameter parsing, and SQL*Plus interaction.                                                                                                    |

## 3. Target Architecture
The target platform is Google BigQuery. The migration will involve:
- **Orchestration:** The shell script's orchestration logic will be re-implemented as a BigQuery Stored Procedure. This stored procedure will handle parameter validation, job logging, and the invocation of the core SQL logic.
- **Data Transformation (SQL):** The `d_ausd_v_ta_apn_ve.sql` script will be translated into standard BigQuery SQL DML/DDL statements. This will likely involve a `MERGE` statement for the `SOF$TA_APN_VE` table, reflecting the source script's `merge via` hint.
- **Logging/Monitoring:** A dedicated BigQuery table, e.g., `project.dataset.job_table`, will be created to record job status, parameters, and processed record counts, replacing the temporary file and implicit job table interactions. An `error_log` table will capture error details.
- **Data Storage:**
    - Source tables (`DWTK_MELDUNGEN`, `PDS$TA_PDP_CONTEXT_ASSOC`) will be migrated to BigQuery tables, maintaining their schema and data.
    - Target tables (`SOF$TA_APN_VE`, `VIA`) will also be created or migrated in BigQuery. The `VIA` table name requires further investigation to confirm its full name and purpose.

## 4. Data Flow & Lineage

1.  **Start:** The BigQuery Stored Procedure `r_ausd_vertrag_control` (or similar name) is invoked with `p_JobKennung` and `p_EintragsNr`.
2.  **Parameter Validation:** The stored procedure validates the input parameters. If invalid, it logs an error to `project.dataset.error_log` and raises an exception.
3.  **Job Logging (Start):** The procedure records the job start time and parameters into `project.dataset.job_table`.
4.  **SQL Execution:** The core data transformation logic, migrated from `d_ausd_v_ta_apn_ve.sql`, is executed within the stored procedure.
    - **Reads:** Data from `project.dataset.DWTK_MELDUNGEN` and `project.dataset.PDS_TA_PDP_CONTEXT_ASSOC`.
    - **Transforms:** (Details dependent on `d_ausd_v_ta_apn_ve.sql` content).
    - **Writes:** Inserts/merges transformed data into `project.dataset.SOF_TA_APN_VE`.
    - **Writes:** Inserts/merges data into `project.dataset.VIA` (requires clarification).
5.  **Record Count:** The stored procedure calculates the number of processed records.
6.  **Job Logging (Completion):** The procedure updates the `project.dataset.job_table` with the completion status, processed record count, and end time.
7.  **End:** The stored procedure completes execution.

## 5. Transformation Logic

**`k_ausd_v_ta_apn_ve.ksh` (Orchestration Layer):**
- **Original Logic:** Shell script sourcing environment and utility scripts, parsing `getopts` parameters (`-j`, `-f`), validating presence of parameters, calling `starteSQLSkript` with an SQL file path, and reading a record count from a temporary file. Error handling involves `DWMSG_MeldeFehler` and exiting with error codes.
- **Target BigQuery Stored Procedure Logic (`project.dataset.r_ausd_vertrag_control`):**
    - **Parameter Handling:** `p_JobKennung STRING`, `p_EintragsNr STRING` will be explicit stored procedure parameters.
    - **Environment/Utility Sourcing:** Replaced by explicit BigQuery `DECLARE` statements for variables, or by defining constants/configuration values. Utility script logic (like date handling, parameter validation functions) will be inlined or re-implemented directly using BigQuery SQL functions and `IF` statements.
    - **Parameter Validation:** `IF param IS NULL OR param = '' THEN ...` logic.
    - **Error Handling:** `RAISE USING MESSAGE` for immediate termination or `INSERT INTO project.dataset.error_log` for logging non-fatal issues.
    - **SQL Script Execution:** The content of `d_ausd_v_ta_apn_ve.sql` will be embedded directly or called via `EXECUTE IMMEDIATE` within the stored procedure.
    - **Record Count:** `SELECT COUNT(*)` on the target table (or a temporary table within the procedure) will replace reading from `tmpFile`.
    - **Job Status:** `INSERT`/`UPDATE` statements on `project.dataset.job_table`.

**`d_ausd_v_ta_apn_ve.sql` (Data Logic Layer):**
- **Original Logic:** An Oracle SQL script that reads from `isbert_schema.dwtk_meldungen` and `pds$ta_pdp_context_assoc`, performs a merge operation on `sof$ta_apn_ve` and `via`, and uses the `DWPA_UTIL_SKRIPT` package.
- **Target BigQuery SQL Logic:**
    - **Schema/Table Naming:** `isbert_schema.dwtk_meldungen` -> `project.dataset.DWTK_MELDUNGEN`. `pds$ta_pdp_context_assoc` -> `project.dataset.PDS_TA_PDP_CONTEXT_ASSOC`. `sof$ta_apn_ve` -> `project.dataset.SOF_TA_APN_VE`. `VIA` -> `project.dataset.VIA` (name confirmation needed).
    - **SQL Syntax Conversion:** Convert Oracle-specific SQL (e.g., `MERGE` syntax, data type functions, date functions) to BigQuery standard SQL.
    - **Package `DWPA_UTIL_SKRIPT`:** Any functions or procedures used from this Oracle package will need to be re-implemented as BigQuery UDFs or part of the stored procedure logic.
    - **Implicit logic:** Ensure that the specific `starteSQLSkript` parameters for `p_EintragsNr` and `p_JobKennung` are correctly passed and utilized within the translated SQL logic, likely as part of `WHERE` clauses or dynamic SQL.

## 6. External Dependencies

| Original External Dependency | Type            | How it's Used                                                    | Migration Strategy                                                                                                                                                                                                                                          |
|:-----------------------------|:----------------|:-----------------------------------------------------------------|:--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `d_ausd_v_ta_apn_ve.sql`     | SQL Script File | Executed by `k_ausd_v_ta_apn_ve.ksh` to perform data operations. | The SQL content will be fully translated into BigQuery Standard SQL and either inlined into the BigQuery Stored Procedure or exist as a separate callable script within a managed orchestration tool (e.g., Cloud Composer DAG).                             |
| Oracle Database Tables       | Database        | `DWTK_MELDUNGEN`, `PDS$TA_PDP_CONTEXT_ASSOC` (read); `SOF$TA_APN_VE`, `VIA` (write). | These tables will be migrated to BigQuery tables. Data will be ingested into BigQuery using appropriate migration tools (e.g., BigQuery Data Transfer Service, custom ETL). Schema translation will also be performed.                               |
| `DWPA_UTIL_SKRIPT`           | Oracle Package  | Used by `d_ausd_v_ta_apn_ve.sql`.                                | Functions or procedures within this package that are used will need to be re-implemented in BigQuery as User-Defined Functions (UDFs) or as part of the main stored procedure logic. This requires understanding the specific functionalities used. |
| Unix Environment (`$HOME`, `$BERT_DIR_ROOT`, `$DW_DIR_UTL`) | Environment Variables | Used for path resolution and temporary file location.                                          | Replaced by explicit project/dataset/table references, BigQuery Stored Procedure parameters, or configuration parameters managed by the orchestration platform (e.g., environment variables in Cloud Composer).                                      |
| Utility Shell Scripts (`f_alis_msgerr.ksh`, etc.) | Shell Scripts   | Sourced for common shell functionalities.                        | Their functionalities (error logging, date functions, parameter handling) will be reimplemented using BigQuery SQL features directly within the stored procedure.                                                                             |
| Temporary File (`$DW_DIR_UTL/...tmp`) | File System     | Used to communicate record count between shell and potentially SQL. | Replaced by BigQuery Stored Procedure variables (`DECLARE`) or by direct `SELECT COUNT(*)` assignments within the procedure.                                                                                                           |

## 7. Unresolved / Risks

-   **`VIA` Table Identification:** The lineage indicates a `WRITES_TABLE` to `TABLE:VIA`. This is a very generic name and might be an alias or an incomplete capture. Its full name, schema, and purpose need to be clarified to ensure correct migration.
-   **`d_ausd_v_ta_apn_ve.sql` Details:** The exact SQL code of `d_ausd_v_ta_apn_ve.sql` is not available in this analysis. A detailed examination of this SQL script is crucial for accurate BigQuery translation, especially for Oracle-specific syntax, functions, and any complex logic.
-   **`DWPA_UTIL_SKRIPT` Package Functionality:** The specific functions or procedures from `DWPA_UTIL_SKRIPT` used by `d_ausd_v_ta_apn_ve.sql` are unknown. This package needs to be analyzed to understand the required re-implementation in BigQuery.
-   **Error Handling Framework (`DWMSG_MeldeFehler`):** The existing custom error handling needs to be fully understood to replicate its logging and notification mechanisms in BigQuery. This might involve building a dedicated error logging table and integrating with BigQuery logging/monitoring services.
-   **Job Control Table:** The structure and content of the "job table" mentioned in the script's purpose are not explicitly defined. A BigQuery equivalent needs to be designed to track job execution status and history.
-   **Parameter Sensitivity:** If `p_JobKennung` or `p_EintragsNr` are sensitive, appropriate security measures (e.g., BigQuery authorized views, fine-grained access control) must be implemented.

## 8. Build Plan

1.  **Define Target Schemas & Tables:**
    *   Create BigQuery datasets (e.g., `project.dataset`).
    *   Define DDL for `project.dataset.DWTK_MELDUNGEN`.
    *   Define DDL for `project.dataset.PDS_TA_PDP_CONTEXT_ASSOC`.
    *   Define DDL for `project.dataset.SOF_TA_APN_VE`.
    *   Define DDL for `project.dataset.VIA` (once fully identified).
    *   Define DDL for `project.dataset.job_table` (job control).
    *   Define DDL for `project.dataset.error_log` (error logging).
    (Language: BigQuery DDL)

2.  **Migrate Source Data:**
    *   Ingest data from Oracle `DWTK_MELDUNGEN` to BigQuery `project.dataset.DWTK_MELDUNGEN`.
    *   Ingest data from Oracle `PDS$TA_PDP_CONTEXT_ASSOC` to BigQuery `project.dataset.PDS_TA_PDP_CONTEXT_ASSOC`.
    (Language: BigQuery Data Transfer Service configuration or custom ETL/ELT scripts)

3.  **Translate `d_ausd_v_ta_apn_ve.sql` to BigQuery SQL:**
    *   Convert Oracle SQL syntax to BigQuery Standard SQL.
    *   Implement logic from `DWPA_UTIL_SKRIPT` as BigQuery UDFs or inline code.
    *   Create a BigQuery Stored Procedure or a standalone SQL script (e.g., `bq_d_ausd_v_ta_apn_ve.sql`) for this logic.
    (Language: BigQuery SQL)

4.  **Develop BigQuery Orchestration Stored Procedure:**
    *   Create `project.dataset.r_ausd_vertrag_control` stored procedure.
    *   Implement parameter validation logic.
    *   Integrate job start/end logging into `project.dataset.job_table`.
    *   Integrate error logging into `project.dataset.error_log`.
    *   Call or embed the translated `bq_d_ausd_v_ta_apn_ve.sql` logic.
    *   Implement record count logic.
    (Language: BigQuery SQL Stored Procedure)

5.  **External Orchestration (Optional):**
    *   If `k_ausd_v_ta_apn_ve.ksh` is part of a larger workflow, consider creating a Cloud Composer (Airflow) DAG to trigger `project.dataset.r_ausd_vertrag_control` and manage its dependencies.
    (Language: Python for Airflow DAG)

6.  **Testing and Validation:**
    *   Unit test each component (SQL script, stored procedure).
    *   Integration test the full data flow.
    *   Perform data validation comparing source and target data.
    (Language: SQL, Python)

