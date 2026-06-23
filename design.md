# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_p_discount.ksh

## 1. Purpose & Scope

This job, `k_ausd_v_ta_p_discount.ksh`, is a KornShell control script responsible for orchestrating a data preparation workflow. Its primary purpose is to invoke a SQL script, `d_ausd_v_ta_p_discount.sql`, for data processing related to `ta_p_discount`. It handles job parameter parsing (`p_JobKennung`, `p_EintragsNr`), performs basic parameter validation, and manages job state (ignoring active jobs and deactivating older ones) via an underlying SQL layer or helper routines. After executing the SQL script, it reads the number of processed records from a temporary file.

The scope of this migration is to re-implement this orchestration logic and the underlying SQL processing in Google Cloud's BigQuery environment, leveraging BigQuery SQL and stored procedures, potentially with Cloud Composer for external orchestration.

## 2. Source Inventory

The primary component of this job is a single KornShell script.

*   **File Name**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_p_discount.ksh`
*   **Technology**: KornShell (shell script)
*   **Summary**: KornShell script to control and execute a SQL script (d_ausd_v_ta_p_discount.sql) for data preparation, handling job parameters and error logging.
*   **Category**: shell
*   **Tool**: KornShell
*   **File Purpose**: Orchestration/ETL control.
*   **Complexity Tier**: (Not available from file_complexity, but inferred as 'medium' due to `semi_auto` migration bucket and script logic)
*   **Migration Flags**: (Not available from file_complexity)
*   **Automation Bucket**: `semi_auto` (B2)

## 3. Target Architecture

The target platform is BigQuery. The architecture will consist of:

*   **BigQuery Stored Procedures**: The core orchestration logic of the KornShell script, including parameter validation, job state management, and the invocation of the primary SQL logic, will be migrated into a BigQuery Stored Procedure. This SP will replace the shell script's control flow.
*   **BigQuery Tables**:
    *   **`job_table`**: A control table to manage job identifiers (`job_kennung`), entry numbers (`eintragsnr`), and their active status, replacing the implicit job management in the legacy system.
    *   **`job_run_log`**: A logging table to store execution details, including `job_kennung`, `eintragsnr`, `tab_name`, and `records_count`, replacing the temporary file mechanism for record counting and existing logging.
    *   **`ta_p_discount`**: The target table where the SQL script's output will be stored or transformed.
*   **BigQuery SQL Script/Stored Procedure for `d_ausd_v_ta_p_discount.sql`**: The actual data transformation logic currently residing in `d_ausd_v_ta_p_discount.sql` will be migrated into a separate BigQuery Stored Procedure or a multi-statement SQL script. This will be invoked by the main control stored procedure.
*   **Orchestration (Optional/External)**: For scheduling and external parameter passing, Cloud Composer (Apache Airflow) or BigQuery Scheduled Queries can be used to invoke the top-level BigQuery Stored Procedure.

## 4. Data Flow & Lineage

The migrated data flow will be as follows:

1.  **External Trigger**: A scheduled job (e.g., Cloud Composer DAG, BigQuery Scheduled Query) initiates the BigQuery control stored procedure (`dataset.r_ausd_vertrag_control`).
2.  **Parameter Passing**: The external trigger passes `p_JobKennung` and `p_EintragsNr` as parameters to the BigQuery stored procedure.
3.  **Control Stored Procedure (`dataset.r_ausd_vertrag_control`)**:
    *   Validates input parameters.
    *   Updates the `dataset.job_table` to manage job activation/deactivation.
    *   Invokes the data transformation stored procedure (`dataset.d_ausd_v_ta_p_discount`).
    *   Queries `dataset.ta_p_discount` to determine the number of processed records directly.
    *   Inserts job execution details and record counts into `dataset.job_run_log`.
4.  **Data Transformation Stored Procedure (`dataset.d_ausd_v_ta_p_discount`)**:
    *   Reads source data (details not available in this job's context, but likely from upstream tables or views).
    *   Performs transformations and loads/updates data into `dataset.ta_p_discount`.
5.  **Logging**: All steps will log relevant information to `dataset.job_run_log`.

**Legacy Dependencies:**
*   `k_ausd_v_ta_p_discount.ksh` **invokes** `d_ausd_v_ta_p_discount.sql`
*   `k_ausd_v_ta_p_discount.ksh` **uses** `ta_p_discount` (implicitly via `d_ausd_v_ta_p_discount.sql`)
*   `k_ausd_v_ta_p_discount.ksh` **sources** `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`

## 5. Transformation Logic

The KornShell script itself is primarily an orchestration wrapper. The actual transformation logic is within `d_ausd_v_ta_p_discount.sql`.

**KornShell Script Logic (to be migrated to BigQuery Stored Procedure `dataset.r_ausd_vertrag_control`):**

*   **Parameter Handling**: `getopts` for `j` (p_JobKennung) and `f` (p_EintragsNr) will be replaced by direct stored procedure parameters.
*   **Variable Initialization**: `v_TabName='ta_p_discount'` will be a `DECLARE`d variable in the SP.
*   **Parameter Validation**: Shell `if` conditions and `pruefeParameterGesetzt` calls will be replaced by BigQuery SQL `IF` statements and `ASSERT` or `RAISE` for error handling.
*   **Job Control**:
    *   Deactivating old active jobs: `UPDATE dataset.job_table SET active_flag = FALSE WHERE job_kennung = p_JobKennung AND active_flag = TRUE AND eintragsnr <> p_EintragsNr;`
    *   Registering/updating current job: `MERGE` statement on `dataset.job_table`.
*   **SQL Script Execution**: The `starteSQLSkript` call will be replaced by a `CALL dataset.d_ausd_v_ta_p_discount(p_EintragsNr, p_JobKennung);`
*   **Record Counting**: Reading from `tmpFile` will be replaced by `SELECT COUNT(*) FROM dataset.ta_p_discount WHERE eintragsnr = p_EintragsNr;`
*   **Logging**: Shell `echo` and `DWMSG_MeldeFehler` calls will be replaced by `INSERT` statements into `dataset.job_run_log` or BigQuery's native logging capabilities.

**SQL Script Logic (`d_ausd_v_ta_p_discount.sql` to be migrated to `dataset.d_ausd_v_ta_p_discount` BigQuery Stored Procedure):**

*   (Details of `d_ausd_v_ta_p_discount.sql` are not directly provided in this job's component, but it's assumed to contain the DML for preparing/transforming `ta_p_discount`.)
*   This SP will encapsulate the actual SELECT, INSERT, UPDATE, DELETE logic against `ta_p_discount` and any source tables.

## 6. External Dependencies

| Legacy External System / Dependency | Description                                                    | Replacement in BigQuery                                                                    |
| :---------------------------------- | :------------------------------------------------------------- | :----------------------------------------------------------------------------------------- |
| **Environment Initialization**      | `. $HOME/.dw_init`                                             | Configuration for the BigQuery environment (e.g., project, dataset, service account) will be managed via standard GCP mechanisms (IAM, `gcloud` settings, environment variables in Cloud Composer/Cloud Functions). |
| **Shell Helper Scripts**            | `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh` | These functionalities will be re-implemented directly in BigQuery Stored Procedures:       |
|                                     | `- Error Handling (`f_alis_msgerr.ksh`)`                       |   - `RAISE` statement for errors.                                                          |
|                                     |                                                                |   - `INSERT` into `job_run_log` for detailed error logging.                                |
|                                     | `- Date Handling (`h_alis_date.ksh`)`                          |   - BigQuery date/time functions (`CURRENT_DATE()`, `FORMAT_DATE()`, etc.).                 |
|                                     | `- Parameter Parsing (`h_alis_parameter.ksh`)`                 |   - Stored procedure input parameters.                                                     |
|                                     | `- SQL*Plus Routines (`h_alis_sqlplus.ksh`)`                   |   - Direct BigQuery SQL commands, `EXECUTE IMMEDIATE`, and other SP features.              |
| **Temporary File**                  | `$DW_DIR_UTL/bert_k_ausd_v_ta_p_discount_$$.tmp`              | Replaced by BigQuery Stored Procedure variables for record counts, or direct queries against target tables. |
| **Table `ta_p_discount`**           | Target table of the SQL processing.                            | A BigQuery table `dataset.ta_p_discount`.                                                 |
| **Environment Variables**           | `BERT_DIR_ROOT`, `DW_DIR_UTL`, `HOME`, `OPTARG`                | Replaced by BigQuery Stored Procedure parameters, dataset/project identifiers, or configuration passed via the orchestration layer. |

## 7. Unresolved / Risks

*   **Logic within `d_ausd_v_ta_p_discount.sql`**: The exact content of this SQL script is critical and needs detailed analysis to ensure accurate migration to BigQuery SQL. This document assumes it contains standard DML, but complex procedural SQL (if present) might require further breaking down or specific BigQuery features (e.g., scripting, UDFs, UDAFs).
*   **`starteSQLSkript` functionality**: The exact actions performed by the `starteSQLSkript` function (likely defined in `h_alis_sqlplus.ksh`) need to be fully understood. Beyond just executing SQL, it might handle connection management, transaction control, or job metadata updates that must be replicated in BigQuery. The current design assumes job table updates are part of the main control SP and the SQL script is a direct call.
*   **`pruefeParameterGesetzt` and `DWMSG_MeldeFehler`**: These are helper functions. Their precise logic, especially for `DWMSG_MeldeFehler`, needs to be reviewed to ensure equivalent error reporting and logging in the BigQuery environment.
*   **`semi_auto` Migration Bucket**: The `semi_auto` classification indicates that some manual intervention or careful design is needed. This likely pertains to translating the shell script's environment setup and specific utility calls into BigQuery-native or Python-orchestrated solutions.

## 8. Build Plan

1.  **Define BigQuery Schemas**:
    *   Create `dataset.job_table` DDL (including `job_kennung`, `eintragsnr`, `active_flag`, `created_at`, `updated_at`).
    *   Create `dataset.job_run_log` DDL (including `job_kennung`, `eintragsnr`, `tab_name`, `records_count`, `processed_at`, `error_message`).
    *   Define `dataset.ta_p_discount` DDL (schema based on `d_ausd_v_ta_p_discount.sql`'s output).
    *   **Language**: BigQuery DDL
2.  **Migrate `d_ausd_v_ta_p_discount.sql`**:
    *   Translate the SQL logic from `d_ausd_v_ta_p_discount.sql` into a BigQuery Stored Procedure: `dataset.d_ausd_v_ta_p_discount`. This SP will accept `p_EintragsNr` and `p_JobKennung` as parameters if needed by its internal logic.
    *   **Language**: BigQuery SQL (Stored Procedure)
3.  **Create Control Stored Procedure**:
    *   Develop `dataset.r_ausd_vertrag_control` BigQuery Stored Procedure (pseudocode provided in Section 5). This SP will encapsulate parameter validation, job table updates, invocation of `dataset.d_ausd_v_ta_p_discount`, and record count logging.
    *   **Language**: BigQuery SQL (Stored Procedure)
4.  **Develop Orchestration Layer**:
    *   Create a Cloud Composer (Airflow) DAG to schedule and invoke `dataset.r_ausd_vertrag_control`, passing the required parameters. Alternatively, configure a BigQuery Scheduled Query.
    *   **Language**: Python (for Airflow DAG) or YAML/JSON (for Cloud Workflows/BigQuery Scheduled Query setup).
5.  **Testing**:
    *   Unit tests for each BigQuery Stored Procedure.
    *   Integration tests for the complete workflow (orchestration -> control SP -> data transformation SP).
    *   Data validation to ensure output in `dataset.ta_p_discount` matches legacy system.
    *   **Language**: SQL, Python.