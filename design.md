# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_vertrag_tmp.ksh

## 1. Purpose & Scope
This shell script acts as a control script for data preparation within the `isbert` data warehouse. Its primary purpose is to orchestrate the execution of an SQL script (`d_ausd_v_ta_vertrag_tmp.sql`) that operates on the `ta_vertrag_tmp` table. The script handles environment setup, parameter parsing, basic error management, and ensures that active jobs are ignored or deactivated as necessary.

The business purpose is to prepare data related to contracts (`ta_vertrag_tmp`) as part of a larger data processing workflow. The script is invoked by an upstream job (likely a UC4 job orchestrating `r_ausd_v_ta_vertrag_tmp.ksh`, which in turn invokes this script).

## 2. Source Inventory
The job is primarily composed of one KornShell script:

*   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_vertrag_tmp.ksh`
    *   **Technology:** KornShell (Shell Script)
    *   **Summary:** Orchestrates an SQL script for `ta_vertrag_tmp` data preparation, handles parameters, and manages job status.
    *   **Complexity Tier:** `medium`
    *   **Migration Bucket:** `retire`
    *   **Key functions:** Parameter parsing (`getopts`), environment sourcing (`. $HOME/.dw_init`), error handling (`f_alis_msgerr.ksh`, `DWMSG_MeldeFehler`), date utilities (`h_alis_date.ksh`), parameter utilities (`h_alis_parameter.ksh`), SQL execution utilities (`h_alis_sqlplus.ksh`), and execution of `d_ausd_v_ta_vertrag_tmp.sql`.

## 3. Target Architecture
The target platform for this migration is Google BigQuery.

*   **Orchestration:** The control logic of the KornShell script will be re-implemented as a BigQuery Stored Procedure. This stored procedure will handle parameter validation, job status management, and the invocation of the core data transformation logic.
*   **Data Transformation:** The SQL logic currently within `d_ausd_v_ta_vertrag_tmp.sql` will be migrated into the BigQuery Stored Procedure, potentially using `EXECUTE IMMEDIATE` for dynamic SQL, or as direct DML statements within the procedure body.
*   **Data Storage:** All source tables (`DWTK_MELDUNGEN`, `SOF$TA_CNTRCT_CRS3`) and target tables (`SOF$TA_VERTRAG_TMP`, `VIA`) will be migrated to BigQuery tables.
*   **Job Management & Logging:** Dedicated BigQuery tables will be created for job tracking (to replace implicit job table updates and the "active jobs" logic), error logging (replacing `f_alis_msgerr.ksh` and `DWMSG_MeldeFehler`), and result logging (to store record counts, replacing temporary files).
*   **External Orchestration (Optional):** If the upstream invocation (`r_ausd_v_ta_vertrag_tmp.ksh` or its UC4 parent) has complex scheduling or inter-job dependencies that cannot be directly translated to BigQuery Stored Procedure scheduling, an external orchestrator like Cloud Composer (Airflow) could be used to trigger the BigQuery Stored Procedure. However, for the scope of this single job, the focus is on the stored procedure.

## 4. Data Flow & Lineage
The current legacy data flow is as follows:

1.  **UC4 Job / `r_ausd_v_ta_vertrag_tmp.ksh` (Upstream)**: An external job, `DW.BERT_AUSD_V_TA_VERTRAG_TMP.xml`, invokes `SCRIPT:R_AUSD_V_TA_VERTRAG_TMP.KSH`. This script, in turn, `INVOKES` our seed script `SCRIPT:K_AUSD_V_TA_VERTRAG_TMP.KSH`.
2.  **`k_ausd_v_ta_vertrag_tmp.ksh` (Current Job)**:
    *   **Initialization:** Sources several utility KornShell scripts for environment setup, error handling, date functions, parameter parsing, and SQL*Plus interaction.
    *   **Parameter Processing:** Reads `p_JobKennung` and `p_EintragsNr` via `getopts`.
    *   **Job Status Management:** Contains logic to ignore active jobs and deactivate old active jobs (implied by the `starteSQLSkript` function and summary).
    *   **SQL Execution:** `EXECUTES_SQL` `d_ausd_v_ta_vertrag_tmp.sql` using a wrapper function `starteSQLSkript`.
    *   **Record Count:** Reads a temporary file (`tmpFile`) to get the number of processed records.
3.  **`d_ausd_v_ta_vertrag_tmp.sql` (Core Logic)**:
    *   **Reads From:** `TABLE:DWTK_MELDUNGEN`, `TABLE:SOF$TA_CNTRCT_CRS3`
    *   **Writes To:** `TABLE:SOF$TA_VERTRAG_TMP`, `TABLE:VIA`

**Target BigQuery Data Flow:**

1.  **External Trigger (e.g., Cloud Composer / Scheduled Query / Upstream BigQuery Job):** Triggers the BigQuery Stored Procedure.
2.  **BigQuery Stored Procedure (`r_ausd_vertrag_control`):**
    *   Receives `p_JobKennung` and `p_EintragsNr` as input parameters.
    *   Performs parameter validation and error logging to a new `error_log` BigQuery table.
    *   Updates a `job_table` BigQuery table for job status management (deactivating old jobs, inserting new job entry).
    *   `EXECUTE IMMEDIATE` (or direct DML) the migrated SQL logic from `d_ausd_v_ta_vertrag_tmp.sql`. This logic will read from BigQuery tables `DWTK_MELDUNGEN` and `SOF$TA_CNTRCT_CRS3`, and write to `SOF$TA_VERTRAG_TMP` and `VIA`.
    *   Calculates record counts (e.g., `SELECT COUNT(*)`) and inserts this into a `job_result_log` BigQuery table.

## 5. Transformation Logic
The transformation logic from the KornShell script itself is primarily orchestrational. The data transformations reside in the SQL script it executes.

**KornShell Script (`k_ausd_v_ta_vertrag_tmp.ksh`) to BigQuery Stored Procedure:**

*   **Environment Variables (`$HOME`, `$BERT_DIR_ROOT`, `$DW_DIR_UTL`):** Replaced by BigQuery Stored Procedure parameters, session variables, or environment variables/configuration specific to the orchestration layer (e.g., Cloud Composer environment variables, BigQuery project/dataset settings).
*   **Parameter Parsing (`getopts`):** Replaced by BigQuery Stored Procedure input parameters (`p_JobKennung`, `p_EintragsNr`).
*   **Parameter Validation (`pruefeParameterGesetzt`):** Replaced by `IF/THEN/END IF` blocks within the BigQuery Stored Procedure, using `IS NULL` or empty string checks.
*   **Error Handling (`f_alis_msgerr.ksh`, `DWMSG_MeldeFehler`):** Replaced by `RAISE USING MESSAGE` for immediate errors and `INSERT` statements into a dedicated BigQuery `error_log` table for persistent logging.
*   **Job Management (implicit in `starteSQLSkript`, deactivation logic):** Replaced by `UPDATE` and `INSERT` statements against a BigQuery `job_table` for tracking active/deactivated jobs and their status.
*   **SQL Execution (`starteSQLSkript`):** The functionality of `starteSQLSkript` will be absorbed by the BigQuery Stored Procedure. The actual SQL from `d_ausd_v_ta_vertrag_tmp.sql` will either be inlined into the stored procedure or dynamically executed via `EXECUTE IMMEDIATE`.
*   **Temporary File (`tmpFile`) for Record Count:** Replaced by `SELECT COUNT(*)` on the target table after population, with the result stored in a BigQuery variable and subsequently logged into a `job_result_log` table.

**SQL Script (`d_ausd_v_ta_vertrag_tmp.sql`) to BigQuery SQL:**

*   The content of `d_ausd_v_ta_vertrag_tmp.sql` will need to be translated from its current SQL dialect (likely Oracle PL/SQL, given the `sqlplus` helper and table naming conventions) to BigQuery Standard SQL.
*   This will involve:
    *   Converting data types.
    *   Adjusting function syntax (e.g., date functions, string manipulation).
    *   Refactoring any procedural elements into BigQuery Scripting language within the stored procedure.
    *   Ensuring table references are updated to their BigQuery equivalents (e.g., `schema.table` or `project.dataset.table`).

## 6. External Dependencies
*   **Legacy Databases:** The script interacts with tables that are presumed to be in an Oracle database, given the `sqlplus` utility. These tables (`DWTK_MELDUNGEN`, `SOF$TA_CNTRCT_CRS3`, `SOF$TA_VERTRAG_TMP`, `VIA`) will be migrated to BigQuery tables. The read/write operations will then occur entirely within BigQuery.
*   **Legacy Utility Scripts:**
    *   `f_alis_msgerr.ksh`: Error handling and messaging. This functionality will be replaced by BigQuery's `RAISE` and logging to a BigQuery `error_log` table.
    *   `h_alis_date.ksh`: Date utilities. BigQuery's rich set of date and time functions will replace this.
    *   `h_alis_parameter.ksh`: Parameter parsing utilities. Replaced by BigQuery Stored Procedure input parameters and validation logic.
    *   `h_alis_sqlplus.ksh`: SQL*Plus wrapper. This is no longer needed as the SQL execution will be native to BigQuery.
*   **Temporary Files:** The use of `tmpFile` for record counts will be eliminated, replaced by BigQuery variables and logging tables.

No other external systems (e.g., SFTP, S3) or unresolved targets were identified by the lineage analysis.

## 7. Unresolved / Risks
*   **SQL Logic Translation:** The actual SQL code within `d_ausd_v_ta_vertrag_tmp.sql` is not provided. A detailed analysis of this file is critical to ensure accurate translation to BigQuery Standard SQL, especially concerning complex queries, stored procedures, or specific Oracle functions.
*   **Full Functionality of Sourced Scripts:** The exact implementations of `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, and `h_alis_sqlplus.ksh` are not known. While general BigQuery equivalents are proposed, any intricate business logic embedded within these scripts must be carefully extracted and reimplemented.
*   **`starteSQLSkript` Wrapper Logic:** The `starteSQLSkript` function within `h_alis_sqlplus.ksh` is assumed to handle job registration and active job management. The precise logic here needs to be understood to faithfully migrate the job control aspects to BigQuery.
*   **"Retire" Migration Bucket:** The job is categorized in the `retire` migration bucket. This implies that decommissioning or a significant redesign might be preferred over a direct "lift and shift" migration. It's a key decision point for the project: confirm if retirement is viable, or if specific functionalities still need to be ported. If retired, this design document becomes a reference for understanding the legacy process. Assuming migration is still required, this design focuses on a BigQuery equivalent.
*   **Upstream Invocation:** The upstream `r_ausd_v_ta_vertrag_tmp.ksh` and its UC4 orchestrator will also need to be migrated or reconfigured to call the new BigQuery Stored Procedure.

## 8. Build Plan
1.  **Define BigQuery Schema:**
    *   Create BigQuery datasets (e.g., `project.dataset`).
    *   Create target BigQuery tables: `SOF$TA_VERTRAG_TMP`, `VIA`.
    *   Create BigQuery tables for source data: `DWTK_MELDUNGEN`, `SOF$TA_CNTRCT_CRS3` (if not already existing as part of a broader data migration).
    *   Create auxiliary BigQuery tables:
        *   `project.dataset.job_table` (for job status and active job management).
        *   `project.dataset.error_log` (for logging errors).
        *   `project.dataset.job_result_log` (for logging processed record counts).
2.  **Migrate SQL Script:**
    *   Translate the content of `d_ausd_v_ta_vertrag_tmp.sql` to BigQuery Standard SQL, adapting data types, functions, and query syntax.
3.  **Develop BigQuery Stored Procedure (orchestration logic):**
    *   Create a BigQuery Stored Procedure (e.g., `CREATE OR REPLACE PROCEDURE project.dataset.r_ausd_vertrag_control(...)`).
    *   Implement parameter handling using procedure input arguments.
    *   Implement parameter validation logic.
    *   Implement job status update logic for `job_table`.
    *   Integrate the migrated BigQuery SQL from `d_ausd_v_ta_vertrag_tmp.sql` into the stored procedure.
    *   Implement record count extraction and logging to `job_result_log`.
    *   Implement error handling and logging to `error_log` table.
4.  **Testing:**
    *   Unit test the BigQuery Stored Procedure with various parameter inputs and data scenarios.
    *   Perform integration testing to ensure correct data flow and results matching the legacy system.
5.  **Deployment & Scheduling:**
    *   Deploy the BigQuery tables and the BigQuery Stored Procedure.
    *   Configure a scheduler (e.g., Cloud Composer, Scheduled Query in BigQuery) to trigger the stored procedure with necessary parameters, replacing the original UC4/KornShell invocation.

**Build Artefacts:**
*   BigQuery DDL scripts for all tables.
*   BigQuery Stored Procedure SQL script (`r_ausd_vertrag_control.sql`).
*   (Optional) Cloud Composer DAG / Workflows YAML for external orchestration.
*   (Optional) Data migration scripts for initial load of `DWTK_MELDUNGEN` and `SOF$TA_CNTRCT_CRS3` if they don't exist in BigQuery already.