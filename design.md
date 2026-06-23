# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_p_discount.ksh

## 1. Purpose & Scope
This migration targets the KornShell script `k_ausd_v_ta_p_discount.ksh`, which acts as a control script for a data preparation process. Its primary purpose is to:
- Control the execution of an underlying SQL script (`d_ausd_v_ta_p_discount.sql`).
- Handle job parameters (`JobKennung` and `EintragsNr`).
- Implement error logging and validation.
- Potentially manage job status (ignoring active jobs, deactivating older ones, and registering job execution).
- Retrieve and report the number of processed records.

The scope of this migration is to re-platform this control logic and its dependent SQL execution to Google BigQuery, specifically leveraging BigQuery Stored Procedures and associated BigQuery SQL for the data transformation.

## 2. Source Inventory
| File Name                                                         | Technology | Complexity Tier | Automation Bucket | Summary                                                                                                                                                                             |
| :---------------------------------------------------------------- | :--------- | :-------------- | :---------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_p_discount.ksh` | KornShell  | medium          | semi_auto         | KornShell script to control and execute a SQL script (d_ausd_v_ta_p_discount.sql) for data preparation, handling job parameters and error logging. |

## 3. Target Architecture
The target architecture in BigQuery will involve:
- **BigQuery Stored Procedure:** The `k_ausd_v_ta_p_discount.ksh` control script will be migrated to a BigQuery Stored Procedure, acting as the orchestrator. This procedure will accept `JobKennung` and `EintragsNr` as input parameters.
- **BigQuery SQL for Data Transformation:** The logic within the `d_ausd_v_ta_p_discount.sql` file (which is invoked by the shell script) will be migrated into a separate BigQuery SQL script or another BigQuery Stored Procedure.
- **Job Control/Logging Tables:** Dedicated BigQuery tables will be created for `error_log` and `job_log` to replace the shell script's internal error handling and job status management.
- **Target Data Table:** The `ta_p_discount` table, which is the subject of the data preparation, will reside in BigQuery.
- **Orchestration (Optional but Recommended):** For scheduling and managing the execution of the BigQuery Stored Procedure, a Cloud Composer DAG, Workflows, or BigQuery Scheduled Queries can be used.

## 4. Data Flow & Lineage
The original shell script's data flow can be summarized as:
1. **Initialization:** Sources environment variables from `$HOME/.dw_init` and utility functions from various `.ksh` files (e.g., `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`).
2. **Parameter Input:** Receives `JobKennung` and `EintragsNr` as command-line arguments.
3. **Validation & Error Handling:** Validates input parameters and uses `DWMSG_MeldeFehler` for error reporting, exiting if validation fails.
4. **SQL Script Execution:** Invokes an external SQL script, `${BERT_DIR_ROOT}/aufbereitung/sql/d_ausd_v_ta_p_discount.sql`, via a `starteSQLSkript` function. This SQL script is responsible for the actual data preparation and interaction with the `ta_p_discount` table.
5. **Record Count & Output:** After the SQL script execution, it reads the number of records from a temporary file (`$DW_DIR_UTL/bert_k_ausd_v_ta_p_discount_$$.tmp`) and prints completion messages.

**Target BigQuery Data Flow:**
1. **Input Parameters:** The BigQuery Stored Procedure `r_ausd_vertrag_control` will receive `p_JobKennung` and `p_EintragsNr` as direct procedure arguments.
2. **Parameter Validation:** `IF` statements will replace the `getopts` and `pruefeParameterGesetzt` logic for validating input.
3. **Error Logging:** Error conditions will insert records into a BigQuery `error_log` table and `SIGNAL SQLSTATE` for graceful procedure termination.
4. **SQL Logic Execution:** The core data manipulation logic from `d_ausd_v_ta_p_discount.sql` will be encapsulated in a separate BigQuery Stored Procedure, e.g., `d_ausd_v_ta_p_discount`, which `r_ausd_vertrag_control` will `CALL`.
5. **Record Counting:** `SELECT COUNT(*)` directly on the `ta_p_discount` table (or relevant intermediate table) will replace the temporary file mechanism.
6. **Job Logging:** Execution status and processed record counts will be inserted into a `job_log` BigQuery table.

## 5. Transformation Logic
The transformation logic from the KornShell script itself is primarily control flow, parameter handling, and error management, with the actual data transformation delegated to the SQL script `d_ausd_v_ta_p_discount.sql`.

**Control Script (KornShell to BigQuery Stored Procedure Pseudocode):**

```sql
-- BigQuery Stored Procedure Pseudocode for k_ausd_v_ta_p_discount.ksh
CREATE OR REPLACE PROCEDURE `project.dataset.r_ausd_vertrag_control`(
  IN p_JobKennung STRING,
  IN p_EintragsNr STRING
)
BEGIN
  DECLARE ErrNr INT64 DEFAULT 0;
  DECLARE ErrArg STRING DEFAULT '';
  DECLARE v_TabName STRING DEFAULT 'ta_p_discount';
  DECLARE v_records INT64 DEFAULT 0;

  -- Parameter validation (replaces getopts and pruefeParameterGesetzt)
  IF p_JobKennung IS NULL OR p_JobKennung = '' THEN
    SET ErrNr = 193;
    SET ErrArg = 'Jobkennung';
  END IF;

  IF ErrNr = 0 AND (p_EintragsNr IS NULL OR p_EintragsNr = '') THEN
    SET ErrNr = 193;
    SET ErrArg = 'EintragsNr';
  END IF;

  -- Error handling (replaces DWMSG_MeldeFehler and shell exit)
  IF ErrNr != 0 THEN
    INSERT INTO `project.dataset.error_log`
      (error_number, error_argument, job_kennung, eintrags_nr, created_at)
    VALUES
      (ErrNr, ErrArg, p_JobKennung, p_EintragsNr, CURRENT_TIMESTAMP());

    -- Log message or raise an error for external orchestration to catch
    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT = FORMAT('FEHLER: 0 E %d %s. Bitte ueber Rahmenscript aufrufen', ErrNr, ErrArg);
  END IF;

  -- Main processing block
  BEGIN
    -- Call the BigQuery Stored Procedure equivalent of d_ausd_v_ta_p_discount.sql
    -- This procedure would contain the actual data transformation logic.
    CALL `project.dataset.d_ausd_v_ta_p_discount`(
      p_EintragsNr,
      p_JobKennung
    );

    -- Record count replacement for temporary file (cat $tmpFile)
    SET v_records = (
      SELECT COUNT(* FROM `project.dataset.ta_p_discount` WHERE eintrags_nr = p_EintragsNr)
      -- Assuming eintrags_nr is a relevant filter for this job's processed records
    );

    -- Log job completion (replaces implicit job table updates)
    INSERT INTO `project.dataset.job_log`
      (job_kennung, eintrags_nr, tab_name, records_processed, status, created_at)
    VALUES
      (p_JobKennung, p_EintragsNr, v_TabName, v_records, 'DONE', CURRENT_TIMESTAMP());

  EXCEPTION WHEN ERROR THEN
    -- Generic error handling for SQL execution failures
    INSERT INTO `project.dataset.error_log`
      (error_number, error_argument, job_kennung, eintrags_nr, created_at, error_message)
    VALUES
      (999, 'SQL_EXECUTION_FAILURE', p_JobKennung, p_EintragsNr, CURRENT_TIMESTAMP(), @@error.message);

    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT = 'SQL execution failed within r_ausd_vertrag_control.';
  END;
END;
```

The actual transformation logic for the data in `ta_p_discount` needs to be extracted from `d_ausd_v_ta_p_discount.sql` and translated into BigQuery SQL within the `d_ausd_v_ta_p_discount` BigQuery Stored Procedure.

## 6. External Dependencies
The original script has the following external dependencies:
- **Environment Initialization:** `$HOME/.dw_init`
- **Utility Scripts:**
    - `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh` (error messaging)
    - `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh` (date handling)
    - `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh` (parameter parsing helpers)
    - `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_sqlplus.ksh` (SQL execution wrapper)
- **Core SQL Script:** `${BERT_DIR_ROOT}/aufbereitung/sql/d_ausd_v_ta_p_discount.sql`
- **Temporary File:** `$DW_DIR_UTL/bert_k_ausd_v_ta_p_discount_$$.tmp` (for record count)
- **Database Connection:** Implied by `h_alis_sqlplus.ksh` and the SQL script.

**Replacement Strategy for BigQuery:**
- **Environment Initialization & Utility Scripts:** These will be replaced by:
    - Direct parameters to the BigQuery Stored Procedure.
    - BigQuery's built-in date functions.
    - BigQuery's error handling constructs (`BEGIN...EXCEPTION`, `SIGNAL SQLSTATE`).
    - Stored procedures calling other stored procedures (for `h_alis_sqlplus.ksh` and `d_ausd_v_ta_p_discount.sql`).
    - Constants or configuration tables within BigQuery for static values that were previously environment variables.
- **Core SQL Script:** Migrated to a dedicated BigQuery Stored Procedure (`d_ausd_v_ta_p_discount`) that will contain the core data transformation logic.
- **Temporary File:** Replaced by `SELECT COUNT(*)` queries into BigQuery variables or by inserting directly into a `job_log` table.
- **Database Connection:** BigQuery automatically handles connections when executing procedures or queries.

## 7. Unresolved / Risks
- **SQL Script Migration (d_ausd_v_ta_p_discount.sql):** The content of this SQL script is critical and needs a separate, detailed analysis and migration plan. Its specific SQL dialect (e.g., Oracle PL/SQL, Teradata SQL) will determine the complexity of conversion to BigQuery SQL. Any complex procedural logic within it will need careful translation.
- **`starteSQLSkript` Function Logic:** The `h_alis_sqlplus.ksh` and the `starteSQLSkript` function within it might contain specific logic for ignoring active jobs or updating a job table. This implicit logic needs to be fully understood and replicated in the BigQuery control procedure or in a separate job control procedure/table.
- **`DWMSG_MeldeFehler` Implementation:** The exact logic of this error reporting mechanism needs to be mapped to BigQuery's error logging, potentially including severity levels or external notifications.
- **Job Control Table Schema:** The schema for the `job_log` and `error_log` tables needs to be designed based on the information captured by the original shell script's job control mechanisms.
- **Performance:** Ensure that the migrated BigQuery SQL and Stored Procedures maintain or improve performance compared to the legacy system, especially for the core data transformation in `d_ausd_v_ta_p_discount.sql`.

## 8. Build Plan
1. **Analyze `d_ausd_v_ta_p_discount.sql`:** Perform a dedicated analysis of the SQL script to understand its exact transformation logic, source tables, and target `ta_p_discount` table.
2. **Design BigQuery Stored Procedure for Data Transformation:** Create the BigQuery Stored Procedure `project.dataset.d_ausd_v_ta_p_discount` in BigQuery SQL, encapsulating the logic from the original `d_ausd_v_ta_p_discount.sql`.
3. **Design BigQuery `ta_p_discount` Table:** Define the DDL for the `ta_p_discount` table in BigQuery, ensuring compatibility with the transformed data.
4. **Design BigQuery Control Tables:** Create DDL for `project.dataset.job_log` and `project.dataset.error_log` tables.
5. **Develop BigQuery Stored Procedure for Control Flow:** Implement `project.dataset.r_ausd_vertrag_control` (as per the pseudocode in Section 5) in BigQuery SQL. This procedure will:
    - Accept `p_JobKennung` and `p_EintragsNr`.
    - Implement parameter validation.
    - Call `project.dataset.d_ausd_v_ta_p_discount`.
    - Handle error logging into `project.dataset.error_log`.
    - Log job status and record counts into `project.dataset.job_log`.
6. **Develop Orchestration (Optional/Recommended):** If external scheduling is required, develop a Cloud Composer DAG or set up a BigQuery Scheduled Query to invoke `project.dataset.r_ausd_vertrag_control` with appropriate parameters.
7. **Testing:** Thoroughly test the BigQuery Stored Procedures and associated tables to ensure functional equivalence and performance.