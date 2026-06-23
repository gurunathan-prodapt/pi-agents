# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_iccid_einzeln.ksh

## 1. Purpose & Scope
This document outlines the migration design for the legacy KornShell script `k_ausd_bp_ta_iccid_einzeln.ksh` to Google Cloud Platform (GCP), specifically targeting BigQuery. The script serves as a control and orchestration component for a data processing job related to ICCID (Integrated Circuit Card ID) data, likely for reporting or data preparation purposes within the `isbert` system. Its primary functions include parameter validation, date validation, and the execution of a core SQL script that performs data extraction, transformation, and loading.

The scope of this migration includes:
- Re-implementing the shell script's logic in BigQuery SQL (via stored procedure or script).
- Migrating the invoked SQL script's logic to BigQuery SQL.
- Replacing legacy database tables with BigQuery tables.
- Addressing external dependencies and shell-specific functionalities.
- Establishing a new orchestration mechanism on GCP.

## 2. Source Inventory
The job consists of the following primary components:

*   **`vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_iccid_einzeln.ksh`**
    *   **Technology:** KornShell
    *   **Complexity Tier:** Medium
    *   **Automation Bucket:** Semi-Auto (B2)
    *   **Purpose:** Orchestrates parameter parsing, validation, date calculation, and executes the core SQL logic.
*   **`vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_bp_ta_iccid_einzeln.sql`** (Invoked by the .ksh script)
    *   **Technology:** SQL (likely Oracle PL/SQL based on context)
    *   **Purpose:** Contains the primary data manipulation logic, reading from source tables and writing to a target table.

## 3. Target Architecture
The migrated solution will leverage Google Cloud Platform services, primarily BigQuery, for data processing and storage, and Cloud Composer/Workflows for orchestration.

*   **Data Processing:** BigQuery Stored Procedures will encapsulate the control logic previously handled by the KornShell script. The core SQL logic will be migrated into a separate BigQuery SQL script or integrated into the stored procedure.
*   **Data Storage:** All source and target tables (e.g., `DWTK_MELDUNGEN`, `SOF$TA_BPR_BASIS`, `SOF$TA_ICCID_EINZELN`) will be migrated to BigQuery tables. Auxiliary tables for error logging and job tracking will also reside in BigQuery.
*   **Orchestration:** Cloud Composer (managed Apache Airflow) or Cloud Workflows will be used to schedule and manage the execution of the BigQuery stored procedures/scripts. Cloud Scheduler can trigger these workflows.
*   **Logging & Monitoring:** Stackdriver Logging and Monitoring will be used for centralized logging and alerting.

## 4. Data Flow & Lineage
The original data flow is initiated by the KornShell script, which then delegates the data manipulation to an SQL script.

**Legacy Flow:**
1.  **`k_ausd_bp_ta_iccid_einzeln.ksh` (KornShell)**:
    *   Loads environment variables (`. $HOME/.dw_init`) and utility scripts (e.g., `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`, `gestern.ksh`).
    *   Parses input parameters (`-j`, `-f`, `-s`, `-l`).
    *   Validates parameters and date format.
    *   Calls `starteSQLSkript` to execute `d_ausd_bp_ta_iccid_einzeln.sql`.
    *   Captures record count from a temporary file (`$DW_DIR_UTL/bert_k_ausd_bp_ta_iccid_einzeln.tmp`).
    *   (Commented out) Updates a job tracking table/system (`FOSJobErzeugeEintrag`).
2.  **`d_ausd_bp_ta_iccid_einzeln.sql` (SQL)**:
    *   Reads data from `TABLE:DWTK_MELDUNGEN` (source).
    *   Reads data from `TABLE:SOF$TA_BPR_BASIS` (source).
    *   Writes processed data to `TABLE:SOF$TA_ICCID_EINZELN` (target).
    *   Leverages `PACKAGE:DWPA_UTIL_SKRIPT`.

**Target BigQuery Flow:**
1.  **Cloud Composer DAG / Cloud Workflow**:
    *   Triggers the BigQuery Stored Procedure.
2.  **BigQuery Stored Procedure (`r_ausd_bp_ta_iccid_einzeln`)**:
    *   Receives parameters.
    *   Performs parameter and date validation using BigQuery SQL procedural language.
    *   Invokes the core BigQuery SQL logic (e.g., via `EXECUTE IMMEDIATE` or by embedding it).
    *   Inserts error messages into a BigQuery error log table.
    *   Calculates and records the number of processed records.
    *   Inserts job tracking information into a BigQuery job tracking table.
3.  **BigQuery SQL (migrated from `d_ausd_bp_ta_iccid_einzeln.sql`)**:
    *   Reads from migrated BigQuery tables: `DWTK_MELDUNGEN_BQ`, `SOF_TA_BPR_BASIS_BQ`.
    *   Applies transformation logic.
    *   Writes results to the target BigQuery table: `SOF_TA_ICCID_EINZELN_BQ`.
    *   Replaces `PACKAGE:DWPA_UTIL_SKRIPT` with BigQuery UDFs or equivalent logic.

## 5. Transformation Logic
The transformation logic will primarily involve migrating shell script control flow and SQL queries to BigQuery SQL syntax and constructs.

*   **Parameter Handling:** The `getopts` logic for parsing `p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, and `p_wiederanlaufWert` will be translated into input parameters for a BigQuery Stored Procedure.
*   **Validation:**
    *   `pruefeParameterGesetzt` will be re-implemented using `IF` statements and checks for `NULL` or empty strings in BigQuery SQL.
    *   `DWDate_Datum_Check` for `DDMMYYYY` format will use `SAFE.PARSE_DATE('%d%m%Y', p_Stichtag)` and conditional logic.
*   **Date Derivation:** The `gestern.ksh` functionality will be replaced by BigQuery's `CURRENT_DATE()` and `DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)`.
*   **SQL Execution:** The `starteSQLSkript` call will be replaced by direct execution of the migrated BigQuery SQL or by calling another BigQuery Stored Procedure that encapsulates the core data logic.
*   **Record Count:** The `eval "v_records=`cat $tmpFile`"` logic will be replaced by a `SELECT COUNT(*)` query against the target table after data insertion, storing the result in a BigQuery variable.
*   **Core SQL Logic:** The actual transformations within `d_ausd_bp_ta_iccid_einzeln.sql` (reading from `DWTK_MELDUNGEN`, `SOF$TA_BPR_BASIS`, and writing to `SOF$TA_ICCID_EINZELN`) will be directly translated into BigQuery DML and DDL statements. `PACKAGE:DWPA_UTIL_SKRIPT` will need to be analyzed for its functionality and migrated to BigQuery UDFs or integrated directly into the SQL.

## 6. External Dependencies
The original script has several external dependencies that need to be addressed:

*   **Environment Initialization (`. $HOME/.dw_init`)**: This script likely sets up environment variables. These variables will need to be configured as environment variables in Cloud Composer/Workflows, or passed as parameters to the BigQuery stored procedure, or explicitly defined within the BigQuery script.
*   **Utility Scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`, `gestern.ksh`)**:
    *   **Error Handling (`f_alis_msgerr.ksh`, `DWMSG_MeldeFehler`)**: Will be replaced by inserting error details into a dedicated BigQuery error logging table and using `SIGNAL SQLSTATE` for error propagation within BigQuery procedures.
    *   **Date Utilities (`h_alis_date.ksh`, `DWDate_Datum_Check`, `gestern.ksh`)**: Replaced by native BigQuery date functions (`CURRENT_DATE()`, `DATE_SUB`, `SAFE.PARSE_DATE`).
    *   **Parameter Utilities (`h_alis_parameter.ksh`, `pruefeParameterGesetzt`)**: Replaced by BigQuery SQL procedural validation logic.
    *   **SQLPlus Utilities (`h_alis_sqlplus.ksh`, `starteSQLSkript`)**: SQLPlus is an Oracle client. This will be replaced by direct BigQuery SQL execution.
*   **Temporary Files (`$DW_DIR_UTL/bert_k_ausd_bp_ta_iccid_einzeln.tmp`)**: File-based temporary storage will be replaced by BigQuery temporary tables, table variables, or direct `SELECT COUNT(*)` operations into BigQuery variables.
*   **Oracle Database Objects (`TABLE:DWTK_MELDUNGEN`, `TABLE:SOF$TA_BPR_BASIS`, `TABLE:SOF$TA_ICCID_EINZELN`)**: These tables, assumed to be in an Oracle database, will be migrated to BigQuery. Appropriate schema and data migration strategies (e.g., Datastream, Batch Data Transfer) will be applied.
*   **Oracle Package (`PACKAGE:DWPA_UTIL_SKRIPT`)**: The functionality of this Oracle package needs to be analyzed and re-implemented as BigQuery User-Defined Functions (UDFs) or integrated directly into the BigQuery SQL transformation logic.
*   **Job Management (`FOSJobDeaktivate`, `FOSJobErzeugeEintrag`)**: These commented-out calls suggest an external job control system. If these functionalities are still required, they will be replaced by an audit/job tracking table in BigQuery and integrated with the GCP orchestration solution (e.g., Airflow's XComs or task logging).

## 7. Unresolved / Risks
*   **Exact Logic of Utility Scripts:** While the purpose of some utility scripts (`h_alis_date.ksh`, `h_alis_parameter.ksh`) is clear, the full extent of their internal logic (e.g., `f_alis_msgerr.ksh` and `DWMSG_MeldeFehler`, `starteSQLSkript` implementation details) needs to be fully understood to ensure accurate migration to BigQuery equivalents.
*   **`d_ausd_bp_ta_iccid_einzeln.sql` Content:** The actual business logic within the SQL script was not directly analyzed by the `shellscript_to_bqsql_design` tool. This SQL script is critical and requires separate, detailed migration and testing to BigQuery SQL.
*   **`PACKAGE:DWPA_UTIL_SKRIPT` Functionality:** The precise functions performed by this Oracle package must be determined to create accurate BigQuery UDFs or inline logic.
*   **Commented-out Job Management:** The commented-out `FOSJobDeaktivate` and `FOSJobErzeugeEintrag` calls need clarification from stakeholders. If these represent retired functionality, they can be ignored. If they are needed for historical or audit purposes, a BigQuery-based job tracking solution must be designed and implemented.
*   **Data Types and Implicit Conversions:** Potential risks exist with data type mismatches or implicit conversions between the legacy Oracle environment and BigQuery, especially for date/time and numeric values. Thorough testing will be required.

## 8. Build Plan
The migration will proceed in an iterative fashion, focusing on modular components:

1.  **Migrate Core SQL Logic:**
    *   **Source:** `d_ausd_bp_ta_iccid_einzeln.sql`
    *   **Target Language:** BigQuery SQL
    *   **Output:** BigQuery SQL script or a BigQuery Stored Procedure.
    *   **Dependencies:** Requires the target BigQuery tables for `DWTK_MELDUNGEN`, `SOF$TA_BPR_BASIS`, `SOF$TA_ICCID_EINZELN` to be defined and possibly populated with sample data.
2.  **Migrate Oracle Database Objects:**
    *   **Source:** `DWTK_MELDUNGEN`, `SOF$TA_BPR_BASIS`, `SOF$TA_ICCID_EINZELN`
    *   **Target Language:** BigQuery DDL (for table schemas)
    *   **Output:** BigQuery table definitions.
    *   **Action:** Extract DDL from Oracle, convert to BigQuery DDL, and create tables. Plan for data transfer (e.g., one-time load via `bq load`, or continuous replication via Datastream/DMS).
3.  **Migrate Shell Script Control Logic:**
    *   **Source:** `k_ausd_bp_ta_iccid_einzeln.ksh`
    *   **Target Language:** BigQuery SQL (Stored Procedure)
    *   **Output:** BigQuery Stored Procedure (`CREATE OR REPLACE PROCEDURE ...`)
    *   **Steps:** Re-implement parameter parsing, validation, date derivation, and execution of the core SQL logic using BigQuery procedural statements (e.g., `DECLARE`, `SET`, `IF`, `CALL`).
4.  **Implement Utility Equivalents:**
    *   **Source:** Oracle package `DWPA_UTIL_SKRIPT` and shell utility functions.
    *   **Target Language:** BigQuery SQL (UDFs) or inline BigQuery SQL.
    *   **Output:** BigQuery UDFs or embedded SQL logic.
    *   **Steps:** Analyze functionality, translate to BigQuery SQL.
5.  **Develop Logging and Job Tracking:**
    *   **Target Language:** BigQuery DDL and DML
    *   **Output:** `CREATE TABLE` statements for `error_log` and `job_tracking` tables, `INSERT` statements within the main stored procedure.
6.  **Orchestration Design and Implementation:**
    *   **Target Language:** Python (for Cloud Composer DAG) or YAML (for Cloud Workflows).
    *   **Output:** Cloud Composer DAG file or Cloud Workflows definition.
    *   **Steps:** Define the DAG/workflow to trigger the BigQuery stored procedure, handle retries, and integrate with Cloud Logging.
7.  **Testing and Validation:**
    *   Perform unit, integration, and end-to-end testing.
    *   Validate data integrity and accuracy against legacy system outputs.
    *   Performance testing and optimization in BigQuery.

This phased approach allows for focused development and testing of individual components, minimizing risks during the migration.