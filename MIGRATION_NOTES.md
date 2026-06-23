# MIGRATION_NOTES.md

## 1. Summary

The KornShell script `h_alis_date.ksh`, located at `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_date.ksh`, has been migrated. This script served as a utility library for common date calculations within a data warehousing environment, historically relying on KornShell logic and interactions with an Oracle database via `sqlplus`.

The migration target platform is **Google BigQuery**. The functionality of `h_alis_date.ksh` has been re-implemented as a set of BigQuery SQL stored procedures. This migration eliminates dependencies on KornShell, Oracle, and temporary file-based inter-process communication, standardizing date-related computations within the BigQuery ecosystem.

## 2. Generated artifacts

The migration has produced the following BigQuery SQL stored procedure files, intended for deployment into the `your_project.date_utilities` dataset:

*   **`date_utilities/DWDate_Vormonat.sql`**:
    *   **Role**: Implements the `DWDate_Vormonat` function, calculating the first day of the previous month.
*   **`date_utilities/DWDate_Datum_Check.sql`**:
    *   **Role**: Implements the `DWDate_Datum_Check` function, validating if a given string represents a valid date according to a specified format.
*   **`date_utilities/DWDate_Datum_LE.sql`**:
    *   **Role**: Implements the `DWDate_Datum_LE` function, comparing two dates to determine if the first is less than or equal to the second (`P1 <= P2`).
*   **`date_utilities/DWDate_Gib_Zeitraum.sql`**:
    *   **Role**: Implements the `DWDate_Gib_Zeitraum` function, calculating a date period (start and end dates) based on an offset and granularity (Day, Month, Year).
*   **`date_utilities/LetzterTagDesMonat.sql`**:
    *   **Role**: Implements the `LetzterTagDesMonat` function, checking if a given date is the last day of its month.
*   **`date_utilities/TageimMonat.sql`**:
    *   **Role**: Implements the `TageimMonat` function, calculating the number of days in a specific month of a given year.
*   **`date_utilities/AddiereDatum.sql`**:
    *   **Role**: Implements the `AddiereDatum` function, adding a specified number of days to a given date.

## 3. Key design decisions

*   **Target Platform Choice (BigQuery Stored Procedures):** The decision to migrate to BigQuery stored procedures was driven by the goal to eliminate legacy KornShell and Oracle dependencies. BigQuery offers robust native date/time functions, allowing for a direct and efficient re-implementation of the original logic within a modern, scalable data warehousing environment. This centralizes date utility logic within BigQuery, making it accessible to other BigQuery-based ETL processes.
*   **Function-to-Procedure Mapping:** Each distinct KornShell function within `h_alis_date.ksh` has been directly mapped to a corresponding BigQuery SQL stored procedure. This maintains a clear one-to-one functional equivalence, simplifying understanding and future maintenance.
*   **Leveraging BigQuery Native Functions:** The core logic of date calculations now exclusively uses BigQuery's rich set of date and time functions (e.g., `DATE_SUB`, `DATE_TRUNC`, `LAST_DAY`, `DATE_ADD`, `PARSE_DATE`, `FORMAT_DATE`, `SAFE.PARSE_DATE`, `DATE_DIFF`). This replaces the complex mix of shell scripting, `sqlplus` calls, and Oracle-specific SQL/PLSQL, leading to more concise, readable, and performant code.
*   **Elimination of Temporary Files and `eval`:** The original script's reliance on temporary files for `sqlplus` output and `eval` for dynamic variable assignment has been completely removed. BigQuery stored procedures handle input and output parameters directly (`IN`, `OUT`), providing a cleaner and more secure mechanism for data exchange.
*   **Error Handling:** BigQuery's `RAISE USING MESSAGE` construct is used for explicit error conditions (e.g., invalid input parameters), replacing the original script's return codes or Oracle's `raise_application_error`. Boolean `OUT` parameters are used where the original script returned success/failure indicators.
*   **Trade-offs:**
    *   **Calling Context Adaptation:** Any existing scripts or applications that previously invoked `h_alis_date.ksh` will require modification to call the new BigQuery stored procedures. This involves changing shell command execution to BigQuery API calls or integrating with an orchestration tool.
    *   **Oracle-specific Date Behavior:** While BigQuery's date functions are comprehensive, there might be subtle differences in how Oracle and BigQuery handle specific date edge cases (e.g., timezone handling, specific `TO_DATE` format behaviors). Thorough testing is crucial to ensure exact functional equivalence.

## 4. Manual steps before go-live

1.  **BigQuery Dataset Creation:**
    *   Ensure the target BigQuery dataset `your_project.date_utilities` exists. If not, create it:
        ```sql
        CREATE SCHEMA IF NOT EXISTS `your_project.date_utilities`;
        ```
    *   **Note:** Replace `your_project` with the actual Google Cloud Project ID.

2.  **IAM Permissions:**
    *   Grant appropriate IAM roles to the service accounts or users that will deploy and execute these stored procedures.
        *   For deployment: `BigQuery Data Editor` or `BigQuery Admin` on the `your_project.date_utilities` dataset.
        *   For execution: `BigQuery Data Viewer` and `BigQuery Job User` on the project, and `BigQuery Data Viewer` on the `your_project.date_utilities` dataset.

3.  **Deployment of Stored Procedures:**
    *   Execute each generated `.sql` file (`DWDate_Vormonat.sql`, `DWDate_Datum_Check.sql`, etc.) in BigQuery. This will create or replace the stored procedures in the `your_project.date_utilities` dataset.

4.  **Update Calling Jobs/Scripts:**
    *   Identify all upstream jobs, scripts, or applications that currently call `h_alis_date.ksh`.
    *   Modify these callers to invoke the new BigQuery stored procedures instead of the legacy KornShell script. This will involve:
        *   Establishing a BigQuery connection (e.g., via `bq` CLI, client libraries, or an orchestration tool like Airflow).
        *   Translating the original shell function calls and parameters into BigQuery procedure calls with appropriate input/output handling.
        *   Example BigQuery procedure call:
            ```sql
            DECLARE result STRING;
            CALL `your_project.date_utilities`.DWDate_Vormonat('%Y%m%d', result);
            SELECT result;
            ```

5.  **Scheduling Configuration:**
    *   If the original `h_alis_date.ksh` was part of a scheduled job, update the scheduler (e.g., Cron, Airflow, Cloud Composer) to execute the new BigQuery procedure calls.

## 5. Known gaps & unresolved references

*   **Unresolved Lineage:** The automated lineage detection for `h_alis_date.ksh` did not yield results. This means that any scripts or jobs calling `h_alis_date.ksh` were not automatically identified. **Manual identification and update of these calling contexts are critical.**
*   **Oracle-specific Date Behavior:** While BigQuery functions are robust, subtle differences in date handling (e.g., specific format parsing, timezone defaults) between Oracle and BigQuery might exist. Thorough testing is required to ensure complete functional equivalence for all edge cases.
*   **Dynamic Variable Names (`eval`):** The original script used `eval` for dynamic variable assignments. In BigQuery, this is handled by explicit `OUT` parameters. Calling contexts must be updated to correctly receive and process these `OUT` parameters.
*   **Error Handling Fidelity:** The BigQuery procedures use `RAISE USING MESSAGE` for errors. While functionally equivalent to the original's error handling, the exact error messages and their structure might differ. Consumers of these procedures should be aware of BigQuery's error model.
*   **`your_project` Placeholder:** The generated code uses `your_project` as a placeholder for the Google Cloud Project ID. This must be replaced with the actual project ID during deployment.

## 6. Validation

Validation should confirm that the BigQuery stored procedures produce identical results to the original `h_alis_date.ksh` script for a comprehensive set of inputs.

1.  **Unit Testing (BigQuery Procedures):**
    *   For each BigQuery stored procedure, execute it with a wide range of test cases, including:
        *   **Standard inputs:** Typical dates, offsets, formats.
        *   **Edge cases:** Leap years, month boundaries (e.g., Jan 1, Dec 31), dates at the beginning/end of months, zero offsets.
        *   **Invalid inputs:** Incorrect date formats for `DWDate_Datum_Check`, non-existent dates (e.g., Feb 30).
    *   **Expected "Passing" Criteria:** The output of the BigQuery stored procedure (via `OUT` parameters or `SELECT` statements) must exactly match the output of the original KornShell function for the same inputs. For `DWDate_Datum_Check` and `DWDate_Datum_LE`, the boolean output should be consistent. For procedures that `RAISE` an error, the BigQuery procedure should also raise an error (or return an expected error state).

2.  **Integration Testing (Calling Jobs):**
    *   After updating the calling jobs/scripts (as per "Manual steps"), run these modified jobs in a test environment.
    *   **Expected "Passing" Criteria:** The overall end-to-end process that consumes these date utilities should execute successfully and produce the same final data or reports as before the migration. This confirms that the integration points are correctly configured and the BigQuery procedures are behaving as expected within the larger workflow.

## 7. Rollback procedure

In case of issues or unexpected behavior after deployment, the following steps outline the rollback procedure:

1.  **Revert Calling Jobs/Scripts:**
    *   Immediately revert any changes made to upstream jobs, scripts, or applications that were modified to call the new BigQuery stored procedures. Restore them to their original state, where they invoke the legacy `h_alis_date.ksh` script.

2.  **Re-enable Original Script:**
    *   Ensure that the original `h_alis_date.ksh` script is available, executable, and correctly configured with its original dependencies (Oracle connection, environment variables, etc.).

3.  **Delete BigQuery Stored Procedures (Optional but Recommended):**
    *   To avoid confusion and ensure a clean state, delete the newly deployed BigQuery stored procedures from the `your_project.date_utilities` dataset.
        ```sql
        DROP PROCEDURE IF EXISTS `your_project.date_utilities`.DWDate_Vormonat;
        DROP PROCEDURE IF EXISTS `your_project.date_utilities`.DWDate_Datum_Check;
        DROP PROCEDURE IF EXISTS `your_project.date_utilities`.DWDate_Datum_LE;
        DROP PROCEDURE IF EXISTS `your_project.date_utilities`.DWDate_Gib_Zeitraum;
        DROP PROCEDURE IF EXISTS `your_project.date_utilities`.LetzterTagDesMonat;
        DROP PROCEDURE IF EXISTS `your_project.date_utilities`.TageimMonat;
        DROP PROCEDURE IF EXISTS `your_project.date_utilities`.AddiereDatum;
        ```
    *   If the entire dataset was created solely for these procedures, consider dropping the dataset as well.

4.  **Monitor:**
    *   Monitor the reverted system to ensure all processes are functioning correctly with the original setup.