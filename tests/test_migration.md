As a senior data-migration QA engineer, I've analyzed the migration design for `gestern.ksh` to BigQuery SQL. The core challenge and key validation point lie in the intentional correction of the legacy script's incomplete leap year logic by leveraging BigQuery's native date functions. This means some tests will aim for *output parity*, while one critical test will explicitly look for an *expected difference* to confirm the improved correctness.

Here are the migration validation tests:

---

## Migration Validation Tests for `gestern.ksh`

### Test Case 1: Standard Day Calculation (Output Parity)

*   **Purpose:** Verify that for a typical day (not a month or year boundary, and not a leap year edge case), the migrated BigQuery SQL produces identical output to the legacy KornShell script. This covers basic date calculation, formatting, and the replacement of the `date` command with `CURRENT_DATE()`.
*   **Setup:**
    1.  **Legacy Script Mocking:** Create a temporary environment where the `date` command is mocked to return `15 10 2023`.
        *   `mock_date.sh`:
            ```bash
            #!/bin/bash
            echo "15 10 2023"
            ```
        *   Make it executable: `chmod +x mock_date.sh`
    2.  **BigQuery SQL Input:** The BigQuery script will be executed with `CURRENT_DATE()` effectively set to `DATE '2023-10-15'`.
*   **Action:**
    1.  **Run Legacy:** Execute the `gestern.ksh` script with the mocked `date` command in its `PATH`. Capture its standard output.
        ```bash
        # Assuming mock_date.sh is in ./mock_bin/
        mkdir -p mock_bin
        echo '#!/bin/bash' > mock_bin/date
        echo 'echo "15 10 2023"' >> mock_bin/date
        chmod +x mock_bin/date

        # Run the legacy script with mocked date
        LEGACY_OUTPUT=$(PATH=./mock_bin:$PATH vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/gestern.ksh)
        echo "Legacy Output: $LEGACY_OUTPUT"
        ```
    2.  **Run Migrated:** Execute the BigQuery SQL, replacing `CURRENT_DATE()` with `DATE '2023-10-15'`. Capture the result.
        ```sql
        -- BigQuery SQL for testing
        DECLARE Var_Datum_Heute STRING;
        DECLARE Var_Monat_Heute STRING;
        DECLARE Var_Datum_Gestern STRING;
        DECLARE Var_Monat_Gestern STRING;

        DECLARE current_date_value DATE DEFAULT DATE '2023-10-15'; -- Mocked date
        DECLARE yesterday_date_value DATE DEFAULT DATE_SUB(current_date_value, INTERVAL 1 DAY);

        SET Var_Datum_Heute = FORMAT_DATE('%Y%m%d', current_date_value);
        SET Var_Monat_Heute = FORMAT_DATE('%Y%m', current_date_value);

        SET Var_Datum_Gestern = FORMAT_DATE('%Y%m%d', yesterday_date_value);
        SET Var_Monat_Gestern = FORMAT_DATE('%Y%m', yesterday_date_value);

        SELECT
          Var_Datum_Heute AS Var_Datum_Heute,
          Var_Datum_Gestern AS Var_Datum_Gestern,
          Var_Monat_Heute AS Var_Monat_Heute,
          Var_Monat_Gestern AS Var_Monat_Gestern;
        ```
*   **Pass/Fail Criterion:**
    *   **Expected Legacy Output:** `20231015 20231014 202310 202310`
    *   **Expected Migrated Output:** `20231015 20231014 202310 202310`
    *   The output of the BigQuery script must exactly match the output of the legacy script.

### Test Case 2: Month Transition (Output Parity)

*   **Purpose:** Verify correct handling of month transitions for yesterday's date, ensuring parity with the legacy script when today is the first day of a month.
*   **Setup:**
    1.  **Legacy Script Mocking:** Mock `date` to return `01 11 2023`.
    2.  **BigQuery SQL Input:** `CURRENT_DATE()` effectively set to `DATE '2023-11-01'`.
*   **Action:**
    1.  **Run Legacy:** Execute `gestern.ksh` with mocked `date`.
        ```bash
        # Update mock_date.sh
        echo '#!/bin/bash' > mock_bin/date
        echo 'echo "01 11 2023"' >> mock_bin/date
        chmod +x mock_bin/date
        LEGACY_OUTPUT=$(PATH=./mock_bin:$PATH vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/gestern.ksh)
        echo "Legacy Output: $LEGACY_OUTPUT"
        ```
    2.  **Run Migrated:** Execute the BigQuery SQL, replacing `CURRENT_DATE()` with `DATE '2023-11-01'`.
        ```sql
        -- BigQuery SQL for testing
        DECLARE current_date_value DATE DEFAULT DATE '2023-11-01'; -- Mocked date
        -- ... rest of the script ...
        SELECT
          FORMAT_DATE('%Y%m%d', current_date_value) AS Var_Datum_Heute,
          FORMAT_DATE('%Y%m%d', DATE_SUB(current_date_value, INTERVAL 1 DAY)) AS Var_Datum_Gestern,
          FORMAT_DATE('%Y%m', current_date_value) AS Var_Monat_Heute,
          FORMAT_DATE('%Y%m', DATE_SUB(current_date_value, INTERVAL 1 DAY)) AS Var_Monat_Gestern;
        ```
*   **Pass/Fail Criterion:**
    *   **Expected Legacy Output:** `20231101 20231031 202311 202310`
    *   **Expected Migrated Output:** `20231101 20231031 202311 202310`
    *   The output of the BigQuery script must exactly match the output of the legacy script.

### Test Case 3: Year Transition (Output Parity)

*   **Purpose:** Verify correct handling of year transitions for yesterday's date, ensuring parity with the legacy script when today is January 1st.
*   **Setup:**
    1.  **Legacy Script Mocking:** Mock `date` to return `01 01 2024`.
    2.  **BigQuery SQL Input:** `CURRENT_DATE()` effectively set to `DATE '2024-01-01'`.
*   **Action:**
    1.  **Run Legacy:** Execute `gestern.ksh` with mocked `date`.
        ```bash
        # Update mock_date.sh
        echo '#!/bin/bash' > mock_bin/date
        echo 'echo "01 01 2024"' >> mock_bin/date
        chmod +x mock_bin/date
        LEGACY_OUTPUT=$(PATH=./mock_bin:$PATH vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/gestern.ksh)
        echo "Legacy Output: $LEGACY_OUTPUT"
        ```
    2.  **Run Migrated:** Execute the BigQuery SQL, replacing `CURRENT_DATE()` with `DATE '2024-01-01'`.
        ```sql
        -- BigQuery SQL for testing
        DECLARE current_date_value DATE DEFAULT DATE '2024-01-01'; -- Mocked date
        -- ... rest of the script ...
        SELECT
          FORMAT_DATE('%Y%m%d', current_date_value) AS Var_Datum_Heute,
          FORMAT_DATE('%Y%m%d', DATE_SUB(current_date_value, INTERVAL 1 DAY)) AS Var_Datum_Gestern,
          FORMAT_DATE('%Y%m', current_date_value) AS Var_Monat_Heute,
          FORMAT_DATE('%Y%m', DATE_SUB(current_date_value, INTERVAL 1 DAY)) AS Var_Monat_Gestern;
        ```
*   **Pass/Fail Criterion:**
    *   **Expected Legacy Output:** `20240101 20231231 202401 202312`
    *   **Expected Migrated Output:** `20240101 20231231 202401 202312`
    *   The output of the BigQuery script must exactly match the output of the legacy script.

### Test Case 4: Leap Year - Legacy Inaccuracy (Transformation Correctness - Expected Difference)

*   **Purpose:** Verify that the migrated BigQuery SQL correctly handles a leap year (e.g., year 2000) where the legacy script's incomplete leap year logic would produce an *incorrect* result. This test confirms the *intended improvement* in correctness as per the migration design.
*   **Setup:**
    1.  **Legacy Script Mocking:** Mock `date` to return `01 03 2000`. (Year 2000 was a leap year, so Feb 29th, 2000 was valid).
    2.  **BigQuery SQL Input:** `CURRENT_DATE()` effectively set to `DATE '2000-03-01'`.
*   **Action:**
    1.  **Run Legacy:** Execute `gestern.ksh` with mocked `date`.
        *   *Legacy Logic Analysis:* For 2000, `2000 % 4 == 0` is true, but `2000 % 100 > 0` is false. Thus, the legacy script's leap year condition is *false*, and it will incorrectly calculate February 2000 as having 28 days.
        ```bash
        # Update mock_date.sh
        echo '#!/bin/bash' > mock_bin/date
        echo 'echo "01 03 2000"' >> mock_bin/date
        chmod +x mock_bin/date
        LEGACY_OUTPUT=$(PATH=./mock_bin:$PATH vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/gestern.ksh)
        echo "Legacy Output: $LEGACY_OUTPUT"
        ```
    2.  **Run Migrated:** Execute the BigQuery SQL, replacing `CURRENT_DATE()` with `DATE '2000-03-01'`.
        *   *BigQuery Logic Analysis:* `DATE_SUB` will correctly identify February 2000 as having 29 days.
        ```sql
        -- BigQuery SQL for testing
        DECLARE current_date_value DATE DEFAULT DATE '2000-03-01'; -- Mocked date
        -- ... rest of the script ...
        SELECT
          FORMAT_DATE('%Y%m%d', current_date_value) AS Var_Datum_Heute,
          FORMAT_DATE('%Y%m%d', DATE_SUB(current_date_value, INTERVAL 1 DAY)) AS Var_Datum_Gestern,
          FORMAT_DATE('%Y%m', current_date_value) AS Var_Monat_Heute,
          FORMAT_DATE('%Y%m', DATE_SUB(current_date_value, INTERVAL 1 DAY)) AS Var_Monat_Gestern;
        ```
*   **Pass/Fail Criterion:**
    *   **Expected Legacy Output:** `20000301 20000228 200003 200002`
    *   **Expected Migrated Output:** `20000301 20000229 200003 200002`
    *   The `Var_Datum_Gestern` value from the BigQuery script (`20000229`) must be *different* from the legacy script (`20000228`), confirming the BigQuery implementation's improved accuracy for this specific leap year case. All other output values should match.

### Test Case 5: Leap Year - Legacy Accuracy (Output Parity)

*   **Purpose:** Verify that for a standard leap year (divisible by 4, not by 100), the migrated BigQuery SQL produces identical output to the legacy KornShell script. This confirms parity where the legacy logic was already correct.
*   **Setup:**
    1.  **Legacy Script Mocking:** Mock `date` to return `01 03 2024`. (Year 2024 is a leap year, so Feb 29th, 2024 was valid).
    2.  **BigQuery SQL Input:** `CURRENT_DATE()` effectively set to `DATE '2024-03-01'`.
*   **Action:**
    1.  **Run Legacy:** Execute `gestern.ksh` with mocked `date`.
        *   *Legacy Logic Analysis:* For 2024, `2024 % 4 == 0` is true, and `2024 % 100 > 0` is true. Thus, the legacy script's leap year condition is *true*, and it will correctly calculate February 2024 as having 29 days.
        ```bash
        # Update mock_date.sh
        echo '#!/bin/bash' > mock_bin/date
        echo 'echo "01 03 2024"' >> mock_bin/date
        chmod +x mock_bin/date
        LEGACY_OUTPUT=$(PATH=./mock_bin:$PATH vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/gestern.ksh)
        echo "Legacy Output: $LEGACY_OUTPUT"
        ```
    2.  **Run Migrated:** Execute the BigQuery SQL, replacing `CURRENT_DATE()` with `DATE '2024-03-01'`.
        *   *BigQuery Logic Analysis:* `DATE_SUB` will correctly identify February 2024 as having 29 days.
        ```sql
        -- BigQuery SQL for testing
        DECLARE current_date_value DATE DEFAULT DATE '2024-03-01'; -- Mocked date
        -- ... rest of the script ...
        SELECT
          FORMAT_DATE('%Y%m%d', current_date_value) AS Var_Datum_Heute,
          FORMAT_DATE('%Y%m%d', DATE_SUB(current_date_value, INTERVAL 1 DAY)) AS Var_Datum_Gestern,
          FORMAT_DATE('%Y%m', current_date_value) AS Var_Monat_Heute,
          FORMAT_DATE('%Y%m', DATE_SUB(current_date_value, INTERVAL 1 DAY)) AS Var_Monat_Gestern;
        ```
*   **Pass/Fail Criterion:**
    *   **Expected Legacy Output:** `20240301 20240229 202403 202402`
    *   **Expected Migrated Output:** `20240301 20240229 202403 202402`
    *   The output of the BigQuery script must exactly match the output of the legacy script.

### Test Case 6: Output Schema and Data Types (Data Quality / Schema Assertions)

*   **Purpose:** Verify that the BigQuery script produces the expected number of columns, column names, and data types, ensuring downstream consumers can correctly interpret the output.
*   **Setup:**
    1.  Execute the BigQuery SQL script as it would run in production (using `CURRENT_DATE()`).
*   **Action:**
    1.  Use the BigQuery client library (e.g., Python `google-cloud-bigquery`) to execute the query and inspect the schema of the returned result set.
*   **Pass/Fail Criterion:**
    *   The result set must contain exactly 4 columns.
    *   The column names must be `Var_Datum_Heute`, `Var_Datum_Gestern`, `Var_Monat_Heute`, `Var_Monat_Gestern`.
    *   All columns must have a data type of `STRING`.

    ```python
    import pytest
    from google.cloud import bigquery

    def test_bigquery_output_schema():
        client = bigquery.Client()
        query = """
        DECLARE Var_Datum_Heute STRING;
        DECLARE Var_Monat_Heute STRING;
        DECLARE Var_Datum_Gestern STRING;
        DECLARE Var_Monat_Gestern STRING;

        DECLARE current_date_value DATE DEFAULT CURRENT_DATE();
        DECLARE yesterday_date_value DATE DEFAULT DATE_SUB(current_date_value, INTERVAL 1 DAY);

        SET Var_Datum_Heute = FORMAT_DATE('%Y%m%d', current_date_value);
        SET Var_Monat_Heute = FORMAT_DATE('%Y%m', current_date_value);

        SET Var_Datum_Gestern = FORMAT_DATE('%Y%m%d', yesterday_date_value);
        SET Var_Monat_Gestern = FORMAT_DATE('%Y%m', yesterday_date_value);

        SELECT
          Var_Datum_Heute AS Var_Datum_Heute,
          Var_Datum_Gestern AS Var_Datum_Gestern,
          Var_Monat_Heute AS Var_Monat_Heute,
          Var_Monat_Gestern AS Var_Monat_Gestern;
        """
        query_job = client.query(query)
        results = query_job.result()

        # Assert number of columns
        assert len(results.schema) == 4, "Expected 4 columns in the output."

        # Assert column names and types
        expected_schema = {
            "Var_Datum_Heute": "STRING",
            "Var_Datum_Gestern": "STRING",
            "Var_Monat_Heute": "STRING",
            "Var_Monat_Gestern": "STRING",
        }

        for field in results.schema:
            assert field.name in expected_schema, f"Unexpected column name: {field.name}"
            assert field.field_type == expected_schema[field.name], \
                f"Column {field.name} has unexpected type: {field.field_type}, expected {expected_schema[field.name]}"

        # Optional: Assert row count (always 1 for this script)
        assert results.total_rows == 1, "Expected exactly one row in the output."

        # Optional: Assert data format for a sample row
        for row in results:
            assert len(row.Var_Datum_Heute) == 8 and row.Var_Datum_Heute.isdigit()
            assert len(row.Var_Datum_Gestern) == 8 and row.Var_Datum_Gestern.isdigit()
            assert len(row.Var_Monat_Heute) == 6 and row.Var_Monat_Heute.isdigit()
            assert len(row.Var_Monat_Gestern) == 6 and row.Var_Monat_Gestern.isdigit()
            break # Only need to check the first row
    ```