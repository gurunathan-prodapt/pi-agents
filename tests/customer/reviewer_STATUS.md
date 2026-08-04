# Reviewer Rejected — Human Review Required

**Job:** `CUSTOMER.HISTORIZATION_LOAD`

The automated reviewer rejected this output after 3 attempt(s). The generated files are committed for human inspection.

## Final Rejection Reason

The build output stubbed out the actual SQL logic for both `d_historization_load.sql` and `d_segment_quality_check.sql`, claiming the source code was not supplied. The source code and pseudocode were fully provided in the design document. The build must implement the actual BigQuery SQL transformations rather than emitting stubs.

## Required Changes

(see explanation above)
## Per-File Review Results

- ✅ `customer/CUSTOMER.HISTORIZATION_LOAD.xml`
- ❌ `customer/d_historization_load.sql`
  - 1. Replace the stubbed output with the actual BigQuery SQL logic provided in the design.
2. Implement the `BEGIN TRANSACTION;` and `COMMIT TRANSACTION;` block.
3. Declare `v_current_time TIMESTAMP` and set it to `CURRENT_TIMESTAMP()` to ensure temporal alignment between the MERGE and INSERT.
4. Implement the `MERGE` and `INSERT` statements from the design.
5. Use the query parameter `@RUN_DATE` directly in the `PARSE_DATE` function (e.g., `PARSE_DATE('%Y-%m-%d', @RUN_DATE)`) instead of declaring a local variable for it.
- ❌ `customer/d_segment_quality_check.sql`
  - 1. Replace the stubbed output with the actual BigQuery SQL logic provided in the design.
2. Implement the `SELECT ROUND(...)` query to calculate the changed percentage.
3. Use the query parameter `@RUN_DATE` directly in the `PARSE_DATETIME` function (e.g., `DATETIME_TRUNC(PARSE_DATETIME('%Y-%m-%d', @RUN_DATE), DAY)`) instead of declaring a local variable for it.
- ✅ `customer/k_historization_load.ksh`
- ✅ `customer/r_historization_load.ksh`