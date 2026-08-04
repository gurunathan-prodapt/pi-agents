# Reviewer Rejected — Human Review Required

**Job:** `DWH/DWH_KERN/PRODUKTION/DW.DWH_RECHNUNG/DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JP.xml`

The automated reviewer rejected this output after 3 attempt(s). The generated files are committed for human inspection.

## Final Rejection Reason

The build output for the SQL file was implemented as a Dataform SQLX view (d_exp_rechnung_taeglich.sqlx) containing a dynamic query parameter (@stichtag). BigQuery does not support query parameters in views, which will cause Dataform compilation and deployment to fail. Furthermore, the companion Python script (r_exp_rechnung_taeglich.py) is designed to look for a plain SQL file (d_exp_rechnung_taeglich.sql) at runtime, meaning it will fail to find the SQL file. The SQL file must be built as a plain .sql file instead of a .sqlx view.

## Required Changes

1. Change the target file path from 'DWH/DWH_KERN/PRODUKTION/DW.DWH_RECHNUNG/sql/d_exp_rechnung_taeglich.sqlx' to 'DWH/DWH_KERN/PRODUKTION/DW.DWH_RECHNUNG/sql/d_exp_rechnung_taeglich.sql'.
2. Remove the Dataform config block entirely.
3. Keep the query as standard BigQuery SQL using the '@stichtag' parameter so that it can be read and executed dynamically by the Python script.
## Per-File Review Results

- ✅ `DWH/DWH_KERN/PRODUKTION/DW.DWH_RECHNUNG/DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JP.xml`
- ✅ `DWH/DWH_KERN/PRODUKTION/DW.DWH_RECHNUNG/DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JS.xml`
- ✅ `DWH/DWH_KERN/PRODUKTION/DW.DWH_RECHNUNG/bin/r_exp_rechnung_taeglich.ksh`
- ❌ `DWH/DWH_KERN/PRODUKTION/DW.DWH_RECHNUNG/sql/d_exp_rechnung_taeglich.sql`
  - 1. Build this file as a plain SQL file ('DWH/DWH_KERN/PRODUKTION/DW.DWH_RECHNUNG/sql/d_exp_rechnung_taeglich.sql') instead of a Dataform SQLX view.
2. Remove the Dataform config block.
3. Retain the standard BigQuery SELECT query using the '@stichtag' parameter so the Python script can read and execute it.