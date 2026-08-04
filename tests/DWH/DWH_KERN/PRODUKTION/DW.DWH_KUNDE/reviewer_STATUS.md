# Reviewer Rejected — Human Review Required

**Job:** `DW.DWH_KUNDE_ABGL_WOECHENTLICH_JS`

The automated reviewer rejected this output after 3 attempt(s). The generated files are committed for human inspection.

## Final Rejection Reason

The build output for the SQL file was generated as a placeholder stub claiming that the source SQL was missing in the context. However, the source SQL was fully provided in the context. The build must be redone to implement the actual BigQuery SQL query as specified in the design.

## Required Changes

1. Replace the placeholder stub in DWH/DWH_KERN/PRODUKTION/DW.DWH_KUNDE/sql/d_abgl_kunde_woech.sql with the actual BigQuery SQL translation.
2. Implement the SELECT query joining DWH_KERN.T_KUNDE and STAMMDATEN.T_KUNDE_REFERENZ on KUNDE.
3. Use PARSE_DATE('%Y%m%d', @p_Stichtag) to filter k.AKTUALISIERT_AM.
4. Use COALESCE instead of NVL for null-safe comparison of PLZ, ORT, and STRASSE.
5. Retain the 'ABWEICHUNG' marker and order by k.KUNDE.
## Per-File Review Results

- ✅ `DWH/DWH_KERN/PRODUKTION/DW.DWH_KUNDE/DW.DWH_KUNDE_ABGL_WOECHENTLICH_JS.xml`
- ✅ `DWH/DWH_KERN/PRODUKTION/DW.DWH_KUNDE/bin/r_abgl_kunde_woech.ksh`
- ❌ `DWH/DWH_KERN/PRODUKTION/DW.DWH_KUNDE/sql/d_abgl_kunde_woech.sql`
  - Replace the placeholder stub with the actual BigQuery SQL translation of the source query. Use the query parameter @p_Stichtag with PARSE_DATE('%Y%m%d', @p_Stichtag) for the date filter, and use COALESCE instead of NVL for null-safe comparisons of PLZ, ORT, and STRASSE. Retain the 'ABWEICHUNG' marker and order by k.KUNDE.