# Reviewer Rejected — Human Review Required

**Job:** `DWH/DWH_KERN/PRODUKTION/DW.DWH_UMSATZ/DW.DWH_UMSATZ_KONSOLIDIERUNG_MONATLICH_JP.xml`

The automated reviewer rejected this output after 3 attempt(s). The generated files are committed for human inspection.

## Final Rejection Reason

The design document incorrectly claims that `umsatz_konsolidierung.mp` and `r_umsatz_konsolidierung_monatlich.ksh` were 'NOT FOUND', even though their full source code is provided in the context. As a result, the design and build completely miss the complex transformation logic (joins with DIM_KONZERNGESELLSCHAFT and STG_TARIFGRUPPEN_MAPPING, STORNO filtering) present in the `.mp` file, replacing it with a dummy SQL script. Additionally, literal print/error messages from the JS file and the MP file are dropped, and the build output contains conflicting duplicate implementations (both PySpark and BigQuery SQL).

## Required Changes

1. Acknowledge and analyze the source code for `umsatz_konsolidierung.mp` and `r_umsatz_konsolidierung_monatlich.ksh` provided in the context.
2. Implement the actual transformation logic from the `.mp` file (normalisation, joins, storno filtering, aggregations) rather than a dummy SQL query.
3. Preserve the exact literal message from the JS file (`Umsatzkonsolidierung fuer Monat &VERARBEITUNGSMONAT, Konzerngesellschaft &KONZERNGESELLSCHAFT angestossen`).
4. Preserve the exact literal error messages from the `.mp` file (`Periodenvalidierung fehlgeschlagen fuer...`, `Zeilenanzahl-Validierung fehlgeschlagen...`, `Konsolidierungstoleranz ueberschritten`).
5. Consolidate the build output to avoid duplicate DAGs and duplicate implementations (choose either PySpark or BigQuery SQL, but implement the full logic).