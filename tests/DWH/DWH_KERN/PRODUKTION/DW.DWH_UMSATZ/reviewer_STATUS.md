# Reviewer Rejected — Human Review Required

**Job:** `DW.DWH_UMSATZ_KONSOLIDIERUNG_MONATLICH_JS`

The automated reviewer rejected this output after 3 attempt(s). The generated files are committed for human inspection.

## Final Rejection Reason

The build output is missing several key components defined in the design and source context. First, the 'check_konsolidierung_toleranz' validation step (which was present in the design as a BigQueryInsertJobOperator and in the source .mp file) has been completely dropped from the generated Airflow DAG. Second, several literal log messages from the source KSH script ('Starte monatliche Umsatzkonsolidierung...', '...mit Fehlercode... abgebrochen', and '...Fehlerzeilen im Konsolidierungs-Protokoll gefunden...') were dropped, violating the output literal preservation rule.

## Required Changes

1. Add the 'check_konsolidierung_toleranz' BigQueryInsertJobOperator back into the Airflow DAG as specified in the design.
2. Restore the missing literal log messages from 'r_umsatz_konsolidierung_monatlich.ksh' ('Starte monatliche Umsatzkonsolidierung fuer Monat...', 'Umsatzkonsolidierung fuer Monat... mit Fehlercode... abgebrochen', and '...Fehlerzeilen im Konsolidierungs-Protokoll gefunden...') either in the DAG's PythonOperators or within the PySpark script/wrapper.