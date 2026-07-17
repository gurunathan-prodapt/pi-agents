# Reviewer Rejected — Human Review Required

**Job:** `DWH/DWH_KERN/PRODUKTION/DW.DWH_KUNDE/DW.DWH_KUNDE_ABGL_WOECHENTLICH_JP.xml`

The automated reviewer rejected this output after 3 attempt(s). The generated files are committed for human inspection.

## Final Rejection Reason

The design document contains three separate, conflicting drafts concatenated together, which causes the build output to generate multiple duplicated and conflicting files (e.g., `dw_dwh_kunde_abgl_woechentlich.py` vs `dag_abgl_kunde_woech.py`). Additionally, the final implementation in `dag_abgl_kunde_woech_bin.py` drops required literal strings from the source shell script, such as 'Anzahl gefundener Abweichungen: ...' and 'Adressabgleich Kundenstammdaten ohne erkennbare Fehler beendet'.

## Required Changes

1. Provide a single, unified design document instead of concatenating multiple drafts.
2. Generate only the necessary target files without duplicates.
3. Ensure all literal print/echo statements from the source shell script (e.g., 'Anzahl gefundener Abweichungen: ...' and 'Adressabgleich Kundenstammdaten ohne erkennbare Fehler beendet') are preserved verbatim in the final Python/Airflow code.