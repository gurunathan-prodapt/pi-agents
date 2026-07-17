# Reviewer Rejected — Human Review Required

**Job:** `DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JS`

The automated reviewer rejected this output after 3 attempt(s). The generated files are committed for human inspection.

## Final Rejection Reason

The design document contains multiple conflicting sections (three different file disposition tables and architectures), leading to a chaotic build output with duplicate files (e.g., `r_exp_rechnung_taeglich.py` vs `r_exp_rechnung_taeglich_operator.py`). Furthermore, the design explicitly fabricates log messages in its 'Verbatim Print Catalog' (e.g., 'Keine Rechnungsdaten gefunden.' instead of 'Keine Rechnungsdaten fuer Stichtag $l_Stichtag exportiert'), which the build then implements. The design must be unified into a single coherent architecture, and all original print literals must be preserved character-for-character.

## Required Changes

1. Consolidate the design document into a single, coherent architecture with one clear File Disposition table and one set of target files.
2. Ensure the design accurately captures the exact literal strings from the source files (e.g., 'Rechnungsexport fuer Stichtag {stichtag} angestossen', 'Starte Export Rechnungsdaten fuer Stichtag {stichtag}', 'Keine Rechnungsdaten fuer Stichtag {stichtag} exportiert', 'Anzahl exportierter Rechnungssaetze: {count}', 'Export Rechnungsdaten ohne erkennbare Fehler beendet').
3. Update the build output to generate only the unified set of files, implementing the exact literal strings without paraphrasing or truncating.