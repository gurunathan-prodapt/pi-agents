# Reviewer Rejected — Human Review Required

**Job:** `DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JS`

The automated reviewer rejected this output after 3 attempt(s). The generated files are committed for human inspection.

## Final Rejection Reason

The build output fails to preserve the literal log messages from the source KornShell script (r_exp_rechnung_taeglich.ksh). The original German messages were translated into English and reworded (e.g., '[WARNING] Export file ... contains 0 rows!' instead of 'Keine Rechnungsdaten fuer Stichtag ... exportiert', and '[SUCCESS] Export completed...' instead of 'Export Rechnungsdaten ohne erkennbare Fehler beendet').

## Required Changes

["Restore the exact log message: 'Starte Export Rechnungsdaten fuer Stichtag {stichtag}'", "Restore the exact log message: 'Anzahl exportierter Rechnungssaetze: {line_count}'", "Restore the exact warning message: 'Keine Rechnungsdaten fuer Stichtag {stichtag} exportiert'", "Restore the exact success message: 'Export Rechnungsdaten ohne erkennbare Fehler beendet'"]