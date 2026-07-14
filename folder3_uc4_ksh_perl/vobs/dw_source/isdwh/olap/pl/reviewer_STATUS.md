# Reviewer Rejected — Human Review Required

**Job:** `Shared Files — folder3_uc4_ksh_perl/vobs/dw_source/isdwh/olap/pl`

The automated reviewer rejected this output after 3 attempt(s). The generated files are committed for human inspection.

## Final Rejection Reason

The build output translates and rewrites the original German log messages (e.g., 'Fehler beim Aufruf des MetaAuth-Service', 'Ungueltige Anzahl von Parametern.') into English. These literal output strings must be preserved verbatim in the migrated code.

## Required Changes

["Restore the exact German error messages (e.g., 'Fehler beim Aufruf des MetaAuth-Service', 'Ungueltige Anzahl von Parametern.') from the source Perl script into the Python logger outputs."]