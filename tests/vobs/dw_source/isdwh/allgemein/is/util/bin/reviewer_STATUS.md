# Reviewer Rejected — Human Review Required

**Job:** `Shared Files — vobs/dw_source/isdwh/allgemein/is/util/bin`

The automated reviewer rejected this output after 3 attempt(s). The generated files are committed for human inspection.

## Final Rejection Reason

The build output violates the literal preservation rule (Check 5). Several German echo statements from `h_alis_sqlplus.ksh` (e.g., "Rufe SQL*PLUS auf mit folgenden Einstellungen", "Sql*Plus-Skript : ", "Skript-Parameter: ") were either completely dropped or translated into English ("Executing script: ... acting under context of user: ..."). Additionally, in `f_alis_msgerr.ksh`, the echo "Fehler wurde von der Shell gemeldet, setze auf Abbruchstatus" was merged with the error code message, altering its original formatting, and the error "Argh!, keinen Variablennamen bei ErmittleNr angegeben" was relegated to a comment in its respective procedure.

## Required Changes

['Restore the exact German logging messages in `starteSQLSkript`, `starteSQLSkriptStrict`, and `starteSQLSkriptUser` (e.g., "Rufe SQL*PLUS auf mit folgenden Einstellungen", "Sql*Plus-Skript : ", "Skript-Parameter: ", "User            : ").', 'In `DWMSG_Fehlerbehandlung`, separate the literal "Fehler wurde von der Shell gemeldet, setze auf Abbruchstatus" from the "ErrorCode ist: " message so both match the source exactly.', 'In `DWMSG_ErmittleNr`, implement the literal error "Argh!, keinen Variablennamen bei ErmittleNr angegeben" as an actual raised error or log, rather than just a comment.']