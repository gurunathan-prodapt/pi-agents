# Reviewer Rejected — Human Review Required

**Job:** `Shared Files — vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin`

The automated reviewer rejected this output after 3 attempt(s). The generated files are committed for human inspection.

## Final Rejection Reason

The build output drops or reworks literal strings from the source files. In `f_alis_msgerr.py`, the literal 'Argh!, keinen Variablennamen bei ErmittleNr angegeben' was dropped entirely from the `dwmsg_ermittle_nr` function. In `h_alis_date.py`, the literal '   1 Zeile erwartet, $anzahl Zeile(n) bekommen' was reworked to hardcode '0' instead of preserving the variable interpolation. All literal output text must be preserved exactly as it appeared in the source.

## Required Changes

(see explanation above)
## Per-File Review Results

- ❌ `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.ksh`
  - 1. Restore the dropped literal `Argh!, keinen Variablennamen bei ErmittleNr angegeben` in the `dwmsg_ermittle_nr` function. Even if the function signature was changed to return a value instead of taking a variable name, you must still accept the variable name parameter (e.g., `var_name=None`) and print this exact literal if it is missing, to preserve the original error handling behavior.
- ❌ `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_date.ksh`
  - 1. Restore the exact formatting of the literal `1 Zeile erwartet, $anzahl Zeile(n) bekommen` in `dw_date_gib_zeitraum`. Do not hardcode `0`; use variable interpolation (e.g., `f"   1 Zeile erwartet, {anzahl} Zeile(n) bekommen"`), defining or mocking the `anzahl` variable as needed to match the original output.
- ✅ `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_parameter.ksh`
- ✅ `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_sqlplus.ksh`