# Reviewer Rejected — Human Review Required

**Job:** `Shared Files — vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin`

The automated reviewer rejected this output after 3 attempt(s). The generated files are committed for human inspection.

## Final Rejection Reason

Generated Python calls source-language primitives that were never translated — these are undefined at runtime and raise NameError:
  - vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.py: SYSDATE

## Required Changes

Replace each untranslated primitive with its target-platform equivalent (e.g. string_lrtrim -> trim(), string_substring -> substring(), re_index -> regexp_instr()/locate(), SYSDATE -> current_timestamp()).