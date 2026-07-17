# Reviewer Rejected — Human Review Required

**Job:** `DWH/DWH_KERN/PRODUKTION/DW.DWH_KUNDE/DW.DWH_KUNDE_ABGL_WOECHENTLICH_JP.xml`

The automated reviewer rejected this output after 3 attempt(s). The generated files are committed for human inspection.

## Final Rejection Reason

The design document contains three completely different, concatenated migration designs for the same job. Consequently, the build output generated three separate, conflicting implementations (three different DAG files, three different Python task files, and multiple SQL files). The design and build must be regenerated to provide a single, coherent implementation.

## Required Changes

1. Provide exactly one unified design document.
2. Generate exactly one set of target files (one DAG, one set of tasks/SQL) that implements the job.
3. Ensure all literal log messages from the source KornShell script (including the warning message and the 'fuer Stichtag' part of the start message) are preserved exactly in the final implementation.