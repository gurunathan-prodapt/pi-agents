# Reviewer Rejected — Human Review Required

**Job:** `DW.DWH_ABPZ_KKM_AIL_AGENT`

The automated reviewer rejected this output after 3 attempt(s). The generated files are committed for human inspection.

## Final Rejection Reason

The build output violates CHECK 5 (Output/print literal preservation). Several literal print statements from the source files were altered, dropped, or replaced with incorrect text. For example, the start message from DW.DWH_ADM_JOB_MONITOR_START.xml ('Added &ADMJOB with &ADMNRJOB') was replaced with the end message. Additionally, messages from DW.DWH_ADM_PRUEFE_AB_INITIO_START_INC.xml and DW.DWH_ADM_PRUEFE_AB_INITIO_ENDE_INC.xml were reworded (e.g., 'Der Prüfung läuft für' changed to 'Der Prüfjob ... läuft im Jobplan') and dropped the '(&TIME &DATE)' and 'PRÜFE ...' substrings.

## Required Changes

["Restore the exact literal print statement 'Added {ADMJOB} with {ADMNRJOB}' for the DW.DWH_ADM_JOB_MONITOR_START task, replacing the duplicated end message.", "Restore the exact literal print statements for DW.DWH_ADM_PRUEFE_AB_INITIO_START_INC, including 'Der Prüfung läuft für {JOBNAME} im Jobplan {JOBPLANNAME}', 'Der Status für die Applikation {APPLIKATION} ist: {STATUS_APPL} ({TIME} {DATE})', 'PRÜFE ... ({TIME} {DATE})', and 'Prüfung erfolgreich, starte Ab Initio Job(s) ({TIME})'.", "Restore the exact literal print statements for DW.DWH_ADM_PRUEFE_AB_INITIO_ENDE_INC, ensuring '({TIME} {DATE})' is included in the status output line."]