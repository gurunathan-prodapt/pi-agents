# Reviewer Rejected — Human Review Required

**Job:** `DW.RPOS_CARM_IMPORT`

The automated reviewer rejected this output after 3 attempt(s). The generated files are committed for human inspection.

## Final Rejection Reason

The design document contains two conflicting target file plans. In the first half, it incorrectly claims that the source files for the wrapper script and Ab Initio graph are missing, providing pseudocode that raises `NotImplementedError`. In the second half, it correctly identifies the source files and provides the actual PySpark implementation. This contradiction caused the build agent to generate duplicate DAGs and scripts, including broken stubs that raise `NotImplementedError`. The design must be regenerated to provide a single, consistent file plan that correctly uses the provided source files without generating error stubs.

## Required Changes

1. Provide a single, unified 'TARGET FILE PLAN' that correctly maps the source files to their target implementations.
2. Do not generate `NotImplementedError` stubs for files that are present in the source context.
3. Ensure only one Airflow DAG is generated for the job, and it points to the correct, fully implemented PySpark and wrapper scripts.
4. Remove any conflicting file plans or pseudocode sections that instruct the build agent to generate duplicate or broken files.