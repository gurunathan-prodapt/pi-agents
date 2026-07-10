# Blocked — Real Source Required

**Job:** `DW.BERT_P_GESCHAEFTSP`

This job references one or more components with **no real source file anywhere in the scanned codebase**. Design and Build ran, but any code generated for those components is a best-guess stub, not a faithful migration — it must not be merged as-is.

## What's missing

2 referenced component(s) have no real source file anywhere in the scanned codebase: DW.BERT_LESE_LOG, DW.HOLE_PFAD. Design/Build cannot produce a faithful migration for these — any generated code for them is a best-guess stub, not a real conversion, and must not be merged without a human supplying or confirming the actual source.

## Next steps

A human must locate the real source for each unresolved component (or confirm one of the suggested candidates below) before this job can be considered migrated:
  (no candidate file found for any of them)