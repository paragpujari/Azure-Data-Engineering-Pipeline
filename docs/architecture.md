# Architecture and operational design

## Layers
- **Bronze:** raw source file plus ingestion metadata; retain source for replay/audit.
- **Silver:** typed records, standardized dimensions, duplicate handling, valid rows and reject rows.
- **Gold:** daily-by-region, product/category, and regional aggregates for analytics.

## ADF orchestration
1. Trigger starts a run and passes `load_date` / source path parameters.
2. Copy activity lands the source file into a date-partitioned Bronze folder.
3. Notebook activity receives the Bronze input and Silver/Gold output locations.
4. Validation activity checks expected row counts and quality thresholds.
5. Success updates the control/watermark table; failure routes to alerting and does not advance the watermark.

## Production-readiness checklist
- Managed identity / service principal with least privilege
- Key Vault for secrets; never commit tokens or connection strings
- Retry policy, timeout, concurrency control and failure path
- Run ID, audit table, row-count reconciliation and rejected-row retention
- Idempotent writes and a defined late-arriving-data policy
- Monitoring for freshness, failed runs, volume anomalies and data quality
- Partition/file-size strategy validated with actual workload
