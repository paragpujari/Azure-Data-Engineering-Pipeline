# Interview discussion guide

1. Why use Bronze/Silver/Gold? Separation of source fidelity, quality enforcement and business consumption.
2. ADF vs Databricks? ADF orchestrates movement and dependencies; Databricks performs distributed transformation.
3. ADLS Gen2? Durable lake storage with hierarchical namespace and access-control options.
4. Why Delta Lake? Transaction log, schema enforcement, ACID operations and MERGE support.
5. What is a watermark? A high-water mark such as last modified timestamp; update only after a successful validated load.
6. How do you make reruns safe? Deterministic paths, run IDs, deduplication/business keys, idempotent overwrite or Delta MERGE.
7. How do you troubleshoot slow Spark jobs? Inspect Spark UI, stages, shuffle volume, skew, task duration, partition count and file sizes.
8. How do you secure the pipeline? Managed identities, least privilege, Key Vault, private endpoints where required, and no secrets in source control.
9. What would you monitor? ADF run status/duration, freshness, row counts, rejected rows, schema changes and KPI reconciliation.
10. What is not implemented yet? A fully deployed Azure subscription, production watermark/control table, configured ADF services and live Power BI report. Be transparent about this.
