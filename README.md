# Azure Data Engineering Pipeline — Retail Sales

**Tech stack:** Azure Data Factory (ADF) · Azure Data Lake Storage Gen2 (ADLS Gen2) · Azure Databricks · PySpark · Delta Lake · SQL

An end-to-end portfolio project demonstrating batch ingestion, lakehouse transformations, data quality checks, incremental-load concepts, and analytics-ready data products using a synthetic retail sales dataset.

> **Important:** This repository contains runnable local PySpark logic, SQL analytics, sample data, and starter ADF JSON templates. It does not create Azure resources or include credentials. Deploy and configure the Azure services in your own subscription before claiming a live Azure deployment.

## Architecture

```text
Synthetic CSV / Source System
          |
          v
Azure Data Factory (scheduled copy + parameters)
          |
          v
ADLS Gen2 / Bronze (immutable raw files)
          |
          v
Azure Databricks + PySpark
  - schema enforcement
  - null / duplicate checks
  - standardization and business rules
          |
          v
Delta Lake / Silver (cleaned transaction rows)
          |
          v
Delta Lake / Gold (daily, product, regional KPIs)
          |
          v
SQL analytics / Power BI-ready tables
```

## Repository structure

```text
azure-data-engineering-pipeline/
├── azure/adf/                 # Starter pipeline, dataset and linked-service templates
├── data/raw/retail_sales.csv  # 1,000 synthetic rows; no personal/customer data
├── data/sample_output/        # Local output examples can be written here
├── docs/                      # Architecture, setup and interview notes
├── notebooks/                 # Databricks notebook source
├── sql/                       # Analytics queries and quality checks
├── src/                       # Local PySpark transformation entry point
├── tests/                     # Lightweight data-rule tests
├── requirements.txt
└── README.md
```

## Data model

| Layer | Object | Grain | Purpose |
|---|---|---|---|
| Bronze | `bronze_retail_sales` | Source row | Preserve ingested source data |
| Silver | `silver_retail_sales` | Valid order line | Typed, standardized, quality-checked data |
| Gold | `gold_daily_sales` | Order date + region | Revenue, cost, profit and order KPIs |
| Gold | `gold_product_performance` | Product | Product/category profitability |
| Gold | `gold_regional_performance` | Region | Regional performance comparison |

### Business definitions
- **Gross sales** = quantity × unit price
- **Net sales** = gross sales × (1 − discount percentage)
- **Cost** = quantity × unit cost
- **Profit** = net sales − cost
- **Profit margin %** = profit ÷ net sales × 100 (null when net sales is zero)
- Cancelled orders are excluded from business KPIs; returned orders are kept separately and should be treated according to the business's agreed accounting policy.

## Run locally

Python 3.10+ is recommended.

```bash
python -m venv .venv
# Windows: .venv\Scripts\activate
# macOS/Linux: source .venv/bin/activate
pip install -r requirements.txt
python src/retail_pipeline.py --input data/raw/retail_sales.csv --output data/sample_output
```

The script uses PySpark and writes Parquet outputs. For Delta Lake, run the transformations in Databricks or configure a compatible Delta Lake runtime.

## Azure implementation steps

1. Create a resource group, ADLS Gen2 storage account, and containers/folders: `bronze`, `silver`, `gold`.
2. Create an Azure Data Factory instance and configure a managed identity.
3. Grant the ADF identity the minimum required Storage Blob Data Contributor access on the target storage account/container.
4. Create source and ADLS linked services. Use managed identity where supported; do not commit secrets.
5. Import/adapt the starter ADF pipeline JSON under `azure/adf/pipelines/`. Parameterize source path, target container, and load date.
6. Configure a Copy activity to land source files under `bronze/retail_sales/load_date=YYYY-MM-DD/`.
7. Create an Azure Databricks workspace and compute. Import `notebooks/retail_sales_lakehouse.py`.
8. Set catalog/schema and storage paths for your environment. Run bronze → silver → gold transformations.
9. Add a Databricks Notebook activity to ADF after the Copy activity; pass the bronze path and target paths as parameters.
10. Add schedule/event trigger, retries, timeout, failure notifications, and monitoring.
11. Query Gold tables from Databricks SQL or publish them to a serving layer for Power BI.
12. Validate row counts, quality metrics, and KPI reconciliation before presenting results.

## Incremental loading design

For a production version, add a control table with `source_name`, `last_successful_watermark`, `run_id`, and status. Read rows where `modified_timestamp > last_successful_watermark`; write to a staging Delta table; use Delta `MERGE` keyed by the business key; update the watermark only after all validation succeeds. The sample CSV does not contain a modification timestamp, so the included implementation is a full-file batch demonstration rather than a falsely claimed incremental pipeline.

## Data quality rules
- `order_id` must be present and unique.
- `order_date`, `product_id`, `quantity`, `unit_price`, and `unit_cost` must be valid.
- Quantity must be positive; prices/costs must be non-negative.
- Discount must be between 0 and 1.
- Invalid rows are separated into a reject dataset for review.
- Business KPIs exclude cancelled transactions.

## Portfolio talking points
- Explain why the Bronze/Silver/Gold architecture separates source fidelity, quality rules, and business aggregates.
- Discuss schema enforcement, idempotency, late-arriving data, retries, watermark management, and `MERGE`.
- Compare partitioning by date with over-partitioning; partition only when data volume and query patterns justify it.
- Use Spark UI to inspect shuffles, skew, task duration, and file sizes.
- Explain security with managed identities, least privilege, secret scopes/Key Vault, and private networking.

## Limitations and next improvements
- The ADF JSON is a starter template; linked-service IDs, dataset paths, notebook parameters, authentication, and Azure resource names must be configured for your subscription.
- Local PySpark output is Parquet. Delta writes are demonstrated in the Databricks notebook.
- Add CI tests, a control/watermark table, alerting, and a Power BI report after validating in Azure.
