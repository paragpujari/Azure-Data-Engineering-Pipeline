# Azure Data Engineering Pipeline | Retail Sales Analytics

An end-to-end data engineering project demonstrating batch data ingestion, data cleansing, transformation, lakehouse architecture, and analytics-ready data preparation using Microsoft Azure services.

## Project Overview

This project builds a scalable data pipeline for retail sales data using Azure Data Factory, Azure Data Lake Storage Gen2, Azure Databricks, PySpark, Delta Lake, and SQL.

The pipeline follows the **Bronze–Silver–Gold architecture** to ingest raw data, clean and transform records, calculate business metrics, and prepare structured datasets for reporting and analytics.

## Tech Stack

- **Orchestration:** Azure Data Factory (ADF)
- **Cloud Storage:** Azure Data Lake Storage Gen2 (ADLS Gen2)
- **Data Processing:** Azure Databricks, PySpark
- **Storage Format:** Delta Lake, Parquet
- **Analytics:** SQL
- **Programming:** Python
- **Version Control:** Git, GitHub

## Architecture

```text
Retail Sales CSV
       |
       v
Azure Data Factory
(Data Ingestion & Orchestration)
       |
       v
ADLS Gen2 — Bronze Layer
(Raw Data)
       |
       v
Azure Databricks + PySpark
(Data Cleaning & Transformation)
       |
       v
Silver Layer
(Cleaned & Validated Data)
       |
       v
Gold Layer
(Aggregated Business Metrics)
       |
       v
SQL Analytics / Power BI-Ready Data
```

## Project Objectives

- Build an end-to-end batch data processing pipeline.
- Ingest retail sales data into a cloud data lake.
- Clean, validate, and transform data using PySpark.
- Implement a medallion architecture using Bronze, Silver, and Gold layers.
- Calculate sales, cost, profit, and profitability metrics.
- Prepare analytics-ready datasets for business reporting.
- Demonstrate data quality, orchestration, and pipeline monitoring concepts.

## Dataset Description

The project uses a synthetic retail sales dataset containing approximately 1,000 records.

| Column | Description |
|---|---|
| `order_id` | Unique order identifier |
| `order_date` | Date of the transaction |
| `customer_id` | Customer identifier |
| `product_id` | Product identifier |
| `product_name` | Product name |
| `category` | Product category |
| `region` | Sales region |
| `city` | Transaction city |
| `sales_channel` | Online, retail store, or marketplace |
| `quantity` | Number of units sold |
| `unit_price` | Selling price per unit |
| `unit_cost` | Cost per unit |
| `discount_pct` | Discount percentage represented as a decimal |
| `order_status` | Completed, returned, or cancelled |

The dataset includes intentional missing values to demonstrate data quality checks and invalid-record handling. It contains synthetic data rather than real customer information.

## Medallion Architecture

### 1. Bronze Layer — Raw Data

- Ingest source CSV files.
- Preserve source records for traceability.
- Capture ingestion timestamps and source file information.
- Organize incoming data by load date in the Azure implementation.

### 2. Silver Layer — Cleaned Data

- Convert columns to appropriate data types.
- Standardize text fields.
- Remove duplicate order identifiers.
- Validate required fields and business rules.
- Separate invalid records for investigation.
- Calculate gross sales, net sales, total cost, and profit.

### 3. Gold Layer — Business Aggregations

Create analytics-ready datasets for:

- Daily sales performance by region.
- Product and category profitability.
- Regional sales performance.
- Order volumes and units sold.
- Profitability and profit margin analysis.

## Data Transformation and Business Logic

The pipeline applies the following calculations:

**Gross Sales**

`Quantity × Unit Price`

**Net Sales**

`Gross Sales × (1 − Discount Percentage)`

**Total Cost**

`Quantity × Unit Cost`

**Profit**

`Net Sales − Total Cost`

**Profit Margin (%)**

`(Profit ÷ Net Sales) × 100`

Cancelled transactions are excluded from business KPI aggregations. Returned transactions remain identifiable and should be handled according to the reporting requirements of the business.

## Data Quality Checks

- Validate required order identifiers and dates.
- Check product identifiers and numeric fields.
- Identify duplicate order records.
- Validate positive quantities.
- Reject negative prices and costs.
- Validate discounts between 0 and 1.
- Separate invalid records from valid data.
- Compare row counts across pipeline stages.

## Repository Structure

```text
Azure-Data-Engineering-Pipeline/
│
├── azure/
│   └── adf/
│       ├── pipelines/
│       │   └── PL_RetailSales_CopyToBronze.json
│       ├── datasets/
│       └── linked_services/
│
├── data/
│   ├── raw/
│   │   └── retail_sales.csv
│   └── sample_output/
│
├── docs/
│   ├── architecture.md
│   └── interview_notes.md
│
├── notebooks/
│   └── retail_sales_lakehouse.py
│
├── sql/
│   └── analytics_queries.sql
│
├── src/
│   └── retail_pipeline.py
│
├── tests/
│   └── test_business_rules.py
│
├── requirements.txt
├── .gitignore
└── README.md
```

## How to Run Locally

### Prerequisites

- Python 3.10 or later
- Java runtime compatible with your PySpark version
- Git
- Required Python dependencies

### Step 1: Clone the Repository

```bash
git clone https://github.com/paragpujari/Data-Engineer.git
cd Data-Engineer/Azure-Data-Engineering-Pipeline
```

Adjust the directory path if you upload this project under a different repository folder.

### Step 2: Create a Virtual Environment

```bash
python -m venv .venv
```

Activate it on Windows:

```bash
.venv\Scripts\activate
```

### Step 3: Install Dependencies

```bash
pip install -r requirements.txt
```

### Step 4: Run the PySpark Pipeline

```bash
python src/retail_pipeline.py --input data/raw/retail_sales.csv --output data/sample_output
```

The pipeline writes Parquet datasets for the Bronze, Silver, rejects, and Gold outputs.

### Step 5: Run the Unit Tests

```bash
pytest tests/
```

## Azure Implementation

To run the pipeline in Azure:

1. Create an Azure resource group and an ADLS Gen2 storage account.
2. Create Bronze, Silver, and Gold containers or folders.
3. Create an Azure Data Factory instance.
4. Configure source and ADLS Gen2 linked services.
5. Set up authentication using managed identity where supported.
6. Configure a Copy activity to ingest source files into the Bronze layer.
7. Create an Azure Databricks workspace and compute environment.
8. Import and configure the lakehouse notebook.
9. Configure storage paths and execute Silver and Gold transformations.
10. Connect ADF to the Databricks notebook for orchestration.
11. Configure triggers, retries, logging, and failure handling.
12. Validate output datasets and query Gold data using SQL.

**Note:** Azure resource names, authentication, linked services, dataset parameters, and storage paths must be configured for your own subscription. The included ADF JSON is a starter template, not a fully deployed pipeline.

## Incremental Loading and Scalability

The project documentation describes how to extend the batch pipeline with:

- Watermark-based incremental ingestion.
- Delta Lake `MERGE` operations for upserts.
- Idempotent pipeline execution.
- Run tracking and audit tables.
- Retry and failure-handling strategies.
- Spark performance monitoring and partition optimization.

The included sample dataset demonstrates full-file batch processing. A production incremental-loading workflow requires a suitable source modification timestamp or another reliable change-tracking mechanism.

## Expected Outcomes

The project is designed to produce structured datasets supporting:

- Daily sales and profit analysis.
- Regional performance comparisons.
- Product-level profitability analysis.
- Data quality reporting.
- Downstream Power BI dashboards.

Actual output metrics should be calculated from the executed pipeline rather than assumed in advance.

## Skills Demonstrated

- Azure Data Factory orchestration
- Azure Data Lake Storage Gen2
- Azure Databricks and PySpark
- Data ingestion and transformation
- ETL/ELT pipeline design
- Medallion architecture
- Delta Lake fundamentals
- SQL analytics and aggregations
- Data quality validation
- Git and GitHub project documentation

## Future Enhancements

- Implement production-grade incremental ingestion.
- Add a pipeline control table and watermark management.
- Integrate Azure Key Vault for secret management.
- Add automated data quality thresholds and alerts.
- Optimize Spark workloads using Spark UI and execution plans.
- Build a Power BI dashboard using Gold-layer datasets.
- Add CI/CD deployment and automated integration tests.

## Project Status

**Portfolio project — local implementation and Azure deployment preparation.**

The repository includes local PySpark processing code, sample data, SQL analytics, tests, and Azure starter templates. A live Azure deployment and Power BI dashboard require additional configuration and execution.

## Author

**Parag Pujari**

Data Engineering | SQL | Python | PySpark | Azure | Analytics

GitHub: [github.com/paragpujari](https://github.com/paragpujari)

