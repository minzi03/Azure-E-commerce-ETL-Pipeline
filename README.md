# Azure E-Commerce ETL Pipeline

![Azure](https://img.shields.io/badge/Azure-Lakehouse-0078D4)
![ADF](https://img.shields.io/badge/Azure_Data_Factory-Orchestration-0078D4)
![Databricks](https://img.shields.io/badge/Databricks-PySpark-EF3E42)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-ACID-00ADD8)
![Synapse](https://img.shields.io/badge/Synapse-Serverless_SQL-0078D4)
![Power BI](https://img.shields.io/badge/Power_BI-Analytics-F2C811)

An end-to-end **Azure Lakehouse data engineering project** for e-commerce analytics, integrating heterogeneous data from **MySQL, MongoDB, REST/HTTP APIs, and CSV/local files** through **Azure Data Factory**, processing it with **Azure Databricks and Delta Lake**, and serving analytics-ready data through **Azure Synapse Serverless SQL and Power BI**.

The platform follows a **Landing → Bronze → Silver → Gold** architecture and demonstrates practical data-engineering patterns including **metadata-driven ingestion, parameterized pipelines, incremental processing, data-quality validation, idempotent Delta processing, dimensional modeling, and analytical serving**.

---

## Table of Contents

- [Project Overview](#project-overview)
- [Business Scenario](#business-scenario)
- [Architecture](#architecture)
- [Technology Stack](#technology-stack)
- [Source Systems](#source-systems)
- [End-to-End Data Flow](#end-to-end-data-flow)
- [Azure Data Factory](#azure-data-factory)
- [Landing and Bronze Layers](#landing-and-bronze-layers)
- [Silver Layer](#silver-layer)
- [Gold Layer](#gold-layer)
- [Incremental Processing](#incremental-processing)
- [Delta Lake Reliability](#delta-lake-reliability)
- [Data Quality](#data-quality)
- [Dimensional Modeling](#dimensional-modeling)
- [Delta Lake Optimization](#delta-lake-optimization)
- [Synapse Serverless SQL](#synapse-serverless-sql)
- [Power BI Analytics](#power-bi-analytics)
- [Observability](#observability)
- [Security](#security)
- [Repository Structure](#repository-structure)
- [How to Run](#how-to-run)
- [Engineering Decisions](#engineering-decisions)
- [Future Improvements](#future-improvements)

---

# Project Overview

This project implements an Azure-based Lakehouse platform for the **Brazilian Olist e-commerce dataset**.

The source data is distributed across different storage and database technologies:

- relational data in **MySQL**;
- document-oriented data in **MongoDB**;
- reference data exposed through **HTTP/REST endpoints**;
- local and CSV-based datasets.

Instead of processing these sources independently, the platform consolidates them into **Azure Data Lake Storage Gen2 (ADLS Gen2)** and applies a Medallion-style transformation architecture.

The high-level flow is:

```text
MySQL ──────────┐
MongoDB ────────┤
REST / HTTP ────┼──► Azure Data Factory
CSV / Local ────┘            │
                              ▼
                       ADLS Gen2 Landing
                              │
                              ▼
                           Bronze
                              │
                              ▼
                    Databricks / PySpark
                              │
                              ▼
                           Silver
                              │
                              ▼
                            Gold
                              │
                              ▼
                  Synapse Serverless SQL
                              │
                              ▼
                          Power BI
````

The project separates:

* **ingestion and orchestration**;
* **storage**;
* **transformation**;
* **data modeling**;
* **serving**;
* **analytics**.

This keeps the pipeline modular and makes individual layers easier to test, troubleshoot, and evolve.

---

# Business Scenario

An e-commerce organization collects operational data from multiple systems covering:

* customers;
* orders;
* order items;
* products;
* sellers;
* payments;
* reviews;
* geolocation;
* product-category reference data.

The analytical platform needs to consolidate these datasets and support questions such as:

* How is revenue changing over time?
* Which products and categories generate the most sales?
* Which regions contribute the most revenue?
* What payment methods do customers prefer?
* How efficiently are orders delivered?
* Which customers contribute the most revenue?
* How does delivery performance relate to customer satisfaction?

The platform therefore needs to:

1. ingest heterogeneous source systems;
2. avoid duplicating ingestion logic across datasets;
3. maintain raw source data for traceability;
4. standardize and validate data before analytics;
5. handle incremental source changes safely;
6. build dimensional models for analytical workloads;
7. optimize larger Delta datasets;
8. expose curated datasets through SQL;
9. support Power BI reporting;
10. provide operational visibility into pipeline executions.

---

# Architecture

![Azure Lakehouse Architecture](assets/pro_architecture.png)

The solution follows a layered architecture:

```text
┌──────────────────────────────────────────────────────────────┐
│                        SOURCE SYSTEMS                        │
│                                                              │
│     MySQL       MongoDB       REST / HTTP       CSV/Local    │
└────────┬───────────┬──────────────┬────────────────┬──────────┘
         │           │              │                │
         └───────────┴──────────────┴────────────────┘
                              │
                              ▼
┌──────────────────────────────────────────────────────────────┐
│                   AZURE DATA FACTORY                         │
│                                                              │
│  Metadata-driven ingestion                                  │
│  Parameterized datasets                                     │
│  Source orchestration                                       │
│  Incremental processing                                     │
│  Scheduled execution                                        │
└──────────────────────────────┬───────────────────────────────┘
                               │
                               ▼
┌──────────────────────────────────────────────────────────────┐
│                       ADLS GEN2                              │
│                                                              │
│  Landing → Bronze → Silver → Gold                           │
└──────────────────────────────┬───────────────────────────────┘
                               │
                               ▼
┌──────────────────────────────────────────────────────────────┐
│                  AZURE DATABRICKS                            │
│                                                              │
│  PySpark transformations                                    │
│  Delta Lake                                                 │
│  Validation / deduplication                                 │
│  MERGE / dimensional modeling                               │
│  Table optimization                                         │
└──────────────────────────────┬───────────────────────────────┘
                               │
                               ▼
┌──────────────────────────────────────────────────────────────┐
│                SYNAPSE SERVERLESS SQL                        │
│                                                              │
│  External data sources                                      │
│  OPENROWSET                                                 │
│  External tables                                            │
│  Analytical views                                           │
└──────────────────────────────┬───────────────────────────────┘
                               │
                               ▼
┌──────────────────────────────────────────────────────────────┐
│                         POWER BI                             │
│                                                              │
│  Sales | Customer | Logistics & Delivery                    │
└──────────────────────────────────────────────────────────────┘
```

---

# Technology Stack

| Layer               | Technology                          | Responsibility                            |
| ------------------- | ----------------------------------- | ----------------------------------------- |
| Sources             | MySQL, MongoDB, REST/HTTP, CSV      | Operational and reference data            |
| Integration Runtime | ADF Self-hosted Integration Runtime | Connectivity to local/on-premises sources |
| Ingestion           | Azure Data Factory                  | Source extraction and data movement       |
| Orchestration       | Azure Data Factory                  | Parameterized and scheduled workflows     |
| Storage             | ADLS Gen2                           | Lakehouse storage                         |
| Table Format        | Delta Lake / Parquet                | Transactional and analytical storage      |
| Processing          | Azure Databricks                    | Distributed data processing               |
| Transformation      | PySpark / Spark SQL                 | Cleaning, validation and modeling         |
| Serving             | Synapse Serverless SQL              | SQL access to Gold datasets               |
| Visualization       | Power BI                            | Analytical dashboards                     |
| Monitoring          | Azure Monitor / ADF diagnostics     | Pipeline observability                    |
| Languages           | Python, PySpark, SQL, JSON          | Implementation                            |

---

# Source Systems

The project demonstrates ingestion from four source categories.

## MySQL

Relational e-commerce entities include:

```text
olist_orders_dataset
olist_order_items_dataset
olist_order_payments_dataset
olist_customers_dataset
olist_sellers_dataset
```

ADF connects to MySQL through parameterized datasets and reusable ingestion logic.

For local MySQL environments, a **Self-hosted Integration Runtime** provides connectivity between the source environment and Azure.

---

## MongoDB

MongoDB stores document-oriented data such as:

```text
order_reviews
```

The ingestion configuration defines:

```json
{
  "enabled": true,
  "entity_name": "order_reviews",
  "collection_name": "order_reviews",
  "sink_folder": "bronze/mongodb/order_reviews/",
  "file_name_prefix": "order_reviews"
}
```

This keeps source-specific metadata separate from transformation logic.

---

## REST / HTTP

HTTP ingestion is used for reference datasets such as product-category translations.

Example configuration:

```json
{
  "enabled": true,
  "entity_name": "product_category_name_translation",
  "relative_url": "<relative-source-url>",
  "sink_folder": "bronze/api/product_category_name_translation/",
  "file_name_prefix": "product_category_name_translation"
}
```

ADF HTTP datasets use parameters so the same ingestion pattern can be reused across endpoints.

---

## CSV / Local Files

Local datasets such as geolocation data are ingested through the Self-hosted Integration Runtime.

Example configuration:

```json
{
  "enabled": true,
  "entity_name": "geolocation",
  "source_folder": "geolocation",
  "file_name": "olist_geolocation_dataset.csv",
  "sink_folder": "bronze/local_files/geolocation/",
  "file_name_prefix": "geolocation"
}
```

---

# End-to-End Data Flow

The logical processing flow is:

```text
Sources
   │
   ▼
ADF ingestion
   │
   ▼
Landing / Bronze
   │
   ▼
Schema enforcement
   │
   ▼
Cleaning + standardization
   │
   ▼
Deduplication
   │
   ▼
Data-quality validation
   │
   ▼
Silver Delta tables
   │
   ▼
Dimensional transformations
   │
   ▼
Gold dimensions / facts / bridges
   │
   ▼
Delta optimization
   │
   ▼
Synapse Serverless SQL
   │
   ▼
Power BI
```

---

# Azure Data Factory

Azure Data Factory is responsible for source ingestion and orchestration.

## Metadata-Driven Ingestion

Rather than maintaining completely separate logic for every source table, the project externalizes source metadata into configuration files.

For MySQL, an entity list can be represented as:

```json
[
  {
    "table_name": "olist_orders_dataset",
    "file_name": "orders.csv"
  },
  {
    "table_name": "olist_order_items_dataset",
    "file_name": "order_items.csv"
  },
  {
    "table_name": "olist_order_payments_dataset",
    "file_name": "payments.csv"
  },
  {
    "table_name": "olist_customers_dataset",
    "file_name": "customers.csv"
  },
  {
    "table_name": "olist_sellers_dataset",
    "file_name": "sellers.csv"
  }
]
```

The conceptual ADF pattern is:

```text
Lookup configuration
        │
        ▼
      ForEach
        │
        ▼
Parameterized source dataset
        │
        ▼
Parameterized sink dataset
        │
        ▼
     ADLS Gen2
```

This design reduces duplicated pipeline logic and simplifies onboarding additional entities.

---

## Parameterized Datasets

ADF datasets externalize values such as:

* source table;
* relative URL;
* file name;
* destination folder;
* source entity.

For example:

```text
@dataset().file_name
@dataset().csv_relative_url
```

This allows a single dataset definition to support multiple physical objects.

---

## Self-Hosted Integration Runtime

A **Self-hosted Integration Runtime** provides connectivity from ADF to local/on-premises sources such as:

* local MySQL;
* local filesystem data.

This demonstrates hybrid data integration rather than limiting the project to cloud-hosted sources.

---

## Scheduling

ADF triggers can automate recurring pipeline execution.

Scheduling provides the foundation for periodic ingestion and incremental processing.

The exact trigger interval can be adjusted based on:

* source arrival frequency;
* freshness requirements;
* Azure cost;
* downstream processing SLAs.

---

# Landing and Bronze Layers

The ingestion architecture separates source arrival from downstream business transformation.

## Landing

The Landing layer acts as the first durable copy of ingested source data.

Its responsibilities include:

* preserving source-delivered data;
* providing replay capability;
* isolating source extraction from transformation;
* supporting troubleshooting and auditing.

Conceptually:

```text
landing/
├── mysql/
├── mongodb/
├── api/
└── local_files/
```

---

## Bronze

Bronze contains raw or minimally transformed datasets used as the starting point for Databricks processing.

Conceptually:

```text
bronze/
├── mysql/
│   ├── orders/
│   ├── order_items/
│   ├── payments/
│   ├── customers/
│   └── sellers/
│
├── mongodb/
│   └── order_reviews/
│
├── api/
│   └── product_category_name_translation/
│
└── local_files/
    └── geolocation/
```

Where applicable, technical metadata should accompany records:

```text
_source_system
_source_entity
_ingestion_timestamp
_pipeline_run_id
_source_file
```

These attributes improve traceability and simplify debugging.

---

# Silver Layer

The Silver layer converts raw datasets into standardized, validated and reusable entities.

Databricks and PySpark are used for transformation.

Typical Silver processing includes:

* explicit schema validation;
* data-type conversion;
* column standardization;
* whitespace and text normalization;
* null handling;
* duplicate detection;
* deterministic deduplication;
* timestamp normalization;
* business-rule validation;
* referential-integrity validation;
* derived attributes.

Examples of derived information include:

* delivery duration;
* order lifecycle metrics;
* review sentiment;
* standardized geographical attributes.

The Silver layer is intended to provide trusted, reusable data before dimensional modeling.

---

# Gold Layer

Gold contains analytics-oriented dimensional models and aggregates.

The project models e-commerce data into facts, dimensions, bridges, and analytical aggregates.

## Core Analytical Objects

```text
Gold
│
├── Dimensions
│   ├── dim_customer
│   ├── dim_product
│   ├── dim_seller
│   └── other conformed dimensions
│
├── Facts
│   ├── fact_sales
│   ├── fact_sales_partitioned
│   ├── fact_sales_agg
│   └── fact_order_payments_partitioned
│
└── Bridges
    └── bridge_order_items
```

The model separates descriptive entities from measurable business events.

---

## Fact Sales

`fact_sales` represents analytical sales events.

Example attributes exposed through Synapse include:

```text
fact_sales_sk
order_id
customer_sk
product_sk
seller_sk
order_status
purchase_date
delivery_date
sales_amount
shipping_cost
payment_amount
payment_type
review_score
review_sentiment
```

The fact combines transactional measures with dimension surrogate keys and business attributes required by downstream analytics.

---

# Incremental Processing

A scalable production-style pipeline should avoid full reloads of large mutable datasets whenever possible.

The project uses the watermark pattern for incremental processing.

The logical extraction condition is:

```sql
last_modified > @OldWatermark
AND last_modified <= @NewWatermark
```

The bounded interval creates a deterministic batch:

```text
Previous successful watermark
          │
          ▼
    OldWatermark
          │
          │  records processed
          ▼
    NewWatermark
          │
          ▼
Records arriving later
are handled next run
```

## Processing Sequence

```text
1. Read OldWatermark
        ↓
2. Determine NewWatermark
        ↓
3. Extract (OldWatermark, NewWatermark]
        ↓
4. Persist source batch
        ↓
5. Process downstream layers
        ↓
6. Validate processing
        ↓
7. Commit NewWatermark
```

The watermark should only advance after the required processing succeeds.

This provides safer retry behavior than updating pipeline state before the batch has been successfully processed.

---

# Delta Lake Reliability

Delta Lake provides the transactional storage foundation for processed datasets.

Key capabilities used or targeted by the pipeline include:

* ACID transactions;
* schema enforcement;
* transaction history;
* incremental upserts;
* partitioned storage;
* table maintenance.

The repository includes Delta transaction-log evidence for Gold datasets such as:

```text
dim_customer
fact_sales_partitioned
```

The Delta logs also capture table-maintenance operations such as `VACUUM`.

---

## Idempotent Processing

Incremental processing should be safe to retry.

Instead of blindly appending every incoming batch, mutable Silver/Gold datasets use deterministic keys and Delta `MERGE` semantics.

Conceptually:

```python
target.alias("target").merge(
    source.alias("source"),
    "target.business_key = source.business_key"
).whenMatchedUpdateAll(
).whenNotMatchedInsertAll(
).execute()
```

The intended property is:

```text
Process batch X
      ↓
Replay batch X
      ↓
No duplicate logical records
```

This is especially important when ADF or Databricks jobs are retried after partial failures.

---

# Data Quality

Data-quality checks are applied before data is promoted to trusted analytical datasets.

## Validation Categories

| Category              | Example                                  |
| --------------------- | ---------------------------------------- |
| Schema                | Required columns and compatible types    |
| Completeness          | Required business keys are populated     |
| Uniqueness            | Duplicate business keys                  |
| Referential Integrity | Fact keys resolve to dimensions          |
| Domain Validation     | Allowed status/payment values            |
| Numeric Validation    | Valid amounts, prices and freight values |
| Temporal Validation   | Valid order/delivery timestamps          |
| Reconciliation        | Input, valid and rejected row counts     |

A useful reconciliation invariant is:

```text
Bronze input
=
Silver valid records
+
Rejected / quarantined records
```

Invalid data should be identifiable rather than silently discarded.

---

# Dimensional Modeling

The Gold layer follows dimensional-modeling principles to make analytical queries easier and more efficient.

## Star Schema

Conceptually:

```text
                 ┌──────────────────┐
                 │   dim_customer   │
                 └────────┬─────────┘
                          │
                          │
┌───────────────┐         │        ┌───────────────┐
│  dim_product  │─────────┼────────│  dim_seller   │
└───────────────┘         │        └───────────────┘
                          │
                    ┌─────▼───────┐
                    │ fact_sales  │
                    └─────────────┘
```

Fact tables contain measurable business events while dimensions contain descriptive context.

---

## SCD Type 2

For dimensions whose attributes need historical tracking, an SCD Type 2 pattern can retain previous versions rather than overwriting them.

A historical dimension structure includes:

```text
customer_sk
customer_id
...
effective_from
effective_to
is_current
```

Example:

```text
customer_sk | customer_id | city            | is_current
------------|-------------|-----------------|-----------
101         | C001        | Sao Paulo       | false
205         | C001        | Rio de Janeiro  | true
```

When a tracked attribute changes:

1. expire the current dimension row;
2. set its effective end timestamp;
3. insert a new dimension version;
4. assign a new surrogate key;
5. mark the new version as current.

Historical facts can therefore remain associated with the dimension state that was valid when the event occurred.

---

# Analytical Scale

The project is designed to demonstrate transformation and dimensional modeling over a multi-million-row analytical fact dataset.

The target Gold fact model contains approximately:

```text
~2.9 million fact rows
```

The exact row count should be validated from the deployed Gold/Synapse environment and retained as execution evidence.

Example:

```sql
SELECT COUNT_BIG(*) AS fact_sales_row_count
FROM gold.fact_sales_table;
```

This scale makes partitioning and query-aware Delta optimization relevant to the project rather than purely theoretical.

---

# Delta Lake Optimization

Larger Gold datasets are optimized for analytical access.

Techniques include:

* partitioning;
* `OPTIMIZE`;
* Z-Ordering;
* Delta transaction-log maintenance;
* `VACUUM` with an appropriate retention policy.

## Partitioning

The repository contains a dedicated:

```text
fact_sales_partitioned
```

Delta dataset.

Partitioning should be selected according to:

* table size;
* query predicates;
* column cardinality;
* expected data distribution.

---

## OPTIMIZE and Z-Ordering

For frequently filtered analytical columns, Delta optimization can improve file layout and reduce unnecessary file scans.

Conceptually:

```sql
OPTIMIZE gold.fact_sales_partitioned
ZORDER BY (customer_sk, product_sk);
```

Optimization columns should be selected from actual query patterns rather than applied indiscriminately.

---

## VACUUM

Delta transaction logs included in the repository provide evidence of maintenance operations on:

```text
dim_customer
fact_sales_partitioned
```

`VACUUM` removes data files that are no longer referenced by the Delta transaction log after the configured retention period.

---

# Synapse Serverless SQL

The Gold layer is exposed through **Azure Synapse Serverless SQL**.

This allows analytical consumers to query Delta/Parquet data directly from ADLS without copying the complete Gold layer into another database.

---

## Managed Identity

Synapse uses a database-scoped credential backed by Managed Identity:

```sql
CREATE DATABASE SCOPED CREDENTIAL WorkspaceIdentity
WITH IDENTITY = 'Managed Identity';
```

This avoids embedding storage credentials directly in analytical queries.

---

## External Data Source

The Gold ADLS location is registered as an external data source:

```sql
CREATE EXTERNAL DATA SOURCE goldlayer
WITH (
    LOCATION = '<ADLS-GOLD-LOCATION>',
    CREDENTIAL = WorkspaceIdentity
);
```

---

## Delta Views

Gold Delta datasets can be exposed through `OPENROWSET`.

Example:

```sql
CREATE OR ALTER VIEW gold.fact_sales_partitioned AS
SELECT *
FROM OPENROWSET(
    BULK 'fact_sales_partitioned/',
    DATA_SOURCE = 'goldlayer',
    FORMAT = 'DELTA'
) AS rows;
```

Other serving objects include:

```text
gold.fact_sales
gold.fact_sales_agg
gold.fact_sales_partitioned
gold.fact_order_payments_partitioned
gold.bridge_order_items
```

---

## External Table

The project also demonstrates external table registration for `fact_sales`.

```sql
CREATE EXTERNAL TABLE gold.fact_sales_table
(
    fact_sales_sk BIGINT,
    order_id NVARCHAR(100),
    customer_sk NVARCHAR(100),
    product_sk NVARCHAR(100),
    seller_sk NVARCHAR(100),
    order_status NVARCHAR(50),
    purchase_date DATE,
    delivery_date DATE,
    sales_amount FLOAT,
    shipping_cost FLOAT,
    payment_amount FLOAT,
    payment_type NVARCHAR(50),
    review_score INT,
    review_sentiment NVARCHAR(50)
)
WITH (
    LOCATION = 'fact_sales/',
    DATA_SOURCE = goldlayer,
    FILE_FORMAT = extfileformat
);
```

Synapse therefore acts as the SQL serving layer between Lakehouse storage and downstream BI consumers.

---

# Power BI Analytics

Power BI consumes curated analytical datasets exposed through the serving layer.

The project includes three primary analytical perspectives.

## Sales Dashboard

![Sales Dashboard](assets/powerbi/Sales_Dashboard.png)

Focus areas include:

* revenue;
* order volume;
* sales trends;
* product/category performance;
* geographical performance;
* payment behavior.

---

## Customer Insights

![Customer Insights](assets/powerbi/Customer_Insights_Dashboard.png)

Focus areas include:

* customer distribution;
* customer purchasing behavior;
* revenue contribution;
* repeat purchasing;
* geographical segmentation.

---

## Logistics & Delivery

![Logistics Dashboard](assets/powerbi/Logistics\&Delivery_Dashboard.png)

Focus areas include:

* delivery duration;
* delayed orders;
* regional delivery performance;
* freight cost;
* customer review behavior.

---

# Business Insights

Analysis of the e-commerce data produced several useful observations.

## Payment Behavior

Credit-card payments account for approximately **74%** of payment activity, making cards the dominant payment method.

## Revenue Growth

Revenue increased by approximately **20% year over year** across the analyzed 2017–2018 period.

## Regional Concentration

The Southeast region contributes approximately **70% of total revenue**, indicating significant geographical concentration.

## Delivery Performance

Average delivery time is approximately **10–12 days**.

Delivery performance is therefore an important dimension for logistics and customer-experience analysis.

## Customer Retention

Repeat customers represent only a small portion of the customer population, approximately **3%**, indicating an opportunity for stronger retention strategies.

---

## Business Recommendations

Based on these findings:

* improve logistics efficiency and investigate delayed-delivery regions;
* develop retention and loyalty initiatives;
* prioritize high-performing geographical markets while identifying growth opportunities elsewhere;
* optimize marketing around observed purchasing patterns;
* monitor customer satisfaction alongside delivery performance.

---

# Observability

A production-oriented data platform requires visibility into both orchestration and data processing.

The monitoring design covers:

```text
ADF pipeline execution
        │
        ├── Status
        ├── Duration
        ├── Rows read
        ├── Rows written
        └── Activity failures
                │
                ▼
        Databricks processing
                │
                ├── Batch/run ID
                ├── Input rows
                ├── Inserted rows
                ├── Updated rows
                ├── Rejected rows
                └── Processing duration
```

Azure Monitor / Log Analytics can be used to centralize ADF diagnostic telemetry and operational alerts.

Important metrics include:

* pipeline success/failure;
* execution duration;
* row counts;
* rejected-record counts;
* watermark boundaries;
* processing freshness;
* Databricks job failures;
* unexpected zero-row loads.

A common pipeline/run identifier should be propagated through the processing layers where possible to simplify end-to-end troubleshooting.

---

# Security

Secrets and environment-specific credentials should never be stored directly in the public repository.

Production deployments should prefer:

* Azure Managed Identity;
* Azure Key Vault;
* Azure RBAC;
* Databricks secret scopes;
* least-privilege permissions;
* parameterized environment configuration.

The repository should not contain:

```text
database passwords
MongoDB passwords
Azure storage keys
SAS tokens
Databricks PATs
service-principal secrets
private connection strings
```

Any credential that has previously been committed to Git should be treated as compromised, rotated, and removed from Git history.

Environment-specific values should use placeholders such as:

```text
<STORAGE_ACCOUNT>
<MYSQL_HOST>
<MONGODB_URI>
<MASTER_KEY_PASSWORD>
<SUBSCRIPTION_ID>
```

---

# End-to-End Validation

The complete architecture can be validated using a known source record.

```text
Known source insert/update
          │
          ▼
ADF ingestion
          │
          ▼
Expected Landing/Bronze record
          │
          ▼
Silver validation + MERGE
          │
          ▼
Gold dimensional model
          │
          ▼
Synapse SQL
          │
          ▼
Power BI serving layer
```

For incremental tests, useful validation metrics include:

```text
Source rows changed
ADF rows read
ADF rows copied
Bronze rows received
Silver rows inserted
Silver rows updated
Silver rows rejected
Gold rows affected
```

Keeping these metrics with a pipeline run ID makes project claims reproducible and easier to verify.

---

# Repository Structure

```text
Azure-E-commerce-ETL-Pipeline/
│
├── README.md
│
├── assets/
│   ├── pro_architecture.png
│   └── powerbi/
│       ├── Sales_Dashboard.png
│       ├── Customer_Insights_Dashboard.png
│       └── Logistics&Delivery_Dashboard.png
│
├── azure_data_factory/
│   ├── dataset/
│   │   ├── CSVFromLinkedServiceToSink.json
│   │   ├── DataFromGithubViaLinkedService.json
│   │   ├── MySql_Customers.json
│   │   ├── MySql_Items.json
│   │   ├── MySql_Orders.json
│   │   ├── MySql_Payments.json
│   │   ├── MySql_Sellers.json
│   │   └── ...
│   │
│   ├── linkedService/
│   │   ├── ADLSForCSV.json
│   │   ├── httpGithubLinkedService.json
│   │   ├── MySQLDB.json
│   │   └── ...
│   │
│   ├── integrationRuntime/
│   │   └── OnPremise-to-Azure-integrationRuntime.json
│   │
│   ├── trigger/
│   │   └── trigger.json
│   │
│   ├── ForEachInput.json
│   └── ForEachInput_MySQL.json
│
├── azure_adls/
│   └── gold/
│       ├── dim_customer/
│       │   └── _delta_log/
│       └── fact_sales_partitioned/
│           └── _delta_log/
│
├── azure_synapse/
│   ├── 01_setup_schema_credential.sql
│   ├── 02_create_file_format_datasource.sql
│   ├── 04_create_fact_bridge_views.sql
│   ├── 05_register_external_table_fact_sales.sql
│   └── new/
│       ├── 01_setup.sql
│       ├── 02_external_objects.sql
│       └── 08_grant_role.sql
│
├── notebooks/
│   └── DataIngestion_MongoDB.ipynb
│
├── http_bronze_config.json
├── local_bronze_config.json
└── mongodb_bronze_config.json
```

As the project is productionized further, transformation notebooks, validation scripts, monitoring artifacts, and pipeline exports should be organized into dedicated folders.

---

# How to Run

## 1. Provision Azure Resources

Create the required Azure resources:

* Azure Data Factory;
* ADLS Gen2;
* Azure Databricks;
* Azure Synapse Analytics;
* Azure Key Vault where required;
* Log Analytics workspace for centralized monitoring.

---

## 2. Configure ADLS Gen2

Create the Lakehouse storage hierarchy:

```text
landing/
bronze/
silver/
gold/
```

Grant the required identities access through Azure RBAC.

---

## 3. Configure Source Connectivity

Configure connections for:

```text
MySQL
MongoDB
HTTP / REST
Local files
```

Use a Self-hosted Integration Runtime for sources that are not directly accessible from Azure.

Do not hard-code credentials in repository artifacts.

---

## 4. Deploy ADF Artifacts

Deploy:

* linked services;
* datasets;
* integration runtime references;
* ingestion pipelines;
* triggers.

Update environment-specific parameters before deployment.

---

## 5. Run Ingestion

Execute the ADF ingestion workflow.

Verify that source data arrives in the expected Landing/Bronze paths.

---

## 6. Execute Databricks Transformations

Run the transformation sequence:

```text
Bronze
   ↓
Silver
   ↓
Gold
```

Validate:

* schemas;
* duplicate handling;
* business keys;
* referential integrity;
* dimensional relationships;
* row reconciliation.

---

## 7. Create Synapse Serving Objects

Execute the SQL scripts under:

```text
azure_synapse/
```

Create:

* schema;
* credential;
* external data source;
* external file format;
* views;
* external tables.

---

## 8. Validate Synapse

Example:

```sql
SELECT COUNT_BIG(*)
FROM gold.fact_sales_table;

SELECT TOP 100 *
FROM gold.fact_sales_partitioned;
```

---

## 9. Connect Power BI

Connect Power BI to the Synapse serving layer and build the analytical model over curated Gold datasets.

---

# Engineering Decisions

## Why Azure Data Factory?

ADF provides managed orchestration and connectivity across relational, HTTP, file-based, and Azure-native data sources.

It also supports parameterization, metadata-driven processing, scheduling, monitoring, and hybrid connectivity through Self-hosted Integration Runtime.

---

## Why Metadata-Driven Ingestion?

Creating one pipeline for every table produces unnecessary duplication.

Metadata-driven ingestion separates:

```text
WHAT to ingest
```

from:

```text
HOW to ingest it
```

This makes onboarding additional datasets easier and reduces maintenance overhead.

---

## Why Medallion Architecture?

Separating Landing, Bronze, Silver, and Gold provides clear responsibilities:

| Layer   | Responsibility                           |
| ------- | ---------------------------------------- |
| Landing | Preserve source delivery                 |
| Bronze  | Raw/minimally transformed Lakehouse data |
| Silver  | Validated and standardized entities      |
| Gold    | Analytics-ready dimensional models       |

This improves traceability, debugging, reuse, and maintainability.

---

## Why Delta Lake?

Delta Lake adds transactional semantics to Lakehouse storage.

It supports:

* ACID transactions;
* schema enforcement;
* MERGE-based processing;
* transaction history;
* table optimization;
* reliable incremental processing.

---

## Why Idempotent MERGE?

Distributed pipelines can be retried after failures.

A pure append strategy can therefore create duplicates.

Business-key-aware `MERGE` allows a previously processed batch to be safely replayed while preserving the intended logical state.

---

## Why SCD Type 2?

Some analytical dimensions change over time.

Overwriting those attributes destroys historical context.

SCD Type 2 preserves historical versions using:

```text
surrogate key
effective_from
effective_to
is_current
```

This enables facts to be analyzed against the dimensional state that existed when the event occurred.

---

## Why Synapse Serverless SQL?

Synapse Serverless SQL exposes Gold datasets directly from ADLS using SQL.

For this project, it avoids requiring a separate dedicated warehouse while providing a familiar serving interface for BI workloads.

---

## Why Power BI Consumes Gold?

Heavy cleaning and transformation logic belongs upstream.

Power BI therefore consumes curated Gold datasets instead of repeatedly reproducing transformation logic inside reports.

This keeps the reporting layer focused on:

* measures;
* semantic relationships;
* visualization;
* business analysis.

---

# Current Capabilities

The repository currently demonstrates concrete artifacts for:

* multi-source ingestion;
* parameterized ADF datasets;
* Self-hosted Integration Runtime;
* ADLS Gen2 storage;
* Delta Lake transaction history;
* partitioned Gold datasets;
* Synapse external objects;
* Managed Identity-based Synapse storage access;
* Power BI analytical outputs.

Additional implementation and validation artifacts are being organized for:

* bounded watermark incremental loading;
* reusable Delta MERGE processing;
* SCD Type 2 validation;
* systematic data-quality reporting;
* Azure Monitor / Log Analytics observability;
* end-to-end run reconciliation.

This distinction keeps the repository transparent about what is directly reproducible from the committed artifacts.

---

# Future Improvements

Planned production-oriented enhancements include:

* complete ADF pipeline exports;
* centralized metadata/control tables;
* automated watermark state management;
* standardized Databricks Bronze/Silver/Gold notebooks;
* automated data-quality framework;
* quarantine datasets;
* automated SCD Type 2 tests;
* idempotency/replay tests;
* Azure Monitor alerts and SLA monitoring;
* Databricks job orchestration;
* CI/CD for ADF, Databricks, and Synapse;
* Infrastructure as Code using Terraform or Bicep;
* Unity Catalog governance and lineage;
* automated PySpark unit and integration tests;
* environment-specific Dev/Test/Prod configuration;
* cost and performance benchmarking.

---

# Resume Mapping

The project is being structured so that major engineering claims can be traced from the resume to implementation evidence.

| Engineering Capability | Repository Evidence                           |
| ---------------------- | --------------------------------------------- |
| Azure Lakehouse        | Architecture + ADLS + Databricks              |
| MySQL ingestion        | ADF MySQL datasets / linked services          |
| MongoDB ingestion      | MongoDB config + notebook                     |
| REST/HTTP ingestion    | HTTP dataset / linked service / config        |
| CSV ingestion          | Local config + Self-hosted IR                 |
| Metadata-driven ADF    | ForEach metadata + parameterized datasets     |
| Delta Lake             | Gold Delta transaction logs                   |
| Partitioning           | `fact_sales_partitioned`                      |
| Synapse Serverless     | External data source, views and tables        |
| Power BI               | Analytical dashboard assets                   |
| Watermark processing   | Incremental pipeline/control artifacts        |
| Idempotent MERGE       | Databricks transformation + replay validation |
| SCD Type 2             | Dimension implementation + history validation |
| Data quality           | Validation rules + run metrics                |
| ~2.9M fact rows        | Gold/Synapse row-count evidence               |
| OPTIMIZE / Z-Ordering  | Databricks optimization code + history        |
| Azure Monitor          | Diagnostic settings + Log Analytics evidence  |

The objective is to make every major project claim independently verifiable from repository artifacts.

---

# Resume Summary

Once the corresponding implementation and validation artifacts are committed, the project supports the following concise resume description:

> Built an end-to-end **Azure Lakehouse** (Landing → Bronze → Silver → Gold) ingesting data from **MySQL, MongoDB, REST APIs, and CSV** through reusable, metadata-driven, parameterized **ADF** pipelines with watermark-based incremental processing.

> Engineered **PySpark/Delta Lake** pipelines with schema validation, deduplication, idempotent **MERGE**, data-quality checks, and **SCD Type 2 Star Schema** modeling for a **2.9M-row fact table**; optimized Delta tables with partitioning, **OPTIMIZE/Z-Ordering** and served analytics through **Synapse and Power BI** with Azure Monitor-based pipeline observability.

---

# References

* Microsoft Azure Data Factory
* Azure Data Lake Storage Gen2
* Azure Databricks
* Delta Lake
* Azure Synapse Analytics
* Microsoft Power BI
* Olist Brazilian E-Commerce Dataset

---
