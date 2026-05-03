# Azure E-Commerce ETL Pipeline

![Azure](https://img.shields.io/badge/Azure-Lakehouse-blue)
![Databricks](https://img.shields.io/badge/Databricks-PySpark-red)
![Synapse](https://img.shields.io/badge/Synapse-Analytics-blue)
![Power BI](https://img.shields.io/badge/PowerBI-Dashboard-yellow)

---

## Overview

This project implements an **end-to-end Azure Lakehouse data platform** using the **Medallion Architecture (Bronze → Silver → Gold)**.

It integrates **Azure Data Factory, Databricks, ADLS Gen2, Synapse, and Power BI** to deliver a scalable pipeline for **e-commerce analytics and business insights**.

---

## Architecture

![Architecture](assets/pro_architecture.png)

### Key Components

- **ADF** → Orchestrates ingestion pipelines from multiple sources  
- **ADLS Gen2 + Delta Lake** → Central storage (Bronze / Silver / Gold)  
- **Databricks (PySpark)** → Data cleaning, transformation, modeling  
- **Synapse (Serverless SQL)** → Serving layer via external tables & views  
- **Power BI** → Visualization and business insights  

---

## Objectives

- Build a **scalable ETL pipeline** using Azure-native services  
- Integrate **multi-source data** (MySQL, MongoDB, HTTP APIs, CSV)  
- Apply **Lakehouse + Star Schema modeling**  
- Enable **real-time analytics via Synapse + Power BI**  

---

## Tech Stack

| Layer            | Technology |
|------------------|----------|
| Orchestration    | Azure Data Factory |
| Storage          | ADLS Gen2 + Delta Lake |
| Processing       | Databricks (PySpark) |
| Serving          | Synapse Analytics |
| Visualization    | Power BI |
| Sources          | MySQL, MongoDB, HTTP API, CSV |
| Languages        | Python, SQL |

---

## Data Flow (Medallion)
Sources → ADF → ADLS (Bronze) → Databricks → Silver → Gold
↓
Synapse
↓
Power BI


---

## Pipeline Stages

### Bronze — Ingestion
- Data from **MySQL, MongoDB, HTTP APIs, CSV**
- Loaded into ADLS using **ADF pipelines (dynamic + parameterized)**

---

### Silver — Transformation
- Built with **Databricks (PySpark + Delta Lake)**
- Includes:
  - Data cleaning & normalization  
  - Type casting & deduplication  
  - Derived fields (delivery time, sentiment, etc.)

---

### Gold — Modeling
- Designed **Star Schema**:
  - Fact: `fact_sales`, `fact_sales_agg`
  - Dimensions: customer, product, orders, seller  
  - Bridge: `bridge_order_items`
- Optimized using:
  - Partitioning  
  - Z-Ordering  
  - Delta optimization  

---

### Serving — Synapse
- External tables & views over Delta/Parquet  
- Serverless SQL enables **Power BI DirectQuery**

---

### Visualization — Power BI

#### Sales Dashboard
![Sales](assets/powerbi/Sales_Dashboard.png)

#### Customer Insights
![Customer](assets/powerbi/Customer_Insights_Dashboard.png)

#### Logistics & Delivery
![Logistics](assets/powerbi/Logistics\&Delivery_Dashboard.png)

---

## Business Insights

### Key Findings
- Credit Card dominates (~74%)
- Revenue grew ~20% YoY (2017 → 2018)
- Southeast region contributes ~70% revenue
- Avg delivery time: **10–12 days**
- Repeat customers: ~3%

### Recommendations
- Improve logistics efficiency (reduce delays)
- Increase retention via loyalty programs
- Focus marketing on high-performing regions
- Optimize campaigns for weekday demand (~77%)

---

## Project Structure
├── databricks/ # ETL pipelines (Bronze → Silver → Gold)
├── azure_data_factory/ # ADF pipelines & configs
├── synapse/ # SQL scripts (external tables, views)
├── docs/ # Architecture & dashboards
└── README.md


---

## How to Run

1. Provision Azure services (ADF, ADLS, Databricks, Synapse)
2. Deploy ADF pipelines (JSON configs)
3. Run ingestion → load Bronze layer
4. Execute Databricks ETL pipelines
5. Run Synapse SQL scripts
6. Connect Power BI to Synapse

---

## Highlights

- End-to-end **Lakehouse architecture**
- Multi-source ingestion (SQL + NoSQL + API)
- Delta Lake with **ACID + optimization**
- Star Schema modeling for analytics
- Real-time BI with Synapse + Power BI

---

## References

- Azure Data Factory  
- Azure Databricks  
- Azure Synapse  
- Delta Lake  
- Olist Dataset (Kaggle)
