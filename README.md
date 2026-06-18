# 🛒 Retail Lakehouse Pipeline | End-to-End Data Engineering Project
![Python](https://img.shields.io/badge/Python-3.13-blue)
![PySpark](https://img.shields.io/badge/PySpark-4.0-orange)
![Snowflake](https://img.shields.io/badge/Snowflake-Data%20Warehouse-blue)
![Tableau](https://img.shields.io/badge/Tableau-Dashboard-purple)
![SQL](https://img.shields.io/badge/SQL-Analytics-green)

### 📌 Project Overview

This project demonstrates the design and implementation of a modern Data Engineering solution using the Medallion Architecture (Bronze, Silver, Gold) with PySpark, Snowflake, and Tableau.

The pipeline ingests raw retail sales data, performs data quality validations and transformations, loads curated datasets into Snowflake, builds a dimensional data model using a Star Schema, and delivers business insights through interactive Tableau dashboards.

### 🎯 Business Objective

Retail organizations generate large volumes of transactional data from stores, departments, and promotions.

The objective of this project is to:

* Build a scalable data pipeline using PySpark
* Implement Medallion Architecture (Bronze → Silver → Gold)
* Store optimized datasets in Parquet format
* Load curated data into Snowflake
* Design a Star Schema for analytical reporting
* Visualize business KPIs using Tableau
* Generate actionable insights for business stakeholders

# 🏗️ Solution Architecture

![Architecture Diagram](drawio/architecture_diagram.png)

## Data Flow

Raw CSV Files
→ Bronze Layer
→ Silver Layer
→ Gold Layer
→ Snowflake Data Warehouse
→ Star Schema
→ Tableau Dashboards
→ Business Insights

## 🛠️ Technology Stack
Technology                                         Purpose

Python                                             Core Programming
PySpark                                            Distributed Data Processing
Parquet                                            Optimized Storage Format
Snowflake                                          Cloud Data Warehouse
SQL                                                Data Modeling & Analytics
Tableau                                            Business Intelligence & Visualization
GitHub                                             Version Control
YAML                                               Configuration Management

## 🥉 Bronze Layer

Purpose

The Bronze Layer stores raw ingested data with minimal modifications.

Source Files

* stores.csv
* features.csv
* train.csv

Transformations

* Schema Inference
* Metadata Enrichment
* Load Timestamp Addition
* Source File Tracking

Output

* bronze_stores
* bronze_features
* bronze_sales

Stored in Parquet format.

## 🥈 Silver Layer

Purpose

The Silver Layer performs data cleansing, standardization, and quality validation.

Data Quality Rules Implemented

* Duplicate Removal
* Column Standardization
* Data Type Conversion
* Null Handling
* Negative Sales Validation
* Business Column Preservation

Output

* silver_stores
* silver_features
* silver_sales

## 🥇 Gold Layer

Purpose

The Gold Layer contains business-ready datasets optimized for analytics and reporting.

Gold Tables

GOLD_STORE_PERFORMANCE

Store-level sales metrics.

GOLD_DEPARTMENT_PERFORMANCE

Department-level sales metrics.

GOLD_MONTHLY_SALES_TREND

Monthly aggregated sales trends.

GOLD_HOLIDAY_IMPACT_ANALYSIS

Holiday vs Non-Holiday sales analysis.

## ❄️ Snowflake Data Warehouse

All Bronze, Silver, and Gold datasets are loaded into Snowflake using Python and the Snowflake Connector.

Schemas

* BRONZE
* SILVER
* GOLD

Benefits

* Centralized Analytics Layer
* Scalable Cloud Storage
* Fast Query Performance
* Integration with Tableau

## ⭐ Star Schema Design

A dimensional data model was created in Snowflake to support analytical workloads.

Fact Table
FACT_SALES

Column
STORE_KEY
DEPARTMENT_KEY
DATE_KEY
WEEKLY_SALES
IS_HOLIDAY

Dimension Tables
DIM_STORE

Column
STORE_KEY
STORE_ID
TYPE
SIZE

DIM_DEPARTMENT

Column
DEPARTMENT_KEY
DEPARTMENT_ID

DIM_DATE

Column
DATE_KEY
DATE_VALUE
YEAR
MONTH
QUARTER
DAY_OF_WEEK
MONTH_NAME
YEAR_MONTH
WEEK_OF_YEAR

Data Model

FACT_SALES connects to:

* DIM_STORE
* DIM_DEPARTMENT
* DIM_DATE

using surrogate keys.

This design improves analytical performance and follows industry-standard dimensional modeling practices.

# 📊 Tableau Dashboard & Business Insights

The Star Schema was connected directly to Tableau Desktop to build business dashboards.

---

## 1. Top Performing Stores
![Top Performing Stores](screenshots/top_performing_stores.png)
### Insight

Store 20 generated the highest overall revenue, followed by Stores 4 and 14.

## 2. Monthly Sales Trend
![Monthly Sales Trend](screenshots/monthly_sales_trend.png)
### Insight

Sales patterns show seasonal fluctuations and recurring demand cycles throughout the year.

## 3. Quarterly Sales Analysis
![Quarterly Sales](screenshots/quarterly_sales.png)
### Insight

Quarterly trends reveal periods of stronger business performance and revenue concentration

## 4. Holiday Impact Analysis
![Holiday Impact](screenshots/holiday_impact.png)
### Insight

Holiday periods show stronger average sales compared to regular weeks, highlighting the impact of seasonal shopping behavior.

## 5. Store Type Analysis
![Store Type Analysis](screenshots/store_type_analysis.png)
### Insight

Type A stores contribute the majority of total sales volume.

## 6. Department Performance
![Department Performance](screenshots/top_departments.png)
### Insight

A small number of departments generate a significant portion of overall revenue.



## 📁 Project Structure
retail-lakehouse-pipeline/

├── config/
├── data/
│   ├── bronze/
│   ├── silver/
│   └── gold/
│
├── docs/
│   └── architecture_diagram.png
│
├── screenshots/
│   ├── top_performing_stores.png
│   ├── monthly_sales_trend.png
│   ├── quarterly_sales.png
│   ├── holiday_impact.png
│   ├── store_type_analysis.png
│   └── top_departments.png
│
├── scripts/
│   ├── bronze_ingestion.py
│   ├── silver_transformation.py
│   ├── gold_transformation.py
│   ├── load_to_snowflake.py
│   └── test_snowflake_connection.py
│
├── sql/
│   ├── star_schema.sql
│   └── business_analytics.sql
│
├── requirements.txt
└── README.md

## 🚀 Key Achievements

* Built an end-to-end Data Engineering pipeline
* Implemented Medallion Architecture
* Processed data using PySpark
* Stored optimized Parquet datasets
* Loaded curated data into Snowflake
* Designed a Star Schema data model
* Created Tableau dashboards for business reporting
* Generated meaningful business insights

## 🔮 Future Enhancements

* Apache Airflow Orchestration
* Snowpipe Automation
* Incremental Data Loading
* dbt Transformations
* Data Quality Monitoring Framework
* CI/CD Pipeline Deployment
* Real-Time Data Ingestion

### 👨‍💻 Author

Neela Konda Reddy Beeram

Data Engineer | PySpark | Snowflake | SQL | AWS | Tableau
