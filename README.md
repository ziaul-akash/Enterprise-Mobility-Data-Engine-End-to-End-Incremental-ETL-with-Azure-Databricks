 <div align="center">

# 🚕 Enterprise Mobility Data Engine
### End-to-End Incremental Lakehouse Pipeline with Azure Databricks & Unity Catalog

<p>
  <img src="https://img.shields.io/badge/Azure-Databricks-blue?logo=databricks" alt="Azure Databricks"/>
  <img src="https://img.shields.io/badge/Architecture-Medallion-orange" alt="Medallion Architecture"/>
  <img src="https://img.shields.io/badge/Storage-Delta%20Lake-green" alt="Delta Lake"/>
  <img src="https://img.shields.io/badge/Governance-Unity%20Catalog-purple" alt="Unity Catalog"/>
  <img src="https://img.shields.io/badge/Processing-Incremental-brightgreen" alt="Incremental Processing"/>
</p>

</div>

<hr/>

## 📌 Project Overview

This project implements an end-to-end, modern data engineering pipeline on Azure using the **Medallion Architecture (Bronze → Silver → Gold)** to process NYC Yellow Taxi trip data.

The pipeline ingests raw trip data, performs data cleansing and enrichment, applies historical dimension tracking, and generates business-level aggregations for analytics and reporting.

Built using Azure Databricks, Delta Lake, PySpark, and Unity Catalog, the solution is designed to be incremental, scalable, automated, and production-ready.

The pipeline also incorporates a two-month vendor reporting delay, idempotent processing, optimized data partitioning, and automated external data sharing through Azure Data Lake Storage Gen2 (ADLS Gen2).

## 🎯 Objectives

- Build an end-to-end data engineering pipeline using Azure Databricks.
- Implement incremental and automated data processing.
- Handle delayed data ingestion and source schema variations.
- Apply data quality checks, cleansing, and enrichment.
- Implement Slowly Changing Dimension (SCD Type 2) for historical taxi zone tracking.
- Optimize storage and query performance using Delta Lake partitioning.
- Deliver analytics-ready datasets for business intelligence and forecasting.
- Enable secure external data sharing through ADLS Gen2.
- Implement centralized data governance using Unity Catalog.

## 🎯 The Problem Solved & Business Impact

### 1. The Business Problem

#### Massive, Unstructured, and Delayed Data Inflow

Raw NYC Yellow Taxi trip files continuously arrive in storage. Managing schema drift, vendor reporting delays, and inconsistent raw data manually creates significant data bottlenecks.

The strict two-month reporting lag makes timely data availability and processing more challenging.

#### Siloed and Inaccurate Historical Context

Taxi zone changes, including borough and location mapping modifications, can break historical reporting when legacy databases fail to preserve previous dimension values.

Without historical tracking, previously reported metrics may become inaccurate when location attributes change.

#### Lack of Ready-to-Use Analytics

Downstream executive teams and BI dashboards require clean, aggregated, and reliable datasets.

Unoptimized raw data can lead to slow query performance, increased compute costs, and a lack of consistent business metrics.

### 2. How the Pipeline Helps Business Decisions & Forecasting

#### 📈 Dynamic Demand & Revenue Optimization

Automated processing of daily trip summaries in the Gold Layer enables business stakeholders to visualize:

- Peak travel hours.
- High-demand pickup and dropoff zones.
- Revenue trends across different locations.
- Passenger volume trends.
- Average fare and trip distance patterns.

These insights support data-driven decisions regarding fleet allocation, operational efficiency, and revenue optimization.

#### 🔮 Reliable Trend Forecasting

Clean, partitioned, and historically consistent datasets provide a reliable foundation for data science teams to develop predictive models.

Potential forecasting applications include:

- Passenger demand spike prediction.
- Demand forecasting by taxi zone and time of day.
- Dynamic pricing and surcharge analysis.
- Fleet deployment optimization.
- Revenue forecasting.
- Seasonal travel trend analysis.

#### 🛡️ Complete Data Governance & Trust

Unity Catalog provides centralized governance, access control, auditing, and data lineage.

SCD Type 2 tracking ensures that historical taxi zone changes are preserved, enabling accurate time-based reporting and reliable historical analytics.

## 💡 Key Improvements & Highlights

### 🔄 Fully Automated & Idempotent Architecture

- Eliminates repetitive manual pipeline execution.
- Dynamically calculates the two-month processing lag.
- Checks for already-processed partitions.
- Safely skips duplicate processing.
- Uses an append-only Delta Lake strategy.
- Supports reliable incremental processing.

### 🕒 SCD Type 2 Dimension Modeling

- Implements historical tracking for taxi zones.
- Preserves changes in zone names and borough mappings.
- Maintains `effective_date` and `end_date`.
- Supports accurate historical reporting.
- Prevents dimension changes from corrupting historical analytics.

### ⚡ Optimized Performance & External Sharing

- Partitions data by vendor and month.
- Reduces unnecessary data scanning.
- Improves query performance for analytical workloads.
- Helps optimize storage and compute costs.
- Automates JSON exports to ADLS Gen2.
- Enables secure collaboration with external stakeholders.

### 🔐 Enterprise Data Governance

- Centralized governance using Unity Catalog.
- Managed access controls and permissions.
- Data lineage and auditing capabilities.
- Secure external location management.
- Improved data discoverability and trust.

## 🏗️ Architecture

<p align="center">
  <img src="https://raw.githubusercontent.com/ziaul-akash/nyctaxi_project/refs/heads/main/solution_architecture/part_1_medallion_architecture.png"
       alt="NYC Taxi Medallion Architecture"
       width="800"/>
</p>

### Medallion Architecture Layers

#### 🟤 Bronze Layer — `nyctaxi.01_bronze`

**Purpose:** Raw data ingestion and storage.

- Stores raw NYC Yellow Taxi trip data from source files.
- Source format: `yellow_tripdata_YYYY-MM.parquet`
- Performs minimal transformations.
- Applies schema enforcement and metadata addition.
- Preserves source-level data for downstream processing.

**Table:**
- `yellow_trips_raw`



#### ⚪ Silver Layer — `nyctaxi.02_silver`

**Purpose:** Data cleansing, validation, enrichment, and dimensional modeling.

**1. `yellow_trips_cleansed`**

- Handles null values.
- Performs data type casting.
- Removes invalid and inconsistent records.
- Applies data quality validations.
- Produces clean and reliable trip records.

**2. `yellow_trips_enriched`**

- Calculates trip duration.
- Enriches trips with pickup and dropoff taxi zones.
- Adds borough and location mappings.
- Joins trip records with the taxi zone dimension.
- Provides a comprehensive dataset for analytics.

**3. `taxi_zone_lookup`**

- Implements SCD Type 2 historical tracking.
- Maintains historical taxi zone changes.
- Tracks changes in borough and zone mappings.
- Uses `effective_date` and `end_date` to preserve historical records.
- Ensures accurate historical reporting when zone attributes change.

<p align="center">
  <img src="https://raw.githubusercontent.com/ziaul-akash/nyctaxi_project/refs/heads/main/solution_architecture/Part_2_databricks_job.png"
       alt="NYC Taxi Medallion Architecture"
       width="800"/>
</p>

#### 🟡 Gold Layer — `nyctaxi.03_gold`

**Purpose:** Business-level aggregations and analytics-ready datasets.

**Table: `daily_trip_summary`**

Provides aggregated metrics such as:

- Total trips
- Average passengers per trip
- Average trip distance
- Average fare
- Minimum fare
- Maximum fare
- Total revenue

<p align="center">
  <img src="https://raw.githubusercontent.com/ziaul-akash/nyctaxi_project/refs/heads/main/solution_architecture/part_4.png"
       alt="NYC Taxi Medallion Architecture"
       width="800"/>
</p>

These metrics support BI dashboards, business reporting, demand analysis, and forecasting.

## ⚙️ Pipeline Design

### 🔄 Incremental Processing Logic

The pipeline is designed to process data incrementally while accounting for delayed vendor reporting.

- Processes data one month at a time.
- Dynamically calculates the processing window based on a strict two-month vendor reporting lag.
- Checks whether the source data is available before processing.
- Detects already-processed partitions to prevent duplicate processing.
- Skips missing or previously processed data safely.
- Uses an append-only Delta Lake strategy for incremental ingestion.
- Supports automated and repeatable execution.

This approach reduces unnecessary computation, avoids duplicate records, and ensures reliable pipeline execution.

### 🧠 Key Transformations

- Raw data ingestion from Parquet files.
- Schema enforcement and metadata handling.
- Data validation and cleansing.
- Null handling and type casting.
- Invalid trip record filtering.
- Trip duration calculation.
- Pickup and dropoff zone enrichment.
- Borough mapping.
- SCD Type 2 historical dimension management.
- Business-level aggregation.
- Partitioning by vendor and month.

## 🧩 Databricks Job Orchestration

Azure Databricks Jobs orchestrate the entire pipeline through a sequence of dependent notebook tasks.

### Workflow

1. Load raw data → Bronze Layer
2. Cleanse data → Silver Cleansed Layer
3. Enrich data → Silver Enriched Layer
4. Update taxi zone dimension → SCD Type 2
5. Aggregate metrics → Gold Layer
6. Export processed data → ADLS Gen2

### Orchestration Features

- Automated scheduled execution.
- Task dependency management.
- Retry and failure handling.
- Incremental processing.
- Monitoring and execution tracking.
- Reduced manual intervention.

## 📤 Data Export & External Sharing

### Requirement

Enable external data teams and stakeholders to access processed NYC taxi data without directly accessing the internal lakehouse tables.

### Solution

The pipeline exports the `yellow_trips_enriched` dataset as partitioned JSON files to Azure Data Lake Storage Gen2.

### Implementation

- Created an Azure Storage Account and container.
- Configured Unity Catalog External Location.
- Created an external table under `nyctaxi.04_export`.
- Exported enriched taxi trip data in JSON format.
- Partitioned exported data by vendor and month.
- Appended new monthly data incrementally.
- Enabled secure external stakeholder collaboration.

**Export destination:**

`nyctaxi_yellow_export/`


## 🧪 Technologies Used

| Technology | Purpose |
|---|---|
| Azure Databricks | Data processing and orchestration |
| PySpark | Distributed data transformations |
| Spark SQL | Data querying and analytics |
| Delta Lake | Reliable storage and incremental processing |
| Unity Catalog | Governance, access control, lineage, and auditing |
| Azure Data Lake Storage Gen2 | Cloud data storage and external sharing |
| Databricks Workflows (Jobs) | Pipeline orchestration |
| Parquet | Source data format |
| JSON | External data export format |

## 📊 Key Features

- End-to-end Azure data engineering pipeline.
- Medallion Architecture implementation.
- Automated incremental data ingestion.
- Two-month vendor reporting lag handling.
- Idempotent processing and duplicate prevention.
- Data quality validation and cleansing.
- SCD Type 2 historical dimension modeling.
- Analytics-ready Gold Layer aggregations.
- Vendor and month-based partitioning.
- Automated ADLS Gen2 data exports.
- Unity Catalog governance and lineage.
- Production-style Databricks Job orchestration.
- Business intelligence and forecasting readiness.

## 🚀 How to Run

1. Upload the source files in the format:

   `yellow_tripdata_YYYY-MM.parquet`

2. Configure Azure storage and Unity Catalog.

3. Verify the required catalogs, schemas, tables, and external locations.

4. Configure the Databricks Job and notebook task dependencies.

5. Run the Databricks Job or enable scheduled execution.

6. Monitor pipeline execution and validate task completion.

7. Verify the processed data in Bronze, Silver, and Gold Layers.

8. Validate the exported JSON data in ADLS Gen2.

## 💡 What I Learned

- Designing scalable cloud-based Lakehouse architectures.
- Building end-to-end incremental ETL pipelines.
- Handling delayed data ingestion and schema variations.
- Implementing idempotent processing with Delta Lake.
- Applying SCD Type 2 dimensional modeling in PySpark.
- Designing historical data tracking for changing dimensions.
- Optimizing analytical workloads using partitioning.
- Managing data governance and security using Unity Catalog.
- Orchestrating production-style workflows using Databricks Jobs.
- Building analytics-ready datasets for BI and forecasting.
- Implementing secure external data sharing using ADLS Gen2.

## 🧠 Short Project Explanation

"I built an end-to-end incremental data engineering pipeline on Azure Databricks using the Medallion Architecture. The pipeline ingests raw NYC Yellow Taxi trip data into the Bronze Layer, cleanses and enriches it in the Silver Layer, and generates business-level aggregations in the Gold Layer.

I implemented an automated incremental processing strategy that dynamically handles the two-month vendor reporting lag and prevents duplicate processing. I also developed an SCD Type 2 dimension model for taxi zones to maintain historical accuracy when location attributes change.

The pipeline is orchestrated using Databricks Jobs, optimized through vendor and month-based partitioning, and governed using Unity Catalog. Additionally, I implemented automated partitioned JSON exports to ADLS Gen2 for external data sharing.

The solution provides reliable, analytics-ready data for business intelligence, demand forecasting, revenue optimization, and fleet deployment planning."

<hr/>

<div align="center">

### 🚕 Enterprise Mobility Data Engine
**Building reliable, scalable, and analytics-ready data pipelines with Azure Databricks.**

</div>
