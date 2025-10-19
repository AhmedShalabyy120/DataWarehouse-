# 📘 Project Requirements: Building the Data Warehouse (Data Engineering)


## This Project material from the Datawithbaraa Channel. Thanks to him for his guidance 


## Objective:
Develop a modern data warehouse using SQL Server to consolidate sales data, enabling analytical reporting and informed decision-making.

## Specifications:

- **Data Sources**: Import data from two source systems (ERP and CRM) provided as CSV files.
- **Data Quality**: Cleanse and resolve data quality issues prior to analysis.
- **Integration**: Combine both sources into a single, user-friendly data model designed for analytical queries.
- **Scope**: Focus on the latest dataset only; historization of data is not required.
- **Documentation**: Provide clear documentation of the data model to support both business stakeholders and analytics teams.

## Data Warehouse Layers

The project will involve designing a multi-layered architecture for the data warehouse. Below is an overview of the data layers:

### 1. **Bronze Layer** (Raw Data)
- **Definition**: Raw, unprocessed data as-is from the source systems (ERP, CRM).
- **Objective**: Ensures traceability and debugging of the original source data.
- **Object Type**: Tables
- **Load Method**: Full Load (Truncate & Insert)
- **Data Transformation**: None (Data remains as-is)

### 2. **Silver Layer** (Cleaned and Standardized Data)
- **Definition**: Clean and standardized data ready for analysis.
- **Objective**: Prepares data for deeper analysis and reporting by cleansing and transforming the raw data.
- **Object Type**: Tables
- **Load Method**: Full Load (Truncate & Insert)
- **Data Transformation**:
    - Data Cleaning
    - Data Standardization
    - Data Normalization
    - Derived Columns
    - Data Enrichment

### 3. **Gold Layer** (Business-Ready Data)
- **Definition**: Data that is ready for reporting and analytical consumption.
- **Objective**: Provides data to be consumed by business stakeholders for reporting and decision-making.
- **Object Type**: Views
- **Load Method**: No Load (Data is transformed and queried directly from the Silver layer)
- **Data Transformation**:
    - Data Integration
    - Data Aggregation
    - Business Logic & Rules

## Technologies Used:
- **SQL Server**: Used for implementing and managing the data warehouse.
- **ETL Processes**: SQL queries will be used for data extraction, transformation, and loading (ETL).

## Data Model Documentation:
- The data model will be designed to consolidate data from both ERP and CRM systems.
- Detailed documentation will be provided, outlining the structure of each table, view, and the relationship between entities.

<img width="847" height="435" alt="image" src="https://github.com/user-attachments/assets/ccf05a55-4ae8-449f-81f1-0b785eae3514" />



