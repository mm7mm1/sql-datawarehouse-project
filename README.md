## DataWarehous and Analytics Project. 

  Welcome to the **DataWarehouse and Analytics Project** repository!
This project demonstrates a comprehensive data warehousing and analytics solution, from building a data warehouse to generating actionable insights. Designed as a portfolio project.

## Project Overview

This project involves:

1. **Data Architecture**: Designing a Modern Data Warehouse Using Medallion Architecture **Bronze**, **Silver**, and **Gold** layers.
2. **ETL Pipelines**: Extracting, transforming, and loading data from source systems into the warehouse.
3. **Data Modeling**: Developing fact and dimension tables optimized for analytical queries.
4. **Analytics & Reporting**: Creating SQL-based reports and dashboards for actionable insights.

🎯 This repository is an excellent resource for professionals and students looking to showcase expertise in:

* SQL Development
* Data Architect
* Data Engineering
* ETL Pipeline Developer
* Data Modeling
* Data Analytics

##     Project requirements
 #### **Building the Data Warehouse (Data Engineering)**

#### Objective
  Develop a modern data warehouse using PostgreSQL to consolidate sales data, enabling analytical reporting and informed decision-making.

#### Specifications

- **Data Sources**: Import data from two source systems (ERP and CRM) provided as CSV files.
- **Data Quality**: Cleanse and resolve data quality issues prior to analysis.
- **Integration**: Combine both sources into a single, user-friendly data model designed for analytical queries.
- **Scope**: Focus on the latest dataset only; historization of data is not required.
- **Documentation**: Provide clear documentation of the data model to support both business stakeholders and analytics teams.

## BI: Analytics & Reporting (Data Analytics)

#### Objective
Develop SQL-based analytics to deliver detailed insights into:
- **Customer Behavior**
- **Product Performance**
- **Sales Trends**

  These insights empower stakeholders with key business metrics, enabling strategic decision-making.

  
## Data Architecture
The data architecture diagram for this project follow The Medallion Architecture Bronze, Silver and Gold layers:
![Data Architecture](/Users/ana/Documents/Data_WareHouse_Project/docs/data_integration.png)

## Getting Started

### Prerequisites
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed and running.

### How to Run
1. Clone the repository to your local machine.
2. Navigate to the project folder and run:
   ```bash
   docker-compose up -d
 
  Database Connection Details
  To connect to the database (via VS Code SQLTools, DBeaver, or pgAdmin), use the following credentials from the docker-compose.yml file:

**Host:** localhost; 
**Port:** 5432; 
**User:** admin; 
**Password:** root; 
**Database:** DataWarehouse; 

## License
This project is licensed under the MIT License. You are free to use, modify, and share this project with proper attribution.


## About me 
Hey, my name is Anastasiia, I am a Junior Data Engineer wanting to achieve high results.

