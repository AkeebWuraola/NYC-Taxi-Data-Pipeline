# NYC-Taxi-Data-Pipeline
<h1>1.  Project Description</h1> 
The NYC Taxi Data Pipeline is an end-to-end data engineering project designed to ingest, process, transform, and analyze NYC Taxi trip data efficiently and at scale.
The pipeline automates the movement of raw data from external sources into a structured analytics-ready format suitable for dashboards, insights, and machine learning applications.

The project demonstrates a modern data architecture that supports:
<li>High-volume data ingestion (Parquet/CSV files)</li>
<li>Data transformation and quality control</li>
<li>Centralized data storage in a data warehouse</li>
<li>Automated scheduling and monitoring</li>
<li>Analytical access through BI tools</li>

<h1>2.  Project Requirements</h1> 
<h3>Functional Requirements</h3>
<li>Ingest NYC Yellow and Green Taxi trip data from a public data source.</li>
<li>Transform and clean datasets (handle nulls, outliers, and schema consistency).</li>
<li>Store cleaned data in a relational database or data warehouse.</li>
<li>Automate periodic data ingestion (e.g., monthly).</li>
<li>Enable querying for KPIs such as:Total trips,Average fare amount,Trip distance distribution,Peak pickup/drop-off times</li>
<li>Provide a BI layer or dashboard for visualization.</li>

<h1>Naming Convention & General Principles</h1>
To ensure consistency in naming, snake case with all lowercase and underscores to separate words will be used for this project
English Language will be used and SQL Reserved words will not be used as object names

**Bronze & Silver Rules**
- All names must start with source system name and table names must match their original names from the source i.e _(tablename) e.g crm_customersinfo

**Gold Rules**
- All names must be meaningful, business aligned names for tables starting with category prefix i.e _ e.g dim_customers,fact_sales
category describes the role of the table; if its a fact (fact) or dimension(dim) table

**Column Naming Convention**
**Surrogate Keys**
- Surrogate keys must use suffix _key: surrogate keys are system generated unique identifiers assigned to each record in a table
- All primary keys must use suffix _id.
  
**Technical Columns**
- Technical columns should start with dw_<column_name> e.g dw_load_date
  
**Stored Procedures**
- All stored procedures must start with stp_load_

**Log tables**
- All log table must start with log_
