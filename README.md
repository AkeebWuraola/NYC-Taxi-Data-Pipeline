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
