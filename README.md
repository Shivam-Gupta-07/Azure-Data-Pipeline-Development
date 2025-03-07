#### Azure Data Pipeline Development

##Project Overview
This project focuses on creating an Azure-based data pipeline to automate the process of ingesting data, performing checks, and storing the processed data into Azure SQL Database. The pipeline is triggered when a file (e.g., orders.csv) is uploaded by a third-party service. The solution ensures the data is processed, transformed, and made available for reporting and analysis.

##Steps Involved
Resource Creation:
Set up the necessary Azure resources required for the pipeline(Azure Data Factory) including SQL Database, Storage accounts and Azure Databricks.

##Azure SQL Database:
Created an Azure SQL DB with a lookup table to store and process the ingested data.

##Developing Logic in Databricks:
Developed the transformation logic using an interactive Databricks cluster for efficient data processing.

##Azure Data Factory Pipeline:
Created a pipeline using Azure Data Factory to automate data movement. The pipeline is triggered by storage events when a new file is uploaded.

##Parameterized Approach:
Implemented a parameterized approach to dynamically read file names, making the pipeline flexible and reusable.

##Notebook Execution in Databricks:
Used Databricks notebooks to execute the data processing logic interactively and ensure smooth operations.

##Generic Pipeline Setup:
Developed a generic pipeline with reusable code for mounting storage and securely accessing storage keys.

##Data Ingestion from Amazon S3 to Azure Storage Account(Datalake):
Set up the ingestion process to move data from Amazon S3 to Azure Datalake ADLS Gen2.

##Populating Azure SQL Database:
Loaded the processed data into the Azure SQL Database for further querying and reporting.

##Performing Data Joins:
Performed SQL joins on the ingested data to combine different datasets for comprehensive reporting.

#Writing Data Back:
Wrote the final processed data back into Azure SQL Database for reporting and analysis.

##Summary
This pipeline automates the process of data ingestion, transformation, and storage. By leveraging Azure Data Factory, Databricks, and Azure SQL Database, it ensures seamless data flow from Amazon S3 to Azure while implementing necessary checks and transformations to improve data quality and usability.
