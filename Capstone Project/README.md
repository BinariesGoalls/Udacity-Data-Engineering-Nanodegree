<!-- PROJECT LOGO -->
<br />

<p align="center">
 </a>
 <h1 align="center">Capstone Project: ETL Pipeline for a Brazillian E-Commerce</h1>
 <p align="center">
  Udacity Nanodegree
  <br />
  <a href=https://github.com/BinariesGoalls/Udacity-Data-Engineering-Nanodegree><strong>Explore the repository»</strong></a>
  <br />
  <br />
 </p>

</p>


<!-- ABOUT THE PROJECT -->

## Project Summary

Olist is a Brazilian startup with headquarter in Curitiba. Its business model is of the e-commerce type, with the main objective of helping people who sell a product to find buyers in all Brazilian e-commerce.

Olist has been growing a lot in recent years, due to the increase in new users and also the increase in transactions on its platform. And a company's growing pains are inevitable, especially with data.

The whole system extends its capacity, both in terms of processing and in terms of data storage, and consequently the query time to databases increases, impacting the speed of analysis and the time to update metrics in dashboards.

The objective of this project was to create an ETL pipeline on the olist datasets to create a analytic Data Warehouse on Amazon Redshift. A use case for this analytic database is to find answers for some questions such as "Has the amount of sales has increased over time?", "Which product categories are top sellers?", "What is the total turnover?", et cetera.

With this in mind, the project will follow these steps:

* Step 1: Scope the Project and Gather Data
* Step 2: Explore and Assess the Data
* Step 3: Define the Data Model
* Step 4: Run ETL to Model the Data
* Step 5: Complete Project Write Up

### Step 1: Scope the Project and Gather Data
#### Scope

In this project, we will aggregate the total of eight datasets that represents each one of the tables from the Olist e-commerce OLTP database, with the objective to model this source of data into “Fact” and “Dimension” tables for the analytics team.

The final solution proposed is a pipeline built upon Python for ingesting data from Amazon S3 into a Redshift Data Warehouse, running transformations on Redshift and saving them into a proposed Star Schema model.

#### Describe and Gather Data

**Data Source** <br>

Brazilian E-Commerce Public Datasets by Olist <br>

https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

**Content** <br>

Olist is the largest department store on marketplaces. It has a catalog with over 950,000 products, hundreds of thousands of orders and a network of over 9,000 partner retailers spread across all regions of Brazil. 

All the files use the CSV format.

**Model** <br>

The data sources are the company's e-commerce OLTP system tables, you can check the ERD of the system in the figure below.

![OLIST-OLTP-ERD](./images/OLIST-OLTP-ERD.png)

### Step 2: Explore and Assess the Data

Please refer to [inspection_notebook.ipynb](https://github.com/BinariesGoalls/Udacity-Data-Engineering-Nanodegree/blob/main/Capstone%20Project/inspection_notebook.ipynb).

### Step 3: Define the Data Model
#### 3.1 Conceptual Data Model

Since the purpose of this data warehouse is for OLAP and BI app usage, we will model these data sets with star schema data modeling, the proposed data model consists of the following tables:

* Fact table:
    * fact_oders (order_id, product_id, date_key, seller_id, payment_key, customer_id, freight_value, price, order_items_qtd, order_status)  
   
* Dimension tables:
    * dim_products (product_id, product_category_name)
    * dim_date (date_key, date, year, quarter, month, day, week, is_weekend) 
    * dim_sellers (seller_id, seller_zip_code_prefix, seller_city, seller_state, seller_geo_lat, seller_geo_lng)          
    * dim_payments (payment_key, order_id, payment_sequential, payment_type, payment_installments)
    * dim_customers (customer_id, customer_zip_code_prefix, customer_city, customer_state, customer_geo_lat, customer_geo_lng)

![OLIST-OLAP-ERD](./images/Capstone%20Project%20ERD.png)   

#### 3.2 Mapping Out Data Pipelines

The pipeline consists on the following steps:

* Store the data into Amazon S3
* Stage the data from S3 to Redshift
* Perform the necessary transformations for storing it in the corresponding tables in the Star Schema
* Do quality checks on the data

### Step 4: Run Pipelines to Model the Data 
#### 4.1 Create the data model

Data processing and data model was created in Python.

Please refer to:
* [create_tables.py](https://github.com/BinariesGoalls/Udacity-Data-Engineering-Nanodegree/blob/main/Capstone%20Project/create_tables.py)
* [etl.py](https://github.com/BinariesGoalls/Udacity-Data-Engineering-Nanodegree/blob/main/Capstone%20Project/etl.py)

#### 4.2 Data Quality Checks

The quality checks performed mainly check the load and quantity of records in the fact and dimension tables.

Please refer to [test_queries.ipynb](https://github.com/BinariesGoalls/Udacity-Data-Engineering-Nanodegree/blob/main/Capstone%20Project/tests_queries.ipynb)

### How to run

Follow the steps to extract and load the data into the data model.

1. Set up Apache Airflow to run in local
2. Create an IAM User in AWS.
3. Create a redshift cluster in AWS.
4. Create a connection to AWS in the Airflow Web UI.
5. Create a connection to the Redshift Cluster in the Airflow Web UI.
6. Create the tables manually on the Redshift Cluster Query Editor using the ```create_tables.sql``` file.
6. In Airflow, turn the DAG execution to ON and eecute it.
7. View the Web UI for detailed insights about the operation.

<!-- CONTACT -->

## Contact

Alisson lima - ali2slima10@gmail.com

Linkedin: [https://www.linkedin.com/in/binariesgoalls/](https://www.linkedin.com/in/binariesgoalls/)
