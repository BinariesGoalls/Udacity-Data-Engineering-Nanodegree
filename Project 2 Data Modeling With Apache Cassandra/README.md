<!-- PROJECT LOGO -->
<br />

<p align="center">
 </a>
 <h1 align="center">Project 2 Data Modeling With Apache Cassandra</h1>
 <p align="center">
  Udacity Nanodegree
  <br />
  <a href=https://github.com/BinariesGoalls/Udacity-Data-Engineering-Nanodegree><strong>Explore the repository»</strong></a>
  <br />
  <br />
 </p>

</p>

> apache, cassandra, nosql, data engineering, ETL, data modeling

<!-- ABOUT THE PROJECT -->

## About The Project

In this project, I learned how to do data modeling in non-relational databases with Apache Cassandra and used Python to build an ETL pipeline.

I applied what I've learned through the data modeling module to build a pipeline that transfers data from a set of CSV files within a directory to create a streamlined CSV file to model and insert data into Apache Cassandra tables. I had to create separate denormalized tables for answering specific queries, properly using partition keys and clustering columns, following the concepts of NoSQL databases.

### Project Description

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analysis team is particularly interested in understanding what songs users are listening to. Currently, there is no easy way to query the data to generate the results, since the data reside in a directory of CSV files on user activity on the app.

They'd like a data engineer to create an Apache Cassandra database which can create queries on song play data to answer the questions, and wish to bring you on the project. Your role is to create a database for this analysis. You'll be able to test your database by running queries given to you by the analytics team from Sparkify to create the results.

### Tools Used

* Python
* Apache Cassandra
* Jupyter notebooks

### Datasets
#### Event Dataset

This event dataset is a collection of CSV files containing the information of user activity across a period of time. Each file in the dataset contains the information regarding the song played, user information and other attributes.

Columns:

```
artist, auth, firstName, gender, itemInSession, lastName, length, level, location, method, page, registration, sessionId, song, status, ts, userId
```


## Data Modeling

### ERD

In this case since I was working with NoSQL databases, each table is modeled to answer a specific knew query. This model enables to efficiently query through databases  containing huge amounts of data. Relational databases are not suitable in this scenario due to the magnitude of data. 

You can see an Entity Relationship Diagram (ERD) of the built data model below:

![database](./images/Project%202%20tables%20ERD.png)

## Project structure

Files:

|  File / Folder   |                         Description                          |
| :--------------: | :----------------------------------------------------------: |
|    event_data    | Folder at the root of the project, where all the CSV data resides |
|      images      |  Folder at the root of the project, where images are stored  |
|  Project 2.ipynb | Jupyter notebook containing the ETL pipeline including data extraction, modeling and loading into the tables. |
|      README      | Readme file |
|event_datafile_new.csv| CSV cointaining the whole data after merging all the CSV files at `event_data` |



## How to Run

Clone the repository into a local machine using

```sh
git clone https://github.com/BinariesGoalls/Udacity-Data-Engineering-Nanodegree
```

### Prerequisites

These are the tools necessaries to run the program.

* Python
* Apache Cassandra
* cassandra python librarie

### Steps

Follow the steps to extract and load the data into the data model.

1. Navigate to `Project 2 Data Modeling with Apache Cassandra` folder

2. Run `Project 2.ipynb` Jupyter Notebook

3. Run Part I to create `event_datafile_new.csv`

4. Run Part 2 to execute the ETL process and load data into tables

4. Check whether the data has been loaded into database by executing the `SELECT` queries



<!-- CONTACT -->

## Contact

Alisson lima - ali2slima10@gmail.com

Linkedin: [https://www.linkedin.com/in/binariesgoalls/](https://www.linkedin.com/in/binariesgoalls/)
