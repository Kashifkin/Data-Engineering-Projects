# AI Job Market Data Warehouse

##  Project Overview

This project focuses on building a **Data Warehouse for AI Job Market data** using a **Star Schema**.

The main objective is to transform raw job-market data into a structured data warehouse that can be used for reporting, analysis, and business insights.

##  Data Warehouse Architecture

The project follows a **Star Schema** consisting of:

* Fact Table
* Dimension Tables
* Primary Keys
* Foreign Keys
* Surrogate Keys

The fact table contains measurable information, while dimension tables contain descriptive information used for analysis.

##  Data Warehouse Process

**Raw Data → Data Cleaning → Dimension Tables → Fact Table → Business Analysis**

The project includes:

* Extracting raw job-market data
* Cleaning and transforming the data
* Creating dimension tables
* Creating surrogate keys
* Creating the fact table
* Establishing relationships using foreign keys
* Performing analytical queries

##  Technologies Used

* Python
* Pandas
* SQL
* PostgreSQL
* Jupyter Notebook
* pgAdmin
* Star Schema

##  Business Analysis

The warehouse can be used to analyze:

* Salary by job title
* Salary by experience level
* Jobs by industry
* Jobs by location
* Remote work trends
* Education requirements
* AI tool usage
* Job satisfaction
* Employee experience
* Company information

##  Key Learning

This project helped me practice:

* Data Warehouse design
* Star Schema
* Fact and Dimension tables
* ETL concepts
* Data cleaning
* Surrogate Keys
* Primary Keys and Foreign Keys
* SQL queries
* PostgreSQL
* Data transformation using Python

##  Project Structure

```text
AI-Job-Market-Data-Warehouse/
│
├── data/
│   └── ai_jobs_market.csv
│
├── python/
│   └── data_cleaning.py
│
├── sql/
│   └── data_warehouse.sql
│
└── README.md
```

## # Project Goal

The goal of this project was to build a practical Data Warehouse and understand how raw AI job-market data can be transformed into structured data for analytical purposes.
