# Sales Data Warehouse

## # Project Overview

This project focuses on building a **Sales Data Warehouse** using a **Star Schema**.

The purpose of the project is to transform raw sales data into a structured warehouse that can support sales reporting, performance analysis, and business insights.

## # Data Warehouse Architecture

The Data Warehouse follows a **Star Schema** consisting of:

* Fact Sales Table
* Product Dimension
* Customer Dimension
* Date Dimension
* Other supporting dimensions
* Primary Keys
* Foreign Keys
* Surrogate Keys

The fact table stores sales transactions and measurable values, while dimension tables provide descriptive information.

## # Data Warehouse Process

**Raw Sales Data → Data Cleaning → Dimension Tables → Fact Table → Business Analysis**

The project includes:

* Extracting sales data
* Cleaning and transforming the data
* Creating dimension tables
* Generating surrogate keys
* Creating the sales fact table
* Establishing relationships between tables
* Writing SQL queries for analysis

## # Technologies Used

* Python
* Pandas
* SQL
* PostgreSQL
* Jupyter Notebook
* pgAdmin
* Star Schema

## # Business Analysis

The warehouse can be used to analyze:

* Total sales
* Sales by product
* Sales by customer
* Sales by date
* Sales by region
* Product performance
* Customer purchasing behavior
* Monthly and yearly sales trends
* Revenue and quantity sold

## # Key Learning

This project helped me practice:

* Data Warehouse design
* Star Schema
* Fact and Dimension tables
* ETL concepts
* Data cleaning
* Surrogate Keys
* Primary and Foreign Keys
* SQL aggregation
* Sales analysis
* PostgreSQL

## # Project Structure

```text
Sales-Data-Warehouse/
│
├── data/
│   └── sales_data.csv
│
├── python/
│   └── data_cleaning.py
│
├── sql/
│   └── sales_data_warehouse.sql
│
└── README.md
```

## # Project Goal

The goal of this project was to build a practical Sales Data Warehouse and understand how transactional sales data can be transformed into a Star Schema for analytical reporting.
