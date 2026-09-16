# University Data Warehouse

## # Project Overview

This project focuses on building a **University Management Data Warehouse** using a **Star Schema**.

The objective is to organize university data into fact and dimension tables so that it can be efficiently used for reporting, analysis, and decision-making.

## # Data Warehouse Architecture

The project follows a **Star Schema** consisting of:

* Fact Table
* Dimension Tables
* Primary Keys
* Foreign Keys
* Surrogate Keys

The dimension tables contain descriptive information, while the fact table stores measurable information related to university activities.

## # Data Warehouse Process

**Raw Data → Data Cleaning → Dimension Tables → Fact Table → SQL Analysis**

The project includes:

* Preparing university data
* Cleaning and transforming the data
* Creating dimension tables
* Creating surrogate keys
* Creating the fact table
* Establishing table relationships
* Performing analytical SQL queries

## # Technologies Used

* SQL
* PostgreSQL
* Jupyter Notebook
* Python
* Pandas
* pgAdmin
* Star Schema

## # Business Analysis

The Data Warehouse can be used to analyze:

* Students by department
* Student enrollment
* Courses by department
* Instructor information
* Student performance
* Department-level statistics
* Course-related information
* Academic trends

## # Key Learning

This project helped me understand and practice:

* Data Warehouse architecture
* Star Schema
* Fact and Dimension tables
* ETL concepts
* Surrogate Keys
* Primary Keys
* Foreign Keys
* Data transformation
* SQL-based analysis
* PostgreSQL

## # Project Structure

```text
University-Data-Warehouse/
│
├── data/
│   └── university_data.csv
│
├── python/
│   └── data_cleaning.py
│
├── sql/
│   └── university_data_warehouse.sql
│
└── README.md
```

## # Project Goal

The goal of this project was to develop a practical University Data Warehouse and understand how university data can be organized into a Star Schema for analytical reporting.
