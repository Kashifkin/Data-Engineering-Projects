# Hospital Data Warehouse

## # Project Overview

This project focuses on building a **Hospital Data Warehouse** using a **Star Schema**.

The objective is to organize hospital-related data into a structured data warehouse that can be used for reporting, operational analysis, and business insights.

## # Data Warehouse Architecture

The project follows a **Star Schema** consisting of:

* Fact Table
* Patient Dimension
* Doctor Dimension
* Hospital/Department Dimensions
* Date Dimension
* Other supporting dimensions
* Primary Keys
* Foreign Keys
* Surrogate Keys

The fact table stores measurable healthcare-related events, while dimension tables provide descriptive information.

## # Data Warehouse Process

**Raw Hospital Data → Data Cleaning → Dimension Tables → Fact Table → SQL Analysis**

The project includes:

* Preparing hospital data
* Cleaning and transforming the data
* Creating dimension tables
* Creating surrogate keys
* Creating the fact table
* Establishing relationships
* Performing analytical SQL queries

## # Technologies Used

* Python
* Pandas
* SQL
* PostgreSQL
* Jupyter Notebook
* pgAdmin
* Star Schema

## # Business Analysis

The Data Warehouse can be used to analyze:

* Patient information
* Patient visits
* Doctors and departments
* Hospital admissions
* Treatment information
* Patient activity
* Department performance
* Medical-related trends
* Hospital operations

## # Key Learning

This project helped me practice:

* Data Warehouse design
* Star Schema
* Fact and Dimension tables
* ETL concepts
* Data cleaning
* Surrogate Keys
* Primary Keys
* Foreign Keys
* SQL analysis
* PostgreSQL
* Data transformation using Python

## # Project Structure

```text
Hospital-Data-Warehouse/
│
├── data/
│   └── hospital_data.csv
│
├── python/
│   └── data_cleaning.py
│
├── sql/
│   └── hospital_data_warehouse.sql
│
└── README.md
```

## # Project Goal

The goal of this project was to build a practical Hospital Data Warehouse and understand how hospital data can be transformed into a Star Schema for structured analytical reporting.
