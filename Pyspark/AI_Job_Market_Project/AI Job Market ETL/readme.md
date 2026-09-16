# AI Job Market PySpark Data Pipeline

## # Project Overview

This project is an **end-to-end PySpark data pipeline built on Databricks** using AI Job Market data.

The pipeline reads raw CSV data from a **Databricks Volume**, cleans and transforms the data using PySpark, performs feature engineering, and generates business insights through Spark DataFrame operations.

## # Project Workflow

**CSV File → Read Data → Remove Duplicates → Handle Missing Values → Clean Text → Fix Data Types → Feature Engineering → Business Analysis → Parquet Output**

## # Technologies Used

* Databricks
* PySpark
* Python
* Spark DataFrame API
* Databricks Volumes
* Parquet
* SQL-style DataFrame operations

## # Data Source

The project uses an AI Job Market dataset containing information such as:

* Job title
* Job category
* Experience level
* Education requirements
* City
* Country
* Industry
* Required skills
* Annual salary
* Minimum salary
* Maximum salary
* Years of experience
* Remote-friendly status
* Senior-level status
* LLM role status
* Demand score
* Posting year

## # Data Ingestion

The CSV file is read from a **Databricks Volume** using PySpark.

The pipeline uses:

* CSV format
* Header detection
* Automatic schema inference

```python
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferschema", True) \
    .load("/Volumes/workspace/csv_files/csv_files/ai_jobs_market_2025_2026.csv")
```

## # Data Cleaning

Several data-cleaning steps are performed before analysis.

### Remove Duplicates

Duplicate records are removed using:

```python
df = df.dropDuplicates()
```

### Handle Missing Values

Missing values are replaced with suitable default values.

For example:

* Missing job information → `unknown`
* Missing skills → `not specified`
* Missing salary values → `0`
* Missing experience → `0`

## # Text Cleaning

Text columns are standardized using `trim()` and `lower()`.

The following columns are cleaned:

* Job title
* Job category
* City
* Country
* Industry

This helps maintain consistent values during analysis.

## # Data Type Transformation

Important columns are converted into appropriate data types.

Examples include:

* Years of experience → Integer
* Salary columns → Double
* Remote-friendly → Boolean
* Senior role → Boolean
* LLM role → Boolean

## # Feature Engineering

New columns are created from the existing data to support analysis.

### Salary Range

The salary range is calculated as:

**Maximum Salary − Minimum Salary**

```python
salary_range = salary_max_usd - salary_min_usd
```

### Experience Category

Employees are categorized based on years of experience:

* Less than 2 years → Entry
* 2 to less than 5 years → Mid
* 5 or more years → Senior

### Remote Flag

A numerical flag is created:

* Yes → 1
* No → 0

### High Demand Flag

Jobs with a demand score greater than 80 are identified as high-demand jobs.

## # Business Analysis

After cleaning and transformation, PySpark is used to generate business insights.

### Top Paying Job Categories

Calculates the average salary for each job category and orders the results from highest to lowest average salary.

### Average Salary by Country

Calculates average annual salary for each country.

### Remote vs Non-Remote Salary Comparison

Compares:

* Average salary
* Total number of jobs

between remote-friendly and non-remote-friendly jobs.

### Top Cities with Highest Total Salary

Calculates the total salary value for jobs grouped by city.

### Most In-Demand Job Categories

Calculates total demand scores for each job category.

### Highest Paying Jobs

Identifies jobs with the highest annual salary along with:

* Job title
* Job category
* Salary
* City
* Country

### Yearly Job Posting Trend

Counts job postings by year to analyze changes in job-posting volume.

## # Data Output

After processing and analysis, the transformed DataFrame is written in **Parquet format**.

```python
df.write.mode("overwrite").parquet(
    "/Volumes/workspace/csv_files/csv_files/ai_jobs_market_2025_2026.csv"
)
```

Parquet provides a structured columnar format suitable for analytical workloads.

## # PySpark Concepts Practiced

This project demonstrates practical use of:

* `SparkSession`
* `spark.read`
* CSV ingestion
* `dropDuplicates()`
* `fillna()`
* `withColumn()`
* `lower()`
* `trim()`
* `cast()`
* `when()`
* `groupBy()`
* `agg()`
* `avg()`
* `sum()`
* `count()`
* `round()`
* `orderBy()`
* `select()`
* `dropDuplicates()`
* Parquet writing

## # Databricks Concepts Practiced

* Databricks Notebooks
* Databricks Volumes
* Spark Session
* PySpark DataFrames
* Distributed data processing
* Data transformation
* Data analysis
* Parquet storage

## # Project Structure

```text
AI-Job-Market-PySpark-Pipeline/
│
├── data/
│   └── ai_jobs_market_2025_2026.csv
│
├── notebooks/
│   └── AI_Job_Market_PySpark_Pipeline.py
│
└── README.md
```

## # Project Objective

The objective of this project was to build a practical **PySpark data pipeline in Databricks** and gain hands-on experience with data ingestion, data cleaning, transformation, feature engineering, analytical processing, and Parquet data storage.

## # Key Learning

Through this project, I practiced how to:

* Process data using PySpark
* Work with Databricks Volumes
* Clean large datasets using Spark DataFrames
* Transform data types
* Create new analytical features
* Perform aggregations using PySpark
* Generate business insights
* Store processed data in Parquet format
* Build an end-to-end data processing pipeline
