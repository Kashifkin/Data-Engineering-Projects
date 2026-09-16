# Data Engineering ETL Pipeline and Business Insights

## Project Overview

This project demonstrates a simple end-to-end **ETL (Extract, Transform, Load) pipeline** for job market data.

The project uses **Python, Pandas, PostgreSQL, SQLAlchemy, psycopg2, and SQL** to extract raw job data from a CSV file, clean and transform the data, load the processed data into PostgreSQL, and answer business questions using SQL.

The project follows a **Bronze → Silver → Gold** architecture.

# Project Architecture

```text
DataEngineer.csv
       |
       v
+----------------+
| Bronze Layer   |
| Raw CSV Data   |
+----------------+
       |
       v
+----------------+
| Silver Layer   |
| Clean &        |
| Transform Data |
+----------------+
       |
       v
+----------------+
| Gold Layer     |
| PostgreSQL     |
+----------------+
       |
       v
+----------------+
| Business       |
| Insights       |
| SQL Queries    |
+----------------+
```

# Technologies Used

* Python
* Pandas
* PostgreSQL
* SQLAlchemy
* psycopg2
* SQL
* Jupyter Notebook
* GitHub

# Dataset

The project uses a job market dataset containing information about job postings and companies.

Important columns include:

* Job Title
* Salary Estimate
* Job Description
* Rating
* Company Name
* Location
* Headquarters
* Size
* Founded
* Type of Ownership
* Industry
* Sector
* Revenue
* Competitors
* Easy Apply

# ETL Pipeline

## 1. Extract - Bronze Layer

The raw CSV file is loaded into a Pandas DataFrame.

```python
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from datetime import datetime

bronze = pd.read_csv("DataEngineer.csv")

bronze.head()
```

The Bronze layer represents the **raw source data** before applying transformations.

# 2. Transform - Silver Layer

A copy of the Bronze DataFrame is created for transformation.

```python
silver = bronze.copy()
```

## Data Quality Checks

The dataset is checked for missing values.

```python
silver.isnull().sum()
```

Duplicate records are also identified.

```python
silver.duplicated().sum()
```

The duplicate records are removed.

```python
silver.drop_duplicates(inplace=True)
```

# Data Transformations

## Easy Apply Conversion

The `Easy Apply` column is converted to Boolean values.

```python
silver["Easy Apply"] = silver["Easy Apply"].astype("bool")
```

## Competitors Conversion

The `Competitors` column is converted to Boolean values.

```python
silver["Competitors"] = silver["Competitors"].astype("bool")
```

## Company Size Categorization

Company sizes are grouped into three categories:

* Small
* Medium
* Large

```python
def categorize_company(size):

    if size in ["1 to 50 employees", "51 to 200 employees"]:
        return "Small"

    elif size in ["201 to 500 employees", "501 to 1000 employees"]:
        return "Medium"

    else:
        return "Large"

silver["Company_Size_Category"] = silver["Size"].apply(categorize_company)
```

# Company Age Calculation

The company's age is calculated using the current year and the year the company was founded.

```python
current_year = datetime.now().year

silver["Company_Age"] = current_year - silver["Founded"]
```

This creates a new `Company_Age` column.

# Company Age Categorization

Companies are categorized based on their age.

```python
def company_category(age):

    if age <= 10:
        return "Startup"

    elif age <= 25:
        return "Growing"

    elif age <= 50:
        return "Established"

    else:
        return "Legacy"

silver["Company_Age_Category"] = silver["Company_Age"].apply(company_category)
```

The resulting categories are:

* Startup
* Growing
* Established
* Legacy

# Rating Categorization

Company ratings are grouped into three categories.

```python
def rating_category(rating):

    if rating >= 4:
        return "Excellent"

    elif rating >= 3:
        return "Good"

    else:
        return "Low"

silver["Rating_Category"] = silver["Rating"].apply(rating_category)
```

# 3. Load - Gold Layer

The transformed Silver DataFrame is loaded into a PostgreSQL database.

A PostgreSQL connection is created using `psycopg2` and SQLAlchemy.

```python
connection = psycopg2.connect(
    host="localhost",
    port="5432",
    database="Business_Table",
    user="postgres",
    password="1234"
)

engine = create_engine(
    "postgresql+psycopg2://postgres:1234@localhost:5432/Business_Table"
)
```

The transformed DataFrame can then be loaded into PostgreSQL using:

```python
silver.to_sql(
    name="Gold_Table",
    con=engine,
    if_exists="replace",
    index=False
)
```

The Gold table contains the cleaned and transformed data that is used for business analysis.

# Important Correction

During the project, the following error occurred:

```text
NameError: name 'df' is not defined
```

The reason was that the DataFrame was named `silver`, not `df`.

Incorrect:

```python
df.to_sql(...)
```

Correct:

```python
silver.to_sql(...)
```

# Business Insights

After loading the Gold table into PostgreSQL, SQL queries are used to answer business questions.

## 1. Average Salary by Job Title

This query calculates the average salary estimate for each job title.

```sql
SELECT
    "Job Title",
    ROUND(AVG(
        NULLIF(
            REGEXP_REPLACE(
                SPLIT_PART("Salary Estimate", '-', 1),
                '[^0-9]', '', 'g'
            ), ''
        )::NUMERIC
    )) AS average_salary
FROM "Gold_Table"
GROUP BY "Job Title"
ORDER BY average_salary DESC;
```

# 2. Number of Jobs by Industry

```sql
SELECT
    "Industry",
    COUNT(*) AS total_jobs
FROM "Gold_Table"
GROUP BY "Industry"
ORDER BY total_jobs DESC;
```

This identifies the industries with the highest number of job postings.

# 3. Number of Jobs by Company Size

```sql
SELECT
    "Company_Size_Category",
    COUNT(*) AS total_jobs
FROM "Gold_Table"
GROUP BY "Company_Size_Category"
ORDER BY total_jobs DESC;
```

# 4. Average Company Rating by Industry

```sql
SELECT
    "Industry",
    ROUND(AVG("Rating")::NUMERIC, 2) AS average_rating
FROM "Gold_Table"
WHERE "Rating" IS NOT NULL
GROUP BY "Industry"
ORDER BY average_rating DESC;
```

# 5. Jobs Available Through Easy Apply

```sql
SELECT
    "Easy Apply",
    COUNT(*) AS total_jobs
FROM "Gold_Table"
GROUP BY "Easy Apply"
ORDER BY total_jobs DESC;
```

# 6. Jobs by Company Size Category

```sql
SELECT
    "Company_Size_Category",
    COUNT(*) AS total_jobs
FROM "Gold_Table"
GROUP BY "Company_Size_Category"
ORDER BY total_jobs DESC;
```

# 7. Jobs by Company Age Category

```sql
SELECT
    "Company_Age_Category",
    COUNT(*) AS total_jobs
FROM "Gold_Table"
GROUP BY "Company_Age_Category"
ORDER BY total_jobs DESC;
```

# 8. Average Company Age by Industry

```sql
SELECT
    "Industry",
    ROUND(AVG("Company_Age")::NUMERIC, 2) AS average_company_age
FROM "Gold_Table"
WHERE "Company_Age" IS NOT NULL
GROUP BY "Industry"
ORDER BY average_company_age DESC;
```

# 9. Jobs by Rating Category

```sql
SELECT
    "Rating_Category",
    COUNT(*) AS total_jobs
FROM "Gold_Table"
GROUP BY "Rating_Category"
ORDER BY total_jobs DESC;
```

# 10. Job Opportunities by Location

```sql
SELECT
    "Location",
    COUNT(*) AS total_jobs
FROM "Gold_Table"
GROUP BY "Location"
ORDER BY total_jobs DESC
LIMIT 20;
```

This identifies the locations with the largest number of job postings in the dataset.

# Project Structure

```text
Data-Engineering-ETL-Project/
│
├── DataEngineer.csv
│
├── ETL_Pipeline.ipynb
│
├── Business_Insights.sql
│
└── README.md
```

# ETL Flow

```text
CSV File
   |
   v
Extract using Pandas
   |
   v
Bronze Layer
   |
   v
Data Quality Checks
   |
   v
Remove Duplicates
   |
   v
Transform Data
   |
   +---- Company Size Category
   |
   +---- Company Age
   |
   +---- Company Age Category
   |
   +---- Rating Category
   |
   v
Silver Layer
   |
   v
Load into PostgreSQL
   |
   v
Gold Table
   |
   v
SQL Business Analysis
```

# Key Data Engineering Concepts Demonstrated

This project demonstrates several practical Data Engineering concepts:

* ETL pipeline development
* Bronze, Silver, and Gold data layers
* CSV data ingestion
* Data quality checks
* Duplicate removal
* Data transformation
* Feature creation
* Data type conversion
* PostgreSQL data loading
* SQLAlchemy
* psycopg2
* SQL aggregation
* `GROUP BY`
* `COUNT`
* `AVG`
* `ORDER BY`
* SQL string manipulation
* Business-oriented data analysis

# Project Outcome

The project converts raw job-market data into a structured dataset suitable for analysis.

The final Gold table in PostgreSQL can be queried to investigate:

* Salary patterns
* Job demand by industry
* Job demand by company size
* Company ratings
* Easy Apply availability
* Company age
* Job locations
* Company characteristics

This project demonstrates how a Data Engineer can build a basic ETL pipeline and prepare reliable data for downstream business analysis.

# Future Improvements

Possible improvements to the pipeline include:

* Incremental data loading
* Data validation rules
* Logging and error handling
* PostgreSQL staging tables
* Star schema data warehouse
* Automated ETL scheduling
* Airflow orchestration
* Data quality monitoring
* Power BI dashboard connected to PostgreSQL
* Cloud-based storage and processing
