# PySpark Ride-Sharing ETL and Data Engineering Project

## Project Overview

This project demonstrates an end-to-end data engineering workflow using PySpark and Databricks.

The project processes ride-sharing data from multiple CSV files, performs data cleaning and transformation, joins related datasets, and creates business-focused Gold tables for analysis.

The main datasets used in the project are:

* Customers
* Drivers
* Locations
* Payments
* Trips
* Vehicles

The project uses PySpark DataFrame operations such as reading CSV files, removing duplicates, handling missing values, changing data types, creating new columns, joining datasets, grouping data, and aggregating business metrics.

## Technologies Used

* Python
* PySpark
* Apache Spark
* Databricks
* CSV
* Spark SQL
* DataFrame API

## Project Architecture

The project follows a simple ETL architecture:

```text
CSV Files
   |
   v
PySpark Extraction
   |
   v
Data Cleaning and Transformation
   |
   v
Data Integration and Joins
   |
   v
Gold Business Tables
   |
   v
Business Analysis
```

## Source Datasets

### Customers

The Customers dataset contains customer information such as:

* customer_id
* first_name
* last_name
* email
* phone_number
* city
* signup_date
* last_updated_timestamp

The data is loaded from CSV using Spark with headers and schema inference.

### Drivers

The Drivers dataset contains:

* driver_id
* first_name
* last_name
* phone_number
* vehicle_id
* driver_rating
* city
* last_updated_timestamp

The driver data is loaded and processed using PySpark.

### Locations

The Locations dataset contains:

* location_id
* city
* state
* country
* latitude
* longitude
* last_updated_timestamp

This dataset provides geographical information that can be connected with trip locations.

### Payments

The Payments dataset contains:

* payment_id
* trip_id
* customer_id
* payment_method
* payment_status
* amount
* transaction_time
* last_updated_timestamp

Payment data is cleaned by removing duplicates, filling missing values, and converting the amount column to a numeric data type.

### Trips

The Trips dataset contains the main ride transaction information:

* trip_id
* driver_id
* customer_id
* vehicle_id
* trip_start_time
* trip_end_time
* start_location
* end_location
* distance_km
* fare_amount
* payment_method
* trip_status
* last_updated_timestamp

The Trips dataset is used as the main transactional dataset for several business analyses.

### Vehicles

The Vehicles dataset contains:

* vehicle_id
* license_plate
* model
* make
* year
* vehicle_type
* last_updated_timestamp

Additional columns such as vehicle age and vehicle category are created during transformation.

## ETL Process

### Extract

The project reads the six CSV datasets using PySpark:

```python
spark.read.format("csv") \
    .option("header", True) \
    .option("inferSchema", True) \
    .load("file_path")
```

The datasets are loaded into Spark DataFrames for further processing.

## Transform

The transformation layer includes several common data engineering operations.

### Remove Duplicates

Duplicate records are removed from datasets such as Payments and Vehicles.

```python
payments = payments.dropDuplicates()
```

### Handle Missing Values

Missing values are replaced with predefined values where required.

For example, payment fields use values such as `unknown`, `pending`, and `0.0` for missing data.

### Data Type Conversion

Columns are converted into appropriate data types.

For example:

```python
payments = payments.withColumn(
    "amount",
    col("amount").cast("double")
)
```

### Derived Columns

The vehicle dataset is enhanced with calculated columns.

Vehicle age is calculated using the year of the vehicle:

```python
vehicles = vehicles.withColumn(
    "Vehicle_age",
    2026 - col("year")
)
```

A vehicle category is also created based on vehicle type, including categories such as Standard, Economy, Premium, Luxury, and commercial.

## Data Integration

The project combines different datasets using PySpark joins.

For example, trips are joined with vehicles to analyze the number of trips by vehicle type:

```python
vehicle_trips = trips.join(
    vehicles,
    trips.vehicle_id == vehicles.vehicle_id,
    "inner"
)
```

The project also combines customers, trips, payments, drivers, vehicles, and locations to create a broader customer-level business view.

## Gold Layer

The Gold layer contains business-oriented datasets designed for analysis.

### Customer Trip Analysis

Customer trip counts are calculated by joining Customers and Trips and grouping by customer.

```python
customer_trips = customers.join(
    trips,
    customers.customer_id == trips.customer_id,
    "inner"
).groupBy(
    customers.customer_id,
    customers.full_name
).agg(
    count("trip_id").alias("total_trips")
)
```

The resulting data provides the number of trips associated with each customer.

### Vehicle Trip Analysis

Trips are grouped by vehicle type to understand trip volume across different vehicle categories.

The project produces counts for:

* Luxury
* SUV
* Sedan
* Van
* Hatchback

### Payment Analysis

Payment amounts are grouped by payment method.

The project analyzes:

* Cash
* Card
* Wallet

and calculates the total payment amount for each method.

### Driver Earnings Analysis

Driver earnings are calculated by joining Trips with Drivers and aggregating trip fare amounts for each driver.

```python
driver_earnings = trips.join(
    drivers,
    trips.driver_id == drivers.driver_id,
    "inner"
).groupBy(
    drivers.driver_id,
    drivers.Full_Name
).agg(
    round(
        sum(trips.fare_amount),
        2
    ).alias("total_earnings")
)
```

This produces a driver-level earnings dataset.

## Gold Customer Summary

A detailed customer business view is created by combining:

* Customers
* Trips
* Payments
* Drivers
* Vehicles
* Locations

The resulting dataset contains information such as:

* customer_id
* full_name
* email
* city
* driver_name
* vehicle_type
* total_trips
* total_payment

Customers with more than three trips are filtered and ordered by total payment.

The final Gold table is stored as:

```text
gold_customer_summary
```

The table is created using Spark's `saveAsTable()` functionality.

## Business Questions Answered

This project can be used to answer questions such as:

1. How many trips has each customer completed?
2. Which vehicle types are used for the most trips?
3. How much payment is received through each payment method?
4. How much has each driver earned from trips?
5. Which customers have more than three trips?
6. What is the total payment associated with frequent customers?
7. Which drivers are associated with customer trips?
8. What vehicle type is associated with different customers?
9. How can customer, driver, vehicle, payment, and trip information be combined into a single business view?

## Key PySpark Concepts Demonstrated

This project demonstrates practical use of:

* SparkSession
* Reading CSV files
* DataFrames
* Schema inference
* `dropDuplicates()`
* `fillna()`
* `withColumn()`
* `cast()`
* `when()`
* `join()`
* `groupBy()`
* `count()`
* `sum()`
* `round()`
* `filter()`
* `orderBy()`
* `saveAsTable()`
* Spark SQL
* Data integration
* Business aggregations

## Project Structure

A simple GitHub structure for this project can be:

```text
pyspark-ride-sharing-etl/
│
├── README.md
├── ride_sharing_etl.py
│
├── data/
│   ├── customers.csv
│   ├── drivers.csv
│   ├── locations.csv
│   ├── payments.csv
│   ├── trips.csv
│   └── vehicles.csv
│
└── sql/
    └── gold_analysis.sql
```

## How to Run

### Step 1: Open Databricks

Open a Databricks workspace and create a notebook.

### Step 2: Create a Spark Session

Initialize the Spark application.

### Step 3: Load the CSV Files

Load Customers, Drivers, Locations, Payments, Trips, and Vehicles into Spark DataFrames.

### Step 4: Clean the Data

Remove duplicates, handle missing values, and convert columns to appropriate data types.

### Step 5: Transform the Data

Create calculated columns such as vehicle age and vehicle category.

### Step 6: Join the Datasets

Connect the datasets using their relevant IDs and location fields.

### Step 7: Create Gold Business Views

Generate customer, vehicle, payment, and driver-level business summaries.

### Step 8: Save Gold Tables

Save the final customer summary as:

```text
gold_customer_summary
```

## Project Objective

The main objective of this project is to demonstrate how raw CSV data can be transformed into structured and business-ready datasets using PySpark.

It provides practical experience with the core responsibilities of a data engineer:

```text
Extract
  ↓
Clean
  ↓
Transform
  ↓
Join
  ↓
Aggregate
  ↓
Create Business Tables
```

## Conclusion

This project demonstrates a practical PySpark ETL workflow for a ride-sharing business. It combines multiple operational datasets, performs data cleaning and transformation, integrates related information, and produces Gold-level business summaries for analysis.

The project is designed to demonstrate practical data engineering skills using Python, PySpark, Databricks, and Spark SQL.
