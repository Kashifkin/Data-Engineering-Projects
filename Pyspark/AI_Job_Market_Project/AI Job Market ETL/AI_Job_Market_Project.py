# Databricks notebook source
# MAGIC %md
# MAGIC ##Spark Session Builder

# COMMAND ----------

from pyspark.sql import *
from pyspark.sql.functions import  *

spark = SparkSession.builder \
    .appName("Sales Data Pipeline") \
    .getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reading CSV File

# COMMAND ----------

df = spark.read.format("csv").option("header", "true")\
    .option("inferschema",True)\
    .load("/Volumes/workspace/csv_files/csv_files/ai_jobs_market_2025_2026.csv")

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Drop Duplicates

# COMMAND ----------

df = df.dropDuplicates()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Handle Missing Values

# COMMAND ----------

df = df.fillna({
    "job_title": "unknown",
    "job_category": "unknown",
    "experience_level": "unknown",
    "education_required": "unknown",
    "city": "unknown",
    "country": "unknown",
    "industry": "unknown",
    "required_skills": "not specified",
    "annual_salary_usd": 0,
    "salary_min_usd": 0,
    "salary_max_usd": 0,
    "years_of_experience": 0
})

# COMMAND ----------

# MAGIC %md
# MAGIC ##Clean Text Columns

# COMMAND ----------

df = df.withColumn("job_title", lower(trim(col("job_title"))))
df = df.withColumn("job_category", lower(trim(col("job_category"))))
df = df.withColumn("city", lower(trim(col("city"))))
df = df.withColumn("country", lower(trim(col("country"))))
df = df.withColumn("industry", lower(trim(col("industry"))))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Fix Data Types

# COMMAND ----------

df = df.withColumn("years_of_experience", col("years_of_experience").cast("int"))
df = df.withColumn("annual_salary_usd", col("annual_salary_usd").cast("double"))
df = df.withColumn("salary_min_usd", col("salary_min_usd").cast("double"))
df = df.withColumn("salary_max_usd", col("salary_max_usd").cast("double"))
df = df.withColumn("is_remote_friendly", col("is_remote_friendly").cast("boolean"))
df = df.withColumn("is_senior", col("is_senior").cast("boolean"))
df = df.withColumn("is_llm_role", col("is_llm_role").cast("boolean"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Feature Engineering

# COMMAND ----------

df = df.withColumn(
    "salary_range",
    col("salary_max_usd") - col("salary_min_usd")
)

# COMMAND ----------

df = df.withColumn(
    "experience_category",
    when(col("years_of_experience") < 2, "entry")
    .when(col("years_of_experience") < 5, "mid")
    .otherwise("senior")
)

# COMMAND ----------

df = df.withColumn(
    "remote_flag",
    when(col("remote_work") == "Yes", 1).otherwise(0)
)

# COMMAND ----------

df = df.withColumn(
    "high_demand",
    when(col("demand_score") > 80, 1).otherwise(0)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Business Insights

# COMMAND ----------

# MAGIC %md
# MAGIC ##Top Paying Job Categories

# COMMAND ----------

from pyspark.sql.functions import col, avg, round

df.groupBy("job_category") \
  .agg(round(avg("annual_salary_usd"), 2).alias("avg_salary")) \
  .orderBy(col("avg_salary").desc()) \
  .display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Average Salary by Country

# COMMAND ----------

df.groupBy("country") \
  .agg(round(avg("annual_salary_usd"),2).alias("avg(annual_salary_usd)")) \
  .orderBy(col("avg(annual_salary_usd)").desc()) \
  .display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Remote vs Non-Remote Salary Comparison

# COMMAND ----------

df.groupBy("is_remote_friendly") \
  .agg(round(avg("annual_salary_usd"), 2).alias("avg_salary"),
    count("*").alias("total_jobs")) \
  .orderBy(col("avg_salary").desc()) \
  .display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Top Cities with Highest Paying Jobs

# COMMAND ----------

from pyspark.sql.functions import sum, col, round

df.groupBy("city") \
  .agg(round(sum("annual_salary_usd"), 2).alias("total_salary")) \
  .orderBy(col("total_salary").desc()) \
  .display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Most In-Demand Job Categories

# COMMAND ----------

df.groupBy("job_category") \
  .agg(round(sum("demand_score"), 2).alias("total_demand")) \
  .orderBy(col("total_demand").desc()) \
  .display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Highest Paying Job

# COMMAND ----------

df.orderBy(col("annual_salary_usd").desc()) \
  .select("job_title", "job_category", "annual_salary_usd", "city", "country").dropDuplicates() \
  .display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Yearly Trend of Job Postings

# COMMAND ----------

df.groupBy("posting_year") \
  .count() \
  .orderBy("posting_year") \
  .display()

# COMMAND ----------

df.write.mode("overwrite").parquet(
    "/Volumes/workspace/csv_files/csv_files/ai_jobs_market_2025_2026.csv")

# COMMAND ----------

spark.stop()

# COMMAND ----------

print("Pipeline executed successfully on Databricks Volume")

# COMMAND ----------

