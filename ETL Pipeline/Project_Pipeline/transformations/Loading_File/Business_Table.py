import dlt 
from pyspark.sql.functions import *

@dlt.table(
    name = "Business_View"
)
def Business_View():
    df_sales = spark.read.table("Gold_Sales_1")
    df_sales_2 = spark.read.table("Gold_Sales_2")

    df_sales = df_sales.withColumn("Total_Revenue",col("Amount") * col("Quantity"))
    df_sales_2 = df_sales_2.withColumn("Total_Revenue",col("Unit_Price") * col("Units_Sold"))
    df_agg = df_sales.groupBy("City").agg(sum(col("Total_Revenue")).alias("City_Revenue"))

    return df_agg
