import dlt
from pyspark.sql.functions import col

@dlt.table(
    name="silver_sales_2",
   
)
def silver_sales_clean():

    df = dlt.read("bronze_sales_2")

    
    df_clean = df.filter(
        col("Region").isNotNull() &
        col("Country").isNotNull() &
        col("Item_Type").isNotNull() &
        col("Sales_Channel").isNotNull() &
        col("Order_Priority").isNotNull() &
        col("Ship_Date").isNotNull() &
        col("Units_Sold").isNotNull() &
        col("Unit_Price").isNotNull() &
        col("Unit_Cost").isNotNull() &
        col("Total_Cost").isNotNull() &
        col("Total_Profit").isNotNull()
    )

   
    df_clean = df_clean.dropDuplicates()

    return df_clean
