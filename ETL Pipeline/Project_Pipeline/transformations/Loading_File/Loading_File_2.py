import dlt 

@dlt.table(
    name = "Gold_Sales_2"
)

def Gold_Sales_2():
  df = spark.read.table("silver_sales_2")
  return df