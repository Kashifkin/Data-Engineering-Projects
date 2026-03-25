import dlt 

@dlt.table(
    name = "Gold_Sales_1"
)

def Gold_Sales_1():
  df = spark.read.table("silver_sales")
  return df