import dlt
from pyspark.sql.functions import col

@dlt.table(
    name="silver_sales",
    
)
def silver_orders_clean():

    df = dlt.read("bronze_sales")

    df_clean = (
        df.filter(
            col("Order_ID").isNotNull() &
            col("Amount").isNotNull() &
            col("Profit").isNotNull() &
            col("Quantity").isNotNull() &
            col("Category").isNotNull() &
            col("Sub-Category").isNotNull() &
            col("PaymentMode").isNotNull() &
            col("Order_Date").isNotNull() &
            col("CustomerName").isNotNull() &
            col("State").isNotNull() &
            col("City").isNotNull() &
            col("Year-Month").isNotNull()
        )
        .dropDuplicates([
            "Order_ID",
            "Order_Date",
            "CustomerName"]))

    return df_clean
