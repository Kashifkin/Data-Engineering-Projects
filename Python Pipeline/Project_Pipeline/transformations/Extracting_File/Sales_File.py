import dlt 

orders_rules = {
    "order_id_not_null": "`Order_ID` IS NOT NULL",
    "amount_positive": "`Amount` > 0",
    "profit_not_null": "`Profit` IS NOT NULL",
    "quantity_positive": "`Quantity` > 0",
    "category_not_null": "`Category` IS NOT NULL",
    "sub_category_not_null": "`Sub-Category` IS NOT NULL",
    "payment_mode_valid": "`PaymentMode` IS NOT NULL",
    "order_date_not_null": "`Order_Date` IS NOT NULL",
    "customer_name_not_null": "`CustomerName` IS NOT NULL",
    "state_not_null": "`State` IS NOT NULL",
    "city_not_null": "`City` IS NOT NULL",
    "year_month_not_null": "`Year-Month` IS NOT NULL"
}



@dlt.table(
    name = "bronze_sales"
)

@dlt.expect_all_or_drop(orders_rules)
def bronze_sales():
    df = spark.read.format("csv")\
        .option("header", True)\
        .option("inferschema", True)\
        .load("/Volumes/workspace/default/csv_file/Sales Dataset.csv")

    return df
        