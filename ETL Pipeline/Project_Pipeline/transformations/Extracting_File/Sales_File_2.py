import dlt 

sales_rules = {
    "region_not_null": "Region IS NOT NULL",
    "country_not_null": "Country IS NOT NULL",
    "item_type_not_null": "Item_Type IS NOT NULL",
    "sales_channel_not_null": "Sales_Channel IS NOT NULL",
    "order_priority_not_null": "Order_Priority IS NOT NULL",
    "ship_date_not_null": "Ship_Date IS NOT NULL",
    "units_sold_positive": "Units_Sold > 0",
    "unit_price_positive": "Unit_Price > 0",
    "unit_cost_positive": "Unit_Cost > 0",
    "total_cost_positive": "Total_Cost > 0",
    "total_profit_not_null": "Total_Profit IS NOT NULL"
}


@dlt.table(
    name = "bronze_sales_2"
)

@dlt.expect_all_or_drop(sales_rules)
def bronze_sales():
    df = spark.read.format("csv")\
        .option("header", True)\
        .option("inferschema", True)\
        .load("/Volumes/workspace/default/csv_file/sales.csv")

    return df
        