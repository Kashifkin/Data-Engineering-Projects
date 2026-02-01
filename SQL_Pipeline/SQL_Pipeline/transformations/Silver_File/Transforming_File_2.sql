CREATE OR REFRESH LIVE TABLE silver_table_2

AS
SELECT DISTINCT
    COALESCE(fiscal_year, 0) AS fiscal_year,
    COALESCE(fiscal_quarter, 0) AS fiscal_quarter,
    COALESCE(quarter_start, date'1970-01-01') AS quarter_start,
    COALESCE(quarter_end, date'1970-01-01') AS quarter_end,
    COALESCE(open_price, 0) AS open_price,
    COALESCE(close_price, 0) AS close_price,
    COALESCE(high_price, 0) AS high_price,
    COALESCE(low_price, 0) AS low_price,
    COALESCE(total_volume, 0) AS total_volume,
    COALESCE(avg_daily_return, 0) AS avg_daily_return,
    COALESCE(return_volatility, 0) AS return_volatility,
    COALESCE(quarter_return, 0) AS quarter_return,
    COALESCE(avg_volatility, 0) AS avg_volatility,
    COALESCE(avg_rsi, 50) AS avg_rsi,               
    COALESCE(price_return_pct, 0) AS price_return_pct

FROM bronze_table_2;
