CREATE OR REFRESH LIVE TABLE silver_table_3
AS
SELECT DISTINCT
    COALESCE(fiscal_year, 0) AS fiscal_year,
    COALESCE(fiscal_quarter, 0) AS fiscal_quarter,
    COALESCE(close_first, 0) AS close_first,
    COALESCE(close_last, 0) AS close_last,
    COALESCE(close_max, 0) AS close_max,
    COALESCE(close_min, 0) AS close_min,
    COALESCE(close_mean, 0) AS close_mean,
    COALESCE(close_std, 0) AS close_std,
    COALESCE(volume_sum, 0) AS volume_sum,
    COALESCE(volume_mean, 0) AS volume_mean,
    COALESCE(price_range_mean, 0) AS price_range_mean,
    COALESCE(volatility_20d_mean, 0) AS volatility_20d_mean,
    COALESCE(rsi_14_mean, 50) AS rsi_14_mean,       
    COALESCE(return_1d_mean, 0) AS return_1d_mean,
    COALESCE(return_1d_std, 0) AS return_1d_std,
    COALESCE(revenue_mac, 0) AS revenue_mac,
    COALESCE(revenue_services, 0) AS revenue_services,
    COALESCE(revenue_wearables_other, 0) AS revenue_wearables_other,
    COALESCE(revenue_ipad, 0) AS revenue_ipad,
    COALESCE(revenue_iphone, 0) AS revenue_iphone,
    COALESCE(revenue_total, 0) AS revenue_total,
    COALESCE(share_mac, 0) AS share_mac,
    COALESCE(share_services, 0) AS share_services,
    COALESCE(share_wearables_other, 0) AS share_wearables_other,
    COALESCE(share_ipad, 0) AS share_ipad,
    COALESCE(share_iphone, 0) AS share_iphone

FROM bronze_table_3;
