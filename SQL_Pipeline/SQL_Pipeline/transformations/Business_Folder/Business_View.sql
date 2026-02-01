CREATE OR REFRESH LIVE TABLE gold_combined_business_view
AS
SELECT
    t1.date,
    t1.close AS close_price,
    t1.price_change_pct,
    t1.volume,
    t2.close_first,
    t2.close_last,
    t2.revenue_total,
    t3.close_max,
    t3.close_min,
    t4.rsi_14,
    t4.volatility_20d,
    t4.macd
FROM LIVE.gold_table_1 t1
JOIN LIVE.gold_table_2 t2
    ON t1.date = t2.date
JOIN LIVE.gold_table_3 t3
    ON t1.date = t3.date
JOIN LIVE.gold_table_4 t4
    ON t1.date = t4.date
WHERE t4.volatility_20d > 0.02       
  AND t1.price_change_pct > 0;        
