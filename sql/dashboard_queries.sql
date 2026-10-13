-- Queries behind the "Food Delivery Ops" Databricks SQL / AI/BI dashboard.
-- Replace `workspace` with your catalog if different.

-- 1. City scorecard (today vs. promised SLA)
SELECT city, total_orders, delivered_orders, cancellation_pct,
       avg_delivery_minutes, p90_delivery_minutes, sla_breach_pct, gmv, commission_revenue
FROM workspace.gold.kpi_city_daily
WHERE order_date = (SELECT max(order_date) FROM workspace.gold.kpi_city_daily)
ORDER BY sla_breach_pct DESC;

-- 2. Hourly demand curve per city (lunch / dinner peaks)
SELECT city, hour(from_utc_timestamp(placed_ts, 'Asia/Kolkata')) AS hour_ist, count(*) AS orders,
       round(avg(delivery_minutes), 1) AS avg_delivery_minutes
FROM workspace.gold.fact_order_lifecycle
GROUP BY ALL
ORDER BY city, hour_ist;

-- 3. Where is time lost? Accept vs. kitchen vs. last mile
SELECT city,
       round(avg(accept_minutes), 1)    AS avg_accept,
       round(avg(prep_minutes), 1)      AS avg_kitchen,
       round(avg(last_mile_minutes), 1) AS avg_last_mile
FROM workspace.gold.fact_order_lifecycle
WHERE order_status = 'DELIVERED'
GROUP BY city;

-- 4. Why are orders cancelled?
SELECT city, cancel_reason, count(*) AS orders
FROM workspace.gold.fact_order_lifecycle
WHERE order_status = 'CANCELLED'
GROUP BY city, cancel_reason
ORDER BY orders DESC;

-- 5. Slowest kitchens (candidates for prep-time SLA conversations)
SELECT restaurant_name, city, cuisine, orders, p50_prep_minutes, p90_prep_minutes
FROM workspace.gold.restaurant_performance
WHERE orders >= 20
ORDER BY p90_prep_minutes DESC
LIMIT 15;

-- 6. Zones under pressure right now (feeds surge pricing / rider nudges)
SELECT window_start, city, zone, orders_placed, idle_riders, demand_supply_ratio, surge_flag
FROM workspace.gold.zone_demand_supply_5m
WHERE window_start >= (SELECT max(window_start) FROM workspace.gold.zone_demand_supply_5m) - INTERVAL 1 HOUR
  AND surge_flag <> 'NORMAL'
ORDER BY demand_supply_ratio DESC;

-- 7. Does a rider shortage cause cancellations? (join the two Gold outputs)
SELECT z.city, z.surge_flag,
       count(DISTINCT f.order_id)                                              AS orders,
       round(100 * avg(CASE WHEN f.cancel_reason = 'NO_RIDER_AVAILABLE' THEN 1 ELSE 0 END), 2) AS no_rider_cancel_pct
FROM workspace.gold.fact_order_lifecycle f
JOIN workspace.gold.zone_demand_supply_5m z
  ON f.city = z.city AND f.zone = z.zone
 AND f.placed_ts >= z.window_start AND f.placed_ts < z.window_end
GROUP BY z.city, z.surge_flag
ORDER BY z.city, z.surge_flag;

-- 8. SCD Type 2 in action: commission history of one restaurant
SELECT restaurant_id, commission_pct, is_active, cuisine, __START_AT, __END_AT
FROM workspace.silver.dim_restaurant
WHERE restaurant_id IN (SELECT restaurant_id FROM workspace.silver.dim_restaurant
                        GROUP BY restaurant_id HAVING count(*) > 1 LIMIT 1)
ORDER BY __START_AT;

-- 9. Data quality trend (from the pipeline event log)
SELECT update_ts, dataset, expectation, failed_records, pass_rate_pct
FROM workspace.gold.dq_expectation_results
ORDER BY update_ts DESC, pass_rate_pct;
