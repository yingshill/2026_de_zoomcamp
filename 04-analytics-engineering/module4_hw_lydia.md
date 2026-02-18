
1. command 'dbt run --select' -> '--select' is a filter to run only specfic parts, not build everything

2. When you run dbt test, dbt generates a SQL query that looks for any rows where payment_type is NOT in your defined list (e.g., [1, 2, 3, 4, 5]).
if value 6 (new, not defined in source or schema yml) is not in your list of accepted values, that SQL query will return rows. In dbt, if a test query returns any rows, the test is considered a failure.

3. 
SELECT COUNT(*)
FROM {{ ref('fct_monthly_zone_revenue') }};

12,184

4. 
SELECT pickup_zone
FROM `lydia-zoomcamp.dbt_prod.fct_monthly_zone_revenue` 
  WHERE service_type = 'Green' 
  AND EXTRACT(YEAR FROM revenue_month) = 2020
ORDER BY revenue_monthly_total_amount DESC
LIMIT 1;

East Harlem North

5. 
SELECT COUNT(*) 
FROM `lydia-zoomcamp.dbt_prod.fct_trips`
WHERE CAST(pickup_datetime AS DATE) BETWEEN '2019-10-01' AND '2019-10-31' 
  AND service_type = 'Green';

6. 43,244,693