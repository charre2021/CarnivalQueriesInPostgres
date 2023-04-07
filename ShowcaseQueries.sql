-- Active: 1679016388513@@127.0.0.1@5432@carnival@public
DROP VIEW IF EXISTS dealership_performance_by_year;

CREATE OR REPLACE VIEW dealership_performance_by_year AS
WITH cte_time_between_sales AS (
     SELECT
          dealership_id,
          DATE_TRUNC('year', purchase_date) AS year_ending,
          purchase_date - LAG(purchase_date,1) OVER(PARTITION BY dealership_id ORDER BY purchase_date) AS time_between_sales
     FROM sales
),
cte_avg_time_between_sales AS (
     SELECT
          dealership_id,
          year_ending,
          ROUND(AVG(time_between_sales),2) AS avg_time_between_sales
     FROM cte_time_between_sales
     GROUP BY dealership_id, year_ending
     ORDER BY dealership_id, year_ending
),
cte_totals AS (
     SELECT
          dealership_id,
          DATE_TRUNC('year', purchase_date) AS year_ending,
          ROUND(SUM(price),2) AS total_sales,
          ROUND(SUM(price - deposit),2) AS total_debt
     FROM sales
     GROUP BY dealership_id, year_ending
     ORDER BY dealership_id, year_ending
),
cte_makes_and_models AS (
     SELECT 
          dealership_id,
          DATE_TRUNC('year', purchase_date) AS year_ending,
          make || ' ' || model AS make_and_model,
          ROUND(SUM(price),2) AS total_revenue
     FROM sales
     INNER JOIN vehicles
     ON sales.vehicle_id = vehicles.vehicle_id
     INNER JOIN vehicletypes
     ON vehicles.vehicle_type_id = vehicletypes.vehicle_type_id
     INNER JOIN vehiclemakes
     ON vehiclemakes.vehicle_make_id = vehicletypes.vehicle_make_id
     INNER JOIN vehiclemodels
     ON vehiclemodels.vehicle_model_id = vehicletypes.vehicle_model_id
     GROUP BY dealership_id, year_ending, make_and_model
     ORDER BY dealership_id, year_ending, make_and_model
),
cte_makes_and_models_by_rank AS (
     SELECT 
          dealership_id,
          year_ending,
          make_and_model,
          total_revenue,
          RANK() OVER(PARTITION BY dealership_id, year_ending ORDER BY dealership_id, year_ending, total_revenue DESC) AS best_seller_by_total_revenue
     FROM cte_makes_and_models
),
employee_cte AS (
     SELECT
          dealership_id,
          DATE_TRUNC('year', purchase_date) AS year_ending,
          employees.employee_id,
          COALESCE(ROUND(SUM(price),2),0.00) AS total_sales
     FROM employees
     LEFT JOIN sales
     ON employees.employee_id = sales.employee_id
     GROUP BY dealership_id, year_ending, employees.employee_id
     ORDER BY dealership_id, year_ending, employees.employee_id
),
employee_rank_cte AS (
     SELECT
          dealership_id,
          year_ending,
          employee_id,
          total_sales,
          RANK() OVER(PARTITION BY dealership_id, year_ending ORDER BY dealership_id, year_ending, total_sales DESC) AS sales_rank
     FROM employee_cte
),
employee_rank_cte_one AS (
     SELECT
          dealership_id,
          year_ending,
          employee_id AS top_employee_by_total_sales
     FROM employee_rank_cte
     WHERE sales_rank = 1
),
cte_makes_and_models_total_revenue_rank AS (
     SELECT 
          dealership_id,
          year_ending,
          make_and_model AS best_seller_by_total_revenue
     FROM cte_makes_and_models_by_rank
     WHERE best_seller_by_total_revenue = 1
),
combined_cte AS (
     SELECT
          cte_totals.dealership_id,
          cte_totals.year_ending,
          total_sales,
          total_debt,
          avg_time_between_sales,
          top_employee_by_total_sales,
          best_seller_by_total_revenue
     FROM cte_totals
     LEFT JOIN cte_avg_time_between_sales
     ON cte_totals.dealership_id = cte_avg_time_between_sales.dealership_id
     AND cte_totals.year_ending = cte_avg_time_between_sales.year_ending
     LEFT JOIN employee_rank_cte_one
     ON cte_totals.dealership_id = employee_rank_cte_one.dealership_id
     AND cte_totals.year_ending = employee_rank_cte_one.year_ending
     LEFT JOIN cte_makes_and_models_total_revenue_rank
     ON cte_totals.dealership_id = cte_makes_and_models_total_revenue_rank.dealership_id
     AND cte_totals.year_ending = cte_makes_and_models_total_revenue_rank.year_ending
     ORDER BY cte_totals.dealership_id, cte_totals.year_ending
)

SELECT
     business_name,
     EXTRACT('Year' FROM year_ending) AS calendar_year,
     TO_CHAR(total_sales,'$FM9,999,999.00') AS total_revenue,
     COALESCE(ROUND((total_sales - LAG(total_sales,1) OVER(PARTITION BY combined_cte.dealership_id ORDER BY combined_cte.dealership_id)) / LAG(total_sales) OVER(PARTITION BY combined_cte.dealership_id ORDER BY combined_cte.dealership_id) * 100.0,2) || '%','N/A') AS total_revenue_yoy_percentage_change,
     TO_CHAR(total_debt,'$FM9,999,999.00') AS total_debt,
     COALESCE(ROUND((total_debt - LAG(total_debt,1) OVER(PARTITION BY combined_cte.dealership_id ORDER BY combined_cte.dealership_id)) / LAG(total_debt) OVER(PARTITION BY combined_cte.dealership_id ORDER BY combined_cte.dealership_id) * 100.0,2) || '%','N/A') AS total_debt_yoy_percentage_change,
     avg_time_between_sales AS avg_days_between_sales,
     COALESCE(ROUND((avg_time_between_sales - LAG(avg_time_between_sales,1) OVER(PARTITION BY combined_cte.dealership_id ORDER BY combined_cte.dealership_id)) / LAG(avg_time_between_sales) OVER(PARTITION BY combined_cte.dealership_id ORDER BY combined_cte.dealership_id) * 100.0,2) || '%','N/A') AS avg_days_between_sales_yoy_percentage_change,
     employees.first_name || ' ' || employees.last_name AS top_employee_name,
     best_seller_by_total_revenue AS best_vehicle_by_total_revenue
FROM combined_cte
INNER JOIN dealerships
ON combined_cte.dealership_id = dealerships.dealership_id
LEFT JOIN employees
ON combined_cte.top_employee_by_total_sales = employees.employee_id;

SELECT * FROM dealership_performance_by_year;

SELECT
     t1.business_name,
     t1.year_ending,
     total_revenue,
     avg_revenue
FROM (
     SELECT
          business_name,
          EXTRACT('Year' FROM purchase_date) AS year_ending,
          TO_CHAR(SUM(price),'$FM9,999,999.00') AS total_revenue
     FROM sales
     INNER JOIN dealerships
     ON sales.dealership_id = dealerships.dealership_id
     GROUP BY business_name, year_ending
     ORDER BY business_name, year_ending ) AS t1
INNER JOIN (
     SELECT
          business_name,
          EXTRACT('Year' FROM purchase_date) AS year_ending,
          TO_CHAR(AVG(price),'$FM9,999,999.00') AS avg_revenue
     FROM sales
     INNER JOIN dealerships
     ON sales.dealership_id = dealerships.dealership_id
     GROUP BY business_name, year_ending
     ORDER BY business_name, year_ending ) AS t2
ON t1.business_name = t2.business_name
AND t1.year_ending = t2.year_ending;

-- Version of Jack's Suggestion

WITH example_cte AS (
SELECT
     business_name,
     EXTRACT('Year' from purchase_date) AS year_ending,
     purchase_date,
     RANK() OVER(PARTITION BY sales.dealership_id ORDER BY purchase_date) AS date_rank
FROM sales
INNER JOIN dealerships
ON sales.dealership_id = dealerships.dealership_id
),
example_cte_two AS (
     SELECT 
          business_name,
          year_ending,
          purchase_date,
          date_rank - 1 AS date_rank
     FROM example_cte
)

SELECT
     ec1.business_name,
     ec1.year_ending,
     ROUND(AVG(ec2.purchase_date - ec1.purchase_date),2) AS avg_days_between_sales
FROM example_cte AS ec1
INNER JOIN example_cte_two AS ec2
ON ec1.date_rank = ec2.date_rank
AND ec1.business_name = ec2.business_name
AND ec1.year_ending = ec2.year_ending
GROUP BY ec1.business_name, ec1.year_ending
ORDER BY ec1.business_name, ec1.year_ending;

-- Original

WITH cte_time_between_sales AS (
     SELECT
          business_name,
          EXTRACT('Year' FROM purchase_date) AS year_ending,
          purchase_date - LAG(purchase_date,1) OVER(PARTITION BY business_name ORDER BY purchase_date) AS time_between_sales
     FROM sales
     INNER JOIN dealerships
     ON sales.dealership_id = dealerships.dealership_id
)
SELECT
     business_name,
     year_ending,
     ROUND(AVG(time_between_sales),2) AS avg_time_between_sales
FROM cte_time_between_sales
WHERE time_between_sales IS NOT NULL
GROUP BY business_name, year_ending
ORDER BY business_name, year_ending;
