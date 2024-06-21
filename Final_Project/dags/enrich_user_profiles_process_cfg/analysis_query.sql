SELECT state, COUNT(*) AS quantity
FROM `rd-final-project.silver_layer.silver_sales` AS sales
  LEFT JOIN `rd-final-project.golden_layer.golden_customers_enriched` AS customers
    ON sales.client_id = customers.client_id

WHERE sales.purchase_date BETWEEN '2022-09-01' AND '2022-09-10'
  AND DATE_DIFF(CURRENT_DATE(), customers.birth_date, YEAR) >= 20
  AND DATE_DIFF(CURRENT_DATE(), customers.birth_date, YEAR) <= 30
  AND sales.product_name = 'TV'

GROUP BY customers.state
ORDER BY quantity DESC
LIMIT 1;
