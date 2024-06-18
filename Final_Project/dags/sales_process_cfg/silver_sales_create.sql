SELECT
    CAST(`rd-final-project.bronze_layer.bronze_sales`.CustomerId AS INT64) AS client_id,
      
    CASE    
      WHEN REGEXP_CONTAINS(`rd-final-project.bronze_layer.bronze_sales`.PurchaseDate, r'\d{4}-\d{2}-\d{1,2}')
        THEN CAST(`rd-final-project.bronze_layer.bronze_sales`.PurchaseDate AS DATE FORMAT 'YYYY-MM-DD')

      WHEN REGEXP_CONTAINS(`rd-final-project.bronze_layer.bronze_sales`.PurchaseDate, r'\d{4}/\d{2}/\d{2}')
        THEN CAST(`rd-final-project.bronze_layer.bronze_sales`.PurchaseDate AS DATE FORMAT 'YYYY/MM/DD')
          
      WHEN REGEXP_CONTAINS(`rd-final-project.bronze_layer.bronze_sales`.PurchaseDate, r'\d{4}-\d{2}-\d{1}')
        THEN CAST(`rd-final-project.bronze_layer.bronze_sales`.PurchaseDate AS DATE FORMAT 'YYYY-MM-DD')

      WHEN REGEXP_CONTAINS(`rd-final-project.bronze_layer.bronze_sales`.PurchaseDate, r'\d{4}-[A-Za-z]{3}-\d{1}')
        THEN CAST(`rd-final-project.bronze_layer.bronze_sales`.PurchaseDate AS DATE FORMAT 'YYYY-MON-DD')
          
      ELSE DATE '1900-01-01'      
    END AS purchase_date,
      
    CAST(`rd-final-project.bronze_layer.bronze_sales`.Product AS STRING) AS product_name,
    CAST(REGEXP_REPLACE(`rd-final-project.bronze_layer.bronze_sales`.Price, r'[$|USD]', '') AS NUMERIC) AS price
    
FROM `rd-final-project.bronze_layer.bronze_sales`;