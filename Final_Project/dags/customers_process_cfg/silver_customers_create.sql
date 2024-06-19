WITH dublicate_highlight_customers AS (
  
  SELECT
    ROW_NUMBER() OVER (PARTITION BY 
      Id, FirstName, LastName,
      Email, RegistrationDate, State
    ) AS count_dub,
    
    Id AS client_id, FirstName AS first_name, LastName AS last_name, 
    Email AS email, RegistrationDate AS registration_date, State AS state
    
FROM `rd-final-project.bronze_layer.bronze_customers`
)

SELECT 
  CAST(client_id AS INT64) AS client_id, 
  INITCAP(CAST(first_name AS STRING)) AS first_name, 
  INITCAP(CAST(last_name AS STRING)) AS last_name,
  CAST(email AS STRING) AS email, 
  CAST(registration_date AS DATE) AS registration_date, 
  CAST(state AS STRING) AS state

FROM dublicate_highlight_customers
WHERE count_dub = 1;