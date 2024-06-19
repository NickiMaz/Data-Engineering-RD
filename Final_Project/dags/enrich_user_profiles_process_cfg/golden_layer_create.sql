SELECT 
       client_id,
       first_name,
       last_name,
       state,
       registration_date,
       email,
       CAST(NULL AS DATE) AS birth_date,
       CAST(NULL AS STRING) AS phone_number

FROM `rd-final-project.silver_layer.silver_customers`