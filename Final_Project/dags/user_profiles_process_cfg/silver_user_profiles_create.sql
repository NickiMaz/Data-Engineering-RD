
WITH users_profilies_stage AS(
  SELECT
    INITCAP(CAST(state AS STRING)) AS state,
    CAST(birth_date AS DATE) AS birth_date,
    INITCAP(CAST(SPLIT(full_name, ' ')[0] AS STRING)) AS first_name,
    INITCAP(CAST(SPLIT(full_name, ' ')[1] AS STRING)) AS last_name, 
  
    REGEXP_REPLACE(
    REGEXP_EXTRACT(phone_number, r'.?\d{3}.?\d{3}.?\d{4}'),
      r'[()\-\.]', ''
    ) AS phone_number,

    CAST(email AS STRING) AS email, 

  FROM `rd-final-project.bronze_layer.bronze_user_profiles`
)

SELECT
  state, birth_date, first_name, last_name, email,
  CONCAT(
    '+1-', SUBSTR(phone_number, 1, 3),
    '-', SUBSTR(phone_number, 4, 3),
    '-', SUBSTR(phone_number, 7, 4)
  ) AS phone_number
FROM users_profilies_stage
