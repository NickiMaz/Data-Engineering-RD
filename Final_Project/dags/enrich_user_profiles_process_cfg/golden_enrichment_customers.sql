MERGE INTO `rd-final-project.golden_layer.golden_customers_enriched` AS customers
USING `rd-final-project.silver_layer.silver_user_profiles` AS user_profiles

ON customers.email = user_profiles.email

WHEN MATCHED
  THEN
    UPDATE
      SET customers.first_name = user_profiles.first_name,
          customers.last_name = user_profiles.last_name,
          customers.state = user_profiles.state,
          customers.phone_number = user_profiles.phone_number,
          customers.birth_date = user_profiles.birth_date