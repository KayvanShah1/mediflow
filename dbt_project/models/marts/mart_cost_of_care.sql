{{ config(materialized='view') }}

SELECT
  d.patient_id,
  d.provider_id,
  DATE_TRUNC('month', service_date) AS month,
  SUM(allowed_amount) AS total_allowed,
  SUM(paid_amount)    AS total_paid
FROM {{ ref('fact_claim') }} d
GROUP BY 1,2,3;
