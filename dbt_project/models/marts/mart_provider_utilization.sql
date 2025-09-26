{{ config(materialized='view') }}

SELECT
  f.provider_id,
  p.specialty,
  COUNT(*) AS claim_count,
  SUM(f.allowed_amount) AS total_allowed
FROM {{ ref('fact_claim') }} f
LEFT JOIN {{ ref('dim_provider') }} p
  ON f.provider_id = p.provider_id
GROUP BY 1,2;
