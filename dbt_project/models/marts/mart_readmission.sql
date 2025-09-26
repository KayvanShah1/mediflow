{{ config(materialized='view') }}

-- Naive proxy: readmission if multiple claims within 30 days (demo only)
WITH claims AS (
  SELECT patient_id, service_date,
         LEAD(service_date) OVER (PARTITION BY patient_id ORDER BY service_date) AS next_service_date
  FROM {{ ref('fact_claim') }}
)
SELECT
  patient_id,
  COUNT_IF(DATEDIFF('day', service_date, next_service_date) <= 30) AS readmit_30d_events
FROM claims
GROUP BY 1;
