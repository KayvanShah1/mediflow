{{ config(materialized='table') }}

SELECT
  c.claim_id,
  c.patient_id,
  c.provider_id,
  c.service_date,
  c.allowed_amount,
  c.paid_amount,
  c.drg_code
FROM {{ ref('stg_claims') }} c;
