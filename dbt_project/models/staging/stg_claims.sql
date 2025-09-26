{{ config(materialized='view') }}

WITH src AS (
  SELECT
    try_to_number(claim_id)::varchar as claim_id,
    patient_id,
    provider_id,
    to_date(service_date) as service_date,
    try_to_number(allowed_amount) as allowed_amount,
    try_to_number(paid_amount) as paid_amount,
    drg_code
  FROM {{ source('raw', 'claims') }}
)
SELECT * FROM src;
