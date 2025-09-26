{{ config(materialized='view') }}

-- Optional: If you have a patients file/table. For demo we derive patients from claims.
SELECT DISTINCT
  patient_id,
  NULL::date as dob,
  NULL::varchar as gender,
  NULL::varchar as zip3
FROM {{ ref('stg_claims') }}
WHERE patient_id IS NOT NULL;
