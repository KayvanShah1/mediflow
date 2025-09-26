{{ config(materialized='table') }}

-- Placeholder fact derived from claims; adjust if you have an ADMISSIONS source
SELECT
  claim_id as admission_id,
  patient_id,
  provider_id,
  service_date as admit_date,
  NULL::date as discharge_date,
  NULL::varchar as admission_type,
  FALSE as readmitted_30d
FROM {{ ref('stg_claims') }};
