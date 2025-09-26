{{ config(materialized='table') }}

SELECT
  patient_id,
  dob,
  gender,
  zip3
FROM {{ ref('stg_patients') }};
