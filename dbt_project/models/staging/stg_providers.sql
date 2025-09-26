{{ config(materialized='view') }}

SELECT
  provider_id,
  npi,
  specialty,
  state
FROM {{ source('raw', 'providers') }};
