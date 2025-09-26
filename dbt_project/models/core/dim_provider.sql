{{ config(materialized='table') }}

SELECT
  provider_id,
  npi,
  specialty,
  state
FROM {{ ref('stg_providers') }};
