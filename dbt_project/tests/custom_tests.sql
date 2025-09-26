-- Example custom test: ensure paid_amount <= allowed_amount
SELECT *
FROM {{ ref('fact_claim') }}
WHERE paid_amount > allowed_amount;
