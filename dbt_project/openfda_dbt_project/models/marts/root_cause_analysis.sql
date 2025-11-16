WITH base AS(
SELECT
    shortage_reason_standardized AS root_cause,
    COUNT(DISTINCT generic_name) AS total_drugs,
    COUNT(*) AS total_entries
FROM
    {{ ref('lifecycle_tracking_stg') }}
GROUP BY
    shortage_reason_standardized
),

ranked AS(
    SELECT
        root_cause,
        total_drugs,
        total_entries,
        ROUND(total_entries / SUM(total_entries) OVER (), 4) AS pct_of_shortages,
        RANK() OVER (ORDER BY total_entries DESC) AS shortage_rank
    FROM base
)

SELECT
    root_cause,
    total_drugs,
    total_entries,
    pct_of_shortages,
    shortage_rank,
    CASE
        WHEN pct_of_shortages >= 0.2 THEN 'High Impact'
        WHEN pct_of_shortages >= 0.1 THEN 'Moderate Impact'
        ELSE 'Low Impact'
    END AS impact_level
FROM ranked