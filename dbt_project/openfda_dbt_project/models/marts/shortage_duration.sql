SELECT  
    generic_name,
    initial_posting_date,
    discontinued_date,
    DATEDIFF(
        'day',
        initial_posting_date,
        COALESCE(discontinued_date, CURRENT_DATE)
    ) AS duration_days
FROM
    {{ ref('lifecycle_tracking_stg') }}

