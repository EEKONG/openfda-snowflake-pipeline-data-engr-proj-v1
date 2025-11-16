SELECT
    package_ndc,
    generic_name,
    shortage_reason_standardized,
    availability_status,
FROM
    {{ ref('drugs_shortages_raw_stg') }}