SELECT
    package_ndc,
    generic_name,
    shortage_reason_standardized,
    status,
    updated_type,
    initial_posting_date,
    update_date,
    change_date,
    discontinued_date,
    resolved_note
FROM
    {{ ref('drugs_shortages_raw_stg') }}