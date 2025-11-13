SELECT
    package_ndc,
    generic_name,
    brand_name,
    manufacturer_name,
    dosage_form_standardized,
    therapeutic_category
FROM
    {{ ref('drugs_shortages_raw_stg') }}