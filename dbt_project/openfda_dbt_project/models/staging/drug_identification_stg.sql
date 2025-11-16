WITH base AS (
    SELECT
        package_ndc,
        generic_name,
        brand_name,
        manufacturer_name,
        dosage_form_standardized,
        therapeutic_category
    FROM {{ ref('drugs_shortages_raw_stg') }}
),

-- Split standardized categories into rows
split AS (
    SELECT
        package_ndc,
        generic_name,
        brand_name,
        manufacturer_name,
        dosage_form_standardized,
        TRIM(value) AS category_raw
    FROM base,
    LATERAL SPLIT_TO_TABLE(therapeutic_category, ',')
),

-- Final cleaning and normalization
cleaned AS (
    SELECT DISTINCT
        package_ndc,
        generic_name,
        brand_name,
        manufacturer_name,
        dosage_form_standardized,
        CASE
            WHEN category_raw IS NULL OR category_raw = '' THEN 'Unknown'
            WHEN LOWER(category_raw) = 'other' THEN 'Unknown'
            WHEN LOWER(category_raw) = 'unknown' THEN 'Unknown'
            ELSE INITCAP(TRIM(category_raw))
        END AS category
    FROM split
)

SELECT
    package_ndc,
    generic_name,
    brand_name,
    manufacturer_name,
    dosage_form_standardized,
    category as therapeutic_cat
FROM cleaned
WHERE category IS NOT NULL
  AND category <> ''
  AND category <> 'Unknown'