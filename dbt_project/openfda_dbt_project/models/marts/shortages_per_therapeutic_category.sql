SELECT
    therapeutic_cat,
    COUNT(DISTINCT generic_name) as total_drugs
FROM
    {{ ref("drug_identification_stg") }}
GROUP BY
    therapeutic_cat
