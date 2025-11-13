SELECT
    package_ndc,
    generic_name,
    product_ndc,
    product_type,
    admin_route,
    active_substance,
    pharm_class_epc,
    pharm_class_moa,
    pharm_class_pe,
    pharm_class_cs
FROM
    {{ ref('drugs_shortages_raw_stg') }}