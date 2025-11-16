
SELECT
    INITCAP(TRIM(update_type)) AS updated_type,
      TRIM(
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(
              INITCAP(TRIM(GENERIC_NAME)),
              '\\b(' ||
                'Injection|Injectable|Tablet|Capsule|Cream|Lotion|Gel|Patch|Powder|' ||
                'Drops|Solution|Suspension|Syrup|Irrigation|Ointment|Topical|' ||
                'Concentrate|Transdermal|Delayed Release|Extended Release|' ||
                'Film Coated|Metered|For Solution|For Injection|Kit' ||
              ')\\b',
              '',
              1, 0, 'i'
            ),

            '[,;]+\\s*$',
            '',
            1, 0, 'i'
          ),

          '\\s*;\\s*',
          '; ',
          1, 0, 'i'
        ),

        '\\s{2,}',
        ' '
      )
    ) AS generic_name,
    TRY_TO_DATE(initial_posting_date) AS initial_posting_date,
    trim(package_ndc) AS package_ndc,
    contact_info,
    TRIM(
        CASE
            WHEN availability IS NULL OR TRIM(availability) = '' THEN 'Unknown'
            WHEN LOWER(availability) LIKE '%pending%' THEN 'Pending'
            WHEN LOWER(availability) LIKE '%unavailable%' THEN 'Unavailable'
            WHEN LOWER(availability) LIKE '%limited%' THEN 'Limited'
            WHEN LOWER(availability) LIKE '%available%' THEN 'Available'
            ELSE 'Unknown'
        END
    ) AS availability_status,
    related_info,
    TRY_TO_DATE(update_date) as update_date,
        NULLIF(
      TRIM(
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(
              INITCAP(TRIM(therapeutic_category)),
              '\\[|\\]', '', 1, 0, 'e'
            ),
            '''', '', 1, 0, 'e'
          ),
          '\\s*,\\s*', ', ', 1, 0, 'e'
        )
      ),
      ''
    ) AS therapeutic_category,
    TRIM(
    REGEXP_REPLACE(
        REGEXP_REPLACE(
            INITCAP(TRIM(dosage_form)),
            '\\s*;\\s*', ' / ', 1, 0, 'e'     
        ),
        '\\s*,\\s*', ' ', 1, 0, 'e'           
    )
) AS dosage_form,
    CASE
        WHEN dosage_form IS NULL THEN 'Unknown'

        WHEN dosage_form ILIKE '%tablet% chewable%' THEN 'Chewable Tablet'
        WHEN dosage_form ILIKE '%chewable% tablet%' THEN 'Chewable Tablet'
        WHEN dosage_form ILIKE '%tablet% extended release%' THEN 'Extended Release Tablet'
        WHEN dosage_form ILIKE '%film% extended release%' THEN 'Extended Release Film'
        WHEN dosage_form ILIKE '%tablet%' THEN 'Tablet'

        WHEN dosage_form ILIKE '%capsule% extended release%' THEN 'Extended Release Capsule'
        WHEN dosage_form ILIKE '%capsule%' THEN 'Capsule'

        WHEN dosage_form ILIKE '%injectable suspension%' THEN 'Injection'
        WHEN dosage_form ILIKE '%injection%' THEN 'Injection'

        WHEN dosage_form ILIKE '%ophthalmic solution%' THEN 'Ophthalmic Solution'
        WHEN dosage_form ILIKE '%ophthalmic ointment%' THEN 'Ophthalmic Ointment'

        WHEN dosage_form ILIKE '%oral powder%' THEN 'Oral Powder'
        WHEN dosage_form ILIKE '%oral solution%' THEN 'Oral Solution'
        WHEN dosage_form ILIKE '%oral suspension%' THEN 'Oral Suspension'

        WHEN dosage_form ILIKE '%topical powder%' THEN 'Topical Powder'
        WHEN dosage_form ILIKE '%topical gel%' THEN 'Topical Gel'
        WHEN dosage_form ILIKE '%topical ointment%' THEN 'Topical Ointment'

        WHEN dosage_form ILIKE '%nasal spray%' THEN 'Nasal Spray'
        WHEN dosage_form ILIKE '%spray%' THEN 'Spray'

        WHEN dosage_form ILIKE '%patch%' THEN 'Patch'
        WHEN dosage_form ILIKE '%transdermal system%' THEN 'Transdermal System'

        WHEN dosage_form ILIKE '%powder for solution%' THEN 'Powder For Solution'
        WHEN dosage_form ILIKE '%powder%' THEN 'Powder'

        WHEN dosage_form ILIKE '%solution drops%' THEN 'Solution'
        WHEN dosage_form ILIKE '%solution%' THEN 'Solution'

        WHEN dosage_form ILIKE '%gel metered%' THEN 'Gel'
        WHEN dosage_form ILIKE '%gel%' THEN 'Gel'

        WHEN dosage_form ILIKE '%cream%' THEN 'Cream'
        WHEN dosage_form ILIKE '%lotion%' THEN 'Lotion'

        WHEN dosage_form ILIKE '%granule%' THEN 'Granule'
        WHEN dosage_form ILIKE '%insert%' THEN 'Insert'
        WHEN dosage_form ILIKE '%ring%' THEN 'Ring'

        WHEN dosage_form ILIKE '%irrigation%' THEN 'Irrigation'
        WHEN dosage_form ILIKE '%irrigant%' THEN 'Irrigation'

        ELSE dosage_form
    END AS dosage_form_standardized,
    presentation,
    TRIM(company_name) as manufacturer_name,

    CASE
      WHEN LOWER(shortage_reason) LIKE '%active ingredient%' THEN 'API Shortage'

      WHEN LOWER(shortage_reason) LIKE '%gmp%' 
         OR LOWER(shortage_reason) LIKE '%good manufacturing%'
         OR LOWER(shortage_reason) LIKE '%regulatory%'
      THEN 'Manufacturing Issue'

      WHEN LOWER(shortage_reason) LIKE '%demand%' THEN 'Demand Surge'

      WHEN LOWER(shortage_reason) LIKE '%shipping%'
         OR LOWER(shortage_reason) LIKE '%supply%'
      THEN 'Supply Chain Issue'

      WHEN LOWER(shortage_reason) LIKE '%discontinuation%' THEN 'Product Discontinued'

      WHEN shortage_reason IS NULL
         OR shortage_reason = ''
         OR LOWER(shortage_reason) = 'unknown'
         OR LOWER(shortage_reason) = 'null'
      THEN
        CASE
            -- Status-driven enrichment
            WHEN status = 'To Be Discontinued' THEN 'Product Discontinued'
            WHEN status = 'Resolved' THEN 'Resolved Without Cause'
            WHEN status = 'Current' THEN 'Under Investigation'

            -- Supply Chain Indicators
            WHEN LOWER(availability) LIKE '%limited%'
              OR LOWER(availability) LIKE '%replenishment%'
              OR LOWER(related_info) LIKE '%allocation%'
              OR LOWER(update_type) LIKE '%allocation%'
            THEN 'Supply Chain Issue'

            -- Manufacturing Indicators
            WHEN LOWER(related_info) LIKE '%manufactur%'
              OR LOWER(related_info) LIKE '%quality%'
              OR LOWER(related_info) LIKE '%gmp%'
              OR LOWER(update_type) LIKE '%manufactur%'
            THEN 'Manufacturing Issue'

            -- API Shortage Indicators
            WHEN LOWER(related_info) LIKE '%api%'
              OR LOWER(related_info) LIKE '%active ingredient%'
            THEN 'API Shortage'

            -- Demand Surge Indicators
            WHEN LOWER(related_info) LIKE '%demand%'
              OR LOWER(update_type) LIKE '%demand%'
            THEN 'Demand Surge'

            -- Nothing matched → still unknown
            ELSE 'Unattributed Shortage'
        END
      ELSE 'Unattributed Shortage'
  END AS shortage_reason_standardized,
    INITCAP(TRIM(status)) AS status,
    openfda_application_number,
    INITCAP(
          CASE
            WHEN openfda_brand_name IS NULL OR TRIM(openfda_brand_name) = '' THEN 'Unknown'
            WHEN LOWER(openfda_brand_name) IN ('na', 'n/a', 'none') THEN 'Unknown'
            ELSE TRIM(
              REGEXP_REPLACE(
                  REGEXP_REPLACE(
                      REGEXP_REPLACE(
                          openfda_brand_name,
                          '\\[|\\]', '',
                          1, 0, 'e'
                      ),
                      '''', '',
                      1, 0, 'e'
                  ),
                  '\\s*,\\s*', ', ',
                  1, 0, 'e'
                )
              )
          END) AS brand_name,
    CASE
        WHEN LOWER(brand_name) IN (
            'sterile water',
            'sodium chloride',
            'dextrose',
            'lactated ringers',
            'bacteriostatic water',
            'powder for solution',
            'solution'
        ) THEN 'Unbranded'
        WHEN brand_name = 'Unknown' THEN 'Unknown'
        ELSE brand_name
    END AS brand_name_standardized,
    openfda_generic_name,
    openfda_manufacturer_name,
    openfda_product_ndc,
    TRIM(openfda_product_type) AS product_type,
    TRIM(openfda_route) as admin_route,
    TRIM(openfda_substance_name) as active_substance,
    openfda_rxcui,
    openfda_spl_id,
    openfda_spl_set_id,
    TRIM(openfda_product_ndc) AS product_ndc,
    openfda_unii,
    openfda_nui,
    TRIM(openfda_pharm_class_epc) AS pharm_class_epc,
    TRIM(openfda_pharm_class_pe) AS pharm_class_pe,
    TRY_TO_DATE(discontinued_date) AS discontinued_date,
    related_info_link,
    TRIM(openfda_pharm_class_cs) AS pharm_class_cs,
    TRY_TO_DATE(change_date) AS change_date,
    TRIM(openfda_pharm_class_moa) AS pharm_class_moa,
    TRIM(resolved_note) AS resolved_note
FROM
    {{ source('masterlanding', 'master_view') }}

