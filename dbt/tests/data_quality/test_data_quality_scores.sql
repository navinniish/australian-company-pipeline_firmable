/*
    Test to ensure all data quality scores are within valid range (0.0 to 1.0)
    and that companies with very low scores are flagged for review.
*/

select
    company_id,
    company_name,
    data_quality_score,
    'Data quality score out of range' as error_reason
from {{ ref('dim_companies') }}
where 
    data_quality_score is not null
    and (
        data_quality_score < 0.0 
        or data_quality_score > 1.0
    )

union all

select
    company_id,
    company_name,
    data_quality_score,
    'Suspiciously low data quality score' as error_reason
from {{ ref('dim_companies') }}
where 
    data_quality_score is not null
    and data_quality_score < 0.1
    and has_valid_abn = true  -- Should have higher quality if ABN is present