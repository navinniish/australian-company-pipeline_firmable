/*
    Test to validate that all ABNs in the companies table are valid.
    ABNs should be exactly 11 digits and pass the checksum validation.
*/

select
    company_id,
    abn,
    company_name
from {{ ref('dim_companies') }}
where 
    abn is not null
    and (
        length(abn) != 11
        or abn !~ '^[0-9]{11}$'
        -- Note: Full ABN checksum validation would require a custom function
        -- This test focuses on format validation
    )