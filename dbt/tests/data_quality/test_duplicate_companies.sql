/*
    Test to identify potential duplicate companies based on normalized names
    and ABNs. This helps identify issues with the entity matching process.
*/

with duplicate_abns as (
    select 
        abn,
        count(*) as duplicate_count,
        array_agg(company_name) as company_names
    from {{ ref('dim_companies') }}
    where abn is not null
    group by abn
    having count(*) > 1
),

duplicate_normalized_names as (
    select 
        normalized_name,
        count(*) as duplicate_count,
        array_agg(distinct company_name) as company_names,
        array_agg(distinct abn) as abns
    from {{ ref('dim_companies') }}
    where 
        normalized_name is not null 
        and length(trim(normalized_name)) > 3  -- Avoid flagging very generic names
    group by normalized_name
    having count(*) > 1
)

-- ABN duplicates (should never happen)
select
    'Duplicate ABN' as issue_type,
    abn as identifier,
    duplicate_count,
    company_names
from duplicate_abns

union all

-- Normalized name duplicates (potential entity matching issues)
select
    'Duplicate Normalized Name' as issue_type,
    normalized_name as identifier,
    duplicate_count,
    company_names
from duplicate_normalized_names
where 
    -- Only flag as issue if they have different ABNs (suggesting they're different companies)
    array_length(abns, 1) > 1
    and not (null = any(abns))  -- Both companies have ABNs but they're different