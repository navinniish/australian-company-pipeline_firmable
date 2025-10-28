{{ config(
    materialized='table',
    indexes=[
        {'columns': ['company_key'], 'unique': true},
        {'columns': ['abn']},
        {'columns': ['normalized_name']},
        {'columns': ['industry_category']},
        {'columns': ['address_state']},
        {'columns': ['quality_tier']}
    ]
) }}

/*
    Company dimension table for analytics and reporting.
    This is the primary dimensional table for company data.
*/

with companies_with_contacts as (
    select 
        c.*,
        
        -- Email contacts
        listagg(
            case when cc.contact_type = 'email' then cc.contact_value end, 
            '; '
        ) within group (order by cc.contact_value) as email_addresses,
        
        -- Phone contacts  
        listagg(
            case when cc.contact_type = 'phone' then cc.contact_value end, 
            '; '
        ) within group (order by cc.contact_value) as phone_numbers,
        
        -- Social media
        listagg(
            case when cc.contact_type = 'linkedin' then cc.contact_value end, 
            '; '
        ) within group (order by cc.contact_value) as linkedin_profiles,
        
        -- Contact counts
        count(case when cc.contact_type = 'email' then 1 end) as email_count,
        count(case when cc.contact_type = 'phone' then 1 end) as phone_count,
        count(case when cc.contact_type in ('linkedin', 'facebook', 'twitter', 'instagram') then 1 end) as social_media_count
        
    from {{ ref('stg_companies') }} c
    left join {{ source('core', 'company_contacts') }} cc 
        on c.company_id = cc.company_id
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31
),

companies_with_names as (
    select 
        cwc.*,
        
        -- Alternative names
        listagg(
            case when cn.name_type = 'trading' then cn.name end, 
            '; '
        ) within group (order by cn.name) as trading_names,
        
        listagg(
            case when cn.name_type = 'business' then cn.name end, 
            '; '
        ) within group (order by cn.name) as business_names,
        
        count(distinct cn.name) as alternative_name_count
        
    from companies_with_contacts cwc
    left join {{ source('core', 'company_names') }} cn 
        on cwc.company_id = cn.company_id
        and cn.is_primary = false
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37
),

companies_with_industry_details as (
    select 
        cwn.*,
        
        -- Industry classifications
        listagg(distinct ic.code, '; ') within group (order by ic.code) as industry_codes,
        avg(ic.confidence_score) as avg_industry_confidence,
        count(distinct ic.classification_system) as classification_systems_count
        
    from companies_with_names cwn
    left join {{ source('core', 'industry_classifications') }} ic 
        on cwn.company_id = ic.company_id
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41
),

final as (
    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['company_id']) }} as company_key,
        
        -- Natural keys
        company_id,
        abn,
        
        -- Company identification
        company_name,
        normalized_name,
        trading_names,
        business_names,
        alternative_name_count,
        
        -- Web presence
        website_url,
        has_website,
        
        -- Business details
        entity_type,
        entity_status,
        industry,
        industry_category,
        industry_codes,
        avg_industry_confidence,
        
        -- Location
        address_line_1,
        address_line_2,
        address_suburb,
        address_state,
        address_postcode,
        address_completeness_score,
        
        -- Contact information
        email_addresses,
        phone_numbers,
        linkedin_profiles,
        email_count,
        phone_count,
        social_media_count,
        has_email_contact,
        has_phone_contact,
        
        -- Dates
        start_date,
        registration_date,
        
        -- Status and compliance
        gst_registered,
        dgr_endorsed,
        is_active,
        has_valid_abn,
        
        -- Data quality
        data_quality_score,
        quality_tier,
        data_source,
        
        -- Calculated business metrics
        case 
            when start_date is not null 
            then datediff('year', start_date, current_date())
            else null
        end as business_age_years,
        
        case 
            when has_website and has_email_contact and has_phone_contact then 'high'
            when (has_website and has_email_contact) or (has_website and has_phone_contact) then 'medium'
            when has_website or has_email_contact or has_phone_contact then 'low'
            else 'none'
        end as digital_presence_level,
        
        case
            when abn is not null and gst_registered = true and is_active = true then 'fully_compliant'
            when abn is not null and is_active = true then 'basic_compliant'
            when abn is not null then 'registered_inactive'
            else 'unregistered'
        end as compliance_status,
        
        -- Audit fields
        created_at,
        updated_at,
        current_timestamp() as dbt_updated_at
        
    from companies_with_industry_details
    where 
        company_name is not null
        and length(trim(company_name)) >= 2
)

select * from final