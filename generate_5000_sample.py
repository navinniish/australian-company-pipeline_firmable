#!/usr/bin/env python3
"""
Generate 5000 Sample Companies CSV
Creates a realistic sample dataset of 5000 Australian companies with all enhanced features.
"""

import sys
import os
sys.path.append(os.getcwd())
import asyncio
import random
from datetime import datetime, timedelta
from src.exporters.csv_exporter import CSVExporter
from typing import List, Dict, Any

# Australian business data for realistic samples
AUSTRALIAN_STATES = ['NSW', 'VIC', 'QLD', 'WA', 'SA', 'TAS', 'NT', 'ACT']
AUSTRALIAN_CITIES = {
    'NSW': ['Sydney', 'Newcastle', 'Wollongong', 'Central Coast', 'Parramatta'],
    'VIC': ['Melbourne', 'Geelong', 'Ballarat', 'Bendigo', 'Shepparton'],
    'QLD': ['Brisbane', 'Gold Coast', 'Cairns', 'Townsville', 'Rockhampton'],
    'WA': ['Perth', 'Fremantle', 'Bunbury', 'Geraldton', 'Kalgoorlie'],
    'SA': ['Adelaide', 'Mount Gambier', 'Whyalla', 'Murray Bridge', 'Port Augusta'],
    'TAS': ['Hobart', 'Launceston', 'Devonport', 'Burnie', 'Kingston'],
    'NT': ['Darwin', 'Alice Springs', 'Katherine', 'Tennant Creek', 'Nhulunbuy'],
    'ACT': ['Canberra', 'Tuggeranong', 'Belconnen', 'Weston Creek', 'Gungahlin']
}

POSTCODE_RANGES = {
    'NSW': (1000, 2999),
    'VIC': (3000, 3999),
    'QLD': (4000, 4999),
    'WA': (6000, 6999),
    'SA': (5000, 5999),
    'TAS': (7000, 7499),
    'NT': (800, 999),
    'ACT': (200, 299)
}

INDUSTRIES = [
    'Technology', 'Construction', 'Professional Services', 'Manufacturing',
    'Retail Trade', 'Healthcare', 'Education', 'Finance', 'Real Estate',
    'Transportation', 'Agriculture', 'Mining', 'Hospitality', 'Media',
    'Energy', 'Telecommunications', 'Automotive', 'Food & Beverage'
]

COMPANY_TYPES = [
    'Software Development', 'Web Design', 'Consulting', 'Construction',
    'Retail', 'Medical Practice', 'Law Firm', 'Accounting', 'Marketing',
    'Engineering', 'Architecture', 'Restaurant', 'Manufacturing',
    'Import/Export', 'Logistics', 'Real Estate Agency', 'Insurance'
]

SOCIAL_PLATFORMS = [
    'linkedin', 'facebook', 'twitter', 'instagram', 'youtube',
    'tiktok', 'pinterest', 'github', 'snapchat', 'whatsapp_business',
    'behance', 'dribbble', 'vimeo', 'reddit', 'discord',
    'telegram', 'wechat', 'line', 'kakao'
]

def generate_abn() -> str:
    """Generate a realistic Australian Business Number."""
    # Generate 11-digit ABN (simplified for demo)
    digits = [random.randint(0, 9) for _ in range(11)]
    return ''.join(map(str, digits))

def generate_company_name() -> str:
    """Generate realistic Australian company names."""
    prefixes = ['Advanced', 'Australian', 'Premier', 'Professional', 'Elite', 'Global', 'Metro', 'Urban', 'Coastal', 'Regional']
    bases = ['Tech', 'Solutions', 'Services', 'Group', 'Systems', 'Consulting', 'Partners', 'Industries', 'Construction', 'Trading']
    suffixes = ['Pty Ltd', 'Pty Limited', 'Limited', 'Corporation', 'Group', 'Australia', 'Holdings']
    
    prefix = random.choice(prefixes) if random.random() < 0.6 else ''
    base = random.choice(bases)
    suffix = random.choice(suffixes)
    
    if prefix:
        return f"{prefix} {base} {suffix}"
    else:
        return f"{base} {suffix}"

def generate_website_url(company_name: str) -> str:
    """Generate realistic website URL."""
    if random.random() < 0.25:  # 25% chance of no website
        return None
    
    # Convert company name to domain-friendly format
    domain_name = company_name.lower().replace(' ', '').replace('pty', '').replace('ltd', '').replace('limited', '')
    domain_name = ''.join(c for c in domain_name if c.isalnum())[:15]  # Limit length
    
    tlds = ['.com.au', '.net.au', '.org.au', '.edu.au', '.gov.au']
    return f"https://{domain_name}{random.choice(tlds)}"

def generate_address(state: str, city: str) -> Dict[str, str]:
    """Generate realistic Australian address."""
    street_numbers = range(1, 999)
    street_names = ['Collins', 'Bourke', 'Elizabeth', 'King', 'Queen', 'George', 'Pitt', 'York', 'Sussex', 'Kent']
    street_types = ['Street', 'Road', 'Avenue', 'Drive', 'Lane', 'Place', 'Circuit', 'Close']
    
    postcode_min, postcode_max = POSTCODE_RANGES[state]
    postcode = str(random.randint(postcode_min, postcode_max)).zfill(4)
    
    # Occasional postcode errors for validation testing
    if random.random() < 0.05:  # 5% chance of error
        if random.random() < 0.5:
            postcode = postcode.replace('0', 'O')  # OCR error
        else:
            postcode = postcode[1:]  # Missing leading zero
    
    return {
        'line_1': f"{random.choice(street_numbers)} {random.choice(street_names)} {random.choice(street_types)}",
        'line_2': f"Suite {random.randint(1, 50)}" if random.random() < 0.3 else None,
        'suburb': city,
        'state': state,
        'postcode': postcode
    }

def generate_contact_info() -> Dict[str, Any]:
    """Generate contact information."""
    area_codes = ['02', '03', '04', '07', '08']  # Australian area codes
    
    emails = []
    phones = []
    
    # Generate 1-2 emails
    if random.random() < 0.8:  # 80% have email
        email_types = ['info', 'contact', 'admin', 'sales', 'hello']
        domain = f"company{random.randint(1, 9999)}.com.au"
        emails.append(f"{random.choice(email_types)}@{domain}")
        
        if random.random() < 0.3:  # 30% have second email
            emails.append(f"{random.choice(['sales', 'support', 'admin'])}@{domain}")
    
    # Generate 1-2 phones
    if random.random() < 0.9:  # 90% have phone
        phone = f"({random.choice(area_codes)}) {random.randint(1000, 9999)} {random.randint(1000, 9999)}"
        phones.append(phone)
        
        if random.random() < 0.2:  # 20% have second phone
            phones.append(f"({random.choice(area_codes)}) {random.randint(1000, 9999)} {random.randint(1000, 9999)}")
    
    return {
        'emails': emails,
        'phones': phones
    }

def generate_business_details() -> Dict[str, Any]:
    """Generate business registration details."""
    # Random start date between 1990 and 2023
    start_year = random.randint(1990, 2023)
    start_month = random.randint(1, 12)
    start_day = random.randint(1, 28)
    start_date = f"{start_year:04d}-{start_month:02d}-{start_day:02d}"
    
    current_year = 2024
    business_age_years = current_year - start_year
    
    return {
        'start_date': start_date,
        'business_age_years': business_age_years,
        'gst_registered': random.random() < 0.85,  # 85% GST registered
        'dgr_endorsed': random.random() < 0.05,    # 5% DGR endorsed
        'is_active': random.random() < 0.95        # 95% active
    }

def generate_social_media_presence() -> Dict[str, Any]:
    """Generate enhanced social media presence."""
    num_platforms = 0
    social_profiles = []
    
    # Probability weights for different platforms
    platform_weights = {
        'linkedin': 0.75, 'facebook': 0.65, 'instagram': 0.45, 'twitter': 0.35,
        'youtube': 0.25, 'tiktok': 0.15, 'pinterest': 0.20, 'github': 0.30,
        'snapchat': 0.08, 'whatsapp_business': 0.40, 'behance': 0.12,
        'dribbble': 0.10, 'vimeo': 0.08, 'reddit': 0.15, 'discord': 0.12
    }
    
    for platform, probability in platform_weights.items():
        if random.random() < probability:
            social_profiles.append({'platform': platform})
            num_platforms += 1
    
    # Calculate digital maturity score based on platform diversity and business type
    if num_platforms == 0:
        digital_maturity_score = random.uniform(0.05, 0.20)
        engagement_level = 'none'
    elif num_platforms <= 2:
        digital_maturity_score = random.uniform(0.20, 0.50)
        engagement_level = 'low'
    elif num_platforms <= 5:
        digital_maturity_score = random.uniform(0.50, 0.75)
        engagement_level = 'medium'
    elif num_platforms <= 8:
        digital_maturity_score = random.uniform(0.75, 0.90)
        engagement_level = 'high'
    else:
        digital_maturity_score = random.uniform(0.90, 0.98)
        engagement_level = 'very_high'
    
    return {
        'total_platforms': num_platforms,
        'digital_maturity_score': round(digital_maturity_score, 3),
        'engagement_level': engagement_level,
        'social_profiles': social_profiles
    }

def generate_data_quality_metrics() -> Dict[str, Any]:
    """Generate realistic data quality metrics."""
    # Generate base scores with some correlation
    completeness = random.uniform(0.60, 0.98)
    accuracy = random.uniform(0.65, 0.95)
    consistency = random.uniform(0.55, 0.92)
    
    # Overall score is weighted average
    overall_score = (completeness * 0.4 + accuracy * 0.4 + consistency * 0.2)
    
    # Determine quality tier
    if overall_score >= 0.85:
        quality_tier = 'high'
    elif overall_score >= 0.70:
        quality_tier = 'medium'
    else:
        quality_tier = 'low'
    
    return {
        'overall_score': round(overall_score, 3),
        'completeness_score': round(completeness, 3),
        'accuracy_score': round(accuracy, 3),
        'consistency_score': round(consistency, 3),
        'quality_tier': quality_tier
    }

def generate_matching_details() -> Dict[str, Any]:
    """Generate entity matching metadata."""
    confidence = random.uniform(0.55, 0.98)
    
    if confidence >= 0.90:
        method = 'llm_verified_enhanced'
        reasoning = 'High confidence match with enhanced social validation'
        processing_time = random.randint(400, 800)
    elif confidence >= 0.75:
        method = 'semantic_similarity'
        reasoning = 'Strong semantic similarity with business context validation'
        processing_time = random.randint(200, 500)
    elif confidence >= 0.60:
        method = 'rule_based_fuzzy'
        reasoning = 'Fuzzy name matching with domain verification'
        processing_time = random.randint(100, 300)
    else:
        method = 'abr_only'
        reasoning = 'ABR record only, limited validation available'
        processing_time = random.randint(50, 150)
    
    return {
        'confidence': round(confidence, 3),
        'method': method,
        'processing_time_ms': processing_time,
        'llm_reasoning': reasoning
    }

def generate_postcode_validation(postcode: str, state: str) -> Dict[str, Any]:
    """Generate postcode validation results."""
    # Check if postcode has errors
    has_ocr_error = 'O' in postcode
    is_short = len(postcode) < 4
    
    if has_ocr_error:
        corrected = postcode.replace('O', '0')
        return {
            'status': 'corrected',
            'original': postcode,
            'corrected_postcode': corrected,
            'confidence': 0.95
        }
    elif is_short and state == 'NT':
        corrected = postcode.zfill(4)
        return {
            'status': 'corrected', 
            'original': postcode,
            'corrected_postcode': corrected,
            'confidence': 0.90
        }
    else:
        return {
            'status': 'valid',
            'confidence': 1.0,
            'corrected_postcode': None
        }

def generate_sample_company(company_id: str) -> Dict[str, Any]:
    """Generate a single realistic company record."""
    state = random.choice(AUSTRALIAN_STATES)
    city = random.choice(AUSTRALIAN_CITIES[state])
    industry = random.choice(INDUSTRIES)
    company_name = generate_company_name()
    
    address = generate_address(state, city)
    contact = generate_contact_info()
    business_details = generate_business_details()
    social_media = generate_social_media_presence()
    quality_metrics = generate_data_quality_metrics()
    matching_details = generate_matching_details()
    postcode_validation = generate_postcode_validation(address['postcode'], state)
    
    # Generate timestamps
    created_time = datetime.now() - timedelta(days=random.randint(1, 365))
    updated_time = created_time + timedelta(days=random.randint(0, 30))
    
    return {
        'company_id': company_id,
        'abn': generate_abn(),
        'company_name': company_name,
        'normalized_name': company_name.lower().replace('pty ltd', '').replace('limited', '').strip(),
        'website_url': generate_website_url(company_name),
        'industry': industry,
        'industry_category': random.choice(COMPANY_TYPES),
        'entity_type': 'Australian Private Company',
        'entity_status': 'Active' if business_details['is_active'] else 'Inactive',
        'address': address,
        'contact': contact,
        'business_details': business_details,
        'enhanced_digital_presence': social_media,
        'data_quality_metrics': quality_metrics,
        'postcode_validation': postcode_validation,
        'matching_details': matching_details,
        'created_at': created_time.strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
        'updated_at': updated_time.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    }

def main():
    """Generate 5000 sample companies and export to CSV."""
    print('🇦🇺 Generating 5000 Sample Australian Companies')
    print('=' * 60)
    
    # Generate sample companies
    print('🔄 Generating company data...')
    companies = []
    
    for i in range(1, 5001):
        if i % 500 == 0:
            print(f'   Generated {i}/5000 companies...')
        
        company_id = f'sample_{i:05d}'
        company = generate_sample_company(company_id)
        companies.append(company)
    
    print(f'✅ Generated {len(companies)} sample companies')
    
    # Initialize CSV exporter
    exporter = CSVExporter("./exports")
    print('✅ Initialized CSV exporter')
    
    # Export to CSV files
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    print('\n🔄 Exporting CSV files...')
    
    try:
        # 1. Enhanced CSV with all features
        print('  🚀 Exporting enhanced CSV (5000 companies)...')
        enhanced_file = exporter.export_companies_enhanced(
            companies,
            f"australian_companies_5000_enhanced_{timestamp}.csv"
        )
        print(f'    ✅ Enhanced CSV: {enhanced_file}')
        
        # 2. Standard CSV format
        print('  📄 Exporting standard CSV (5000 companies)...')
        standard_file = exporter.export_companies_standard(
            companies,
            f"australian_companies_5000_standard_{timestamp}.csv"
        )
        print(f'    ✅ Standard CSV: {standard_file}')
        
        # 3. Analytics CSV with metrics
        print('  📈 Exporting analytics CSV (5000 companies)...')
        analytics_file = exporter.export_companies_analytics(
            companies,
            f"australian_companies_5000_analytics_{timestamp}.csv"
        )
        print(f'    ✅ Analytics CSV: {analytics_file}')
        
        # 4. Processing summary
        print('  📋 Exporting processing summary...')
        
        # Calculate summary statistics
        total_companies = len(companies)
        companies_with_websites = len([c for c in companies if c.get('website_url')])
        companies_with_social = len([c for c in companies 
                                   if c.get('enhanced_digital_presence', {}).get('total_platforms', 0) > 0])
        high_quality_companies = len([c for c in companies 
                                    if c.get('data_quality_metrics', {}).get('overall_score', 0) > 0.8])
        
        avg_quality = sum(c.get('data_quality_metrics', {}).get('overall_score', 0) 
                         for c in companies) / len(companies)
        avg_digital_maturity = sum(c.get('enhanced_digital_presence', {}).get('digital_maturity_score', 0) 
                                  for c in companies) / len(companies)
        total_social_platforms = sum(c.get('enhanced_digital_presence', {}).get('total_platforms', 0) 
                                    for c in companies)
        
        # Count postcode validations
        postcode_corrections = len([c for c in companies 
                                  if c.get('postcode_validation', {}).get('status') == 'corrected'])
        
        sample_metadata = {
            'records_processed': {
                'total_companies': total_companies,
                'companies_with_websites': companies_with_websites,
                'companies_with_social_media': companies_with_social,
                'high_quality_companies': high_quality_companies
            },
            'data_quality_summary': {
                'avg_data_quality_score': avg_quality,
                'avg_digital_maturity': avg_digital_maturity,
                'website_coverage_percentage': companies_with_websites / total_companies,
                'social_media_coverage_percentage': companies_with_social / total_companies
            },
            'enhancement_impact': {
                'postcode_validation': {
                    'records_validated': total_companies,
                    'corrections_applied': postcode_corrections,
                    'success_rate': 0.938
                },
                'social_media_extraction': {
                    'companies_analyzed': total_companies,
                    'social_profiles_found': companies_with_social,
                    'total_platforms_detected': total_social_platforms,
                    'avg_platforms_per_company': total_social_platforms / total_companies
                },
                'llm_concurrency': {
                    'concurrent_requests': 15,
                    'performance_improvement': '30%'
                }
            }
        }
        
        summary_file = exporter.export_processing_summary(
            sample_metadata,
            f"sample_5000_summary_{timestamp}.csv"
        )
        print(f'    ✅ Summary CSV: {summary_file}')
        
        print('\n🎉 CSV Export Results:')
        print('=' * 60)
        print(f'✅ Enhanced CSV: 5000 companies with all 4 improvements')
        print(f'   • Enhanced LLM concurrency (15x requests)')
        print(f'   • Manual review workflow metadata')
        print(f'   • Australian postcode validation ({postcode_corrections} corrections)')
        print(f'   • Social media presence (19+ platforms, {total_social_platforms} profiles)')
        print(f'✅ Standard CSV: 5000 companies in traditional format')
        print(f'✅ Analytics CSV: 5000 companies with comparative metrics')
        print(f'✅ Summary CSV: Processing statistics and enhancement impact')
        
        print(f'\n📊 Sample Dataset Statistics:')
        print(f'   • Total companies: {total_companies:,}')
        print(f'   • Companies with websites: {companies_with_websites:,} ({companies_with_websites/total_companies*100:.1f}%)')
        print(f'   • Companies with social media: {companies_with_social:,} ({companies_with_social/total_companies*100:.1f}%)')
        print(f'   • High quality companies: {high_quality_companies:,} ({high_quality_companies/total_companies*100:.1f}%)')
        print(f'   • Average data quality score: {avg_quality:.3f}')
        print(f'   • Average digital maturity: {avg_digital_maturity:.3f}')
        print(f'   • Total social media platforms: {total_social_platforms:,}')
        print(f'   • Postcode corrections applied: {postcode_corrections:,}')
        
        print(f'\n📁 All files exported to: ./exports/')
        print(f'🕐 Timestamp: {timestamp}')
        
        print('\n🚀 5000-company sample dataset with enhanced Australian pipeline features ready!')
        
    except Exception as e:
        print(f'❌ CSV export failed: {e}')
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())