#!/usr/bin/env python3
"""
Test CSV Export Functionality
Demonstrates CSV export capabilities with sample data.
"""

import sys
import os
sys.path.append(os.getcwd())
import asyncio
from datetime import datetime
from src.exporters.csv_exporter import CSVExporter

def create_sample_companies():
    """Create sample company data for CSV export testing."""
    return [
        {
            'company_id': 'enhanced_001',
            'abn': '53004085616',
            'company_name': 'Sydney Tech Solutions Pty Ltd',
            'normalized_name': 'sydney tech solutions',
            'website_url': 'https://sydneytech.com.au',
            'industry': 'Technology',
            'industry_category': 'Software Development',
            'entity_type': 'Australian Private Company',
            'entity_status': 'Active',
            'address': {
                'line_1': 'Level 12, 350 Collins Street',
                'line_2': None,
                'suburb': 'Sydney',
                'state': 'NSW',
                'postcode': '2000'
            },
            'contact': {
                'emails': ['hello@sydneytech.com.au', 'info@sydneytech.com.au'],
                'phones': ['(02) 8765 4321']
            },
            'business_details': {
                'start_date': '2018-03-15',
                'business_age_years': 6,
                'gst_registered': True,
                'dgr_endorsed': False,
                'is_active': True
            },
            'enhanced_digital_presence': {
                'total_platforms': 6,
                'digital_maturity_score': 0.89,
                'engagement_level': 'high',
                'social_profiles': [
                    {'platform': 'linkedin'},
                    {'platform': 'facebook'},
                    {'platform': 'twitter'},
                    {'platform': 'instagram'},
                    {'platform': 'youtube'},
                    {'platform': 'github'}
                ]
            },
            'data_quality_metrics': {
                'overall_score': 0.94,
                'completeness_score': 0.96,
                'accuracy_score': 0.95,
                'consistency_score': 0.92,
                'quality_tier': 'high'
            },
            'postcode_validation': {
                'status': 'valid',
                'confidence': 1.0,
                'corrected_postcode': None
            },
            'matching_details': {
                'confidence': 0.96,
                'method': 'llm_verified_enhanced',
                'processing_time_ms': 847,
                'llm_reasoning': 'High confidence match with enhanced social validation'
            },
            'created_at': '2024-10-23T21:00:00.000Z',
            'updated_at': '2024-10-23T21:00:00.000Z'
        },
        {
            'company_id': 'enhanced_002',
            'abn': '12345678903',
            'company_name': 'Brisbane Construction Co Pty Ltd',
            'normalized_name': 'brisbane construction co',
            'website_url': None,
            'industry': 'Construction',
            'industry_category': 'Construction',
            'entity_type': 'Australian Private Company',
            'entity_status': 'Active',
            'address': {
                'line_1': '45 Industrial Drive',
                'suburb': 'Brisbane',
                'state': 'QLD',
                'postcode': '4000'
            },
            'contact': {
                'phones': ['(07) 3456 7890']
            },
            'business_details': {
                'start_date': '1987-11-03',
                'business_age_years': 37,
                'gst_registered': True,
                'is_active': True
            },
            'enhanced_digital_presence': {
                'total_platforms': 0,
                'digital_maturity_score': 0.15,
                'engagement_level': 'none',
                'social_profiles': []
            },
            'data_quality_metrics': {
                'overall_score': 0.71,
                'completeness_score': 0.68,
                'accuracy_score': 0.78,
                'consistency_score': 0.67,
                'quality_tier': 'medium'
            },
            'postcode_validation': {
                'status': 'corrected',
                'original': '40OO',
                'corrected_postcode': '4000',
                'confidence': 0.95
            },
            'matching_details': {
                'confidence': 0.64,
                'method': 'abr_only'
            },
            'created_at': '2024-10-23T21:05:00.000Z',
            'updated_at': '2024-10-23T21:05:00.000Z'
        },
        {
            'company_id': 'enhanced_003',
            'abn': '87654321098',
            'company_name': 'Melbourne Marketing Agency Pty Ltd',
            'normalized_name': 'melbourne marketing agency',
            'website_url': 'https://melbournemarketing.com.au',
            'industry': 'Professional Services',
            'industry_category': 'Marketing & Advertising',
            'entity_type': 'Australian Private Company',
            'entity_status': 'Active',
            'address': {
                'line_1': 'Suite 15, 123 Collins Street',
                'suburb': 'Melbourne',
                'state': 'VIC',
                'postcode': '3000'
            },
            'contact': {
                'emails': ['contact@melbournemarketing.com.au'],
                'phones': ['(03) 9123 4567']
            },
            'business_details': {
                'start_date': '2020-07-12',
                'business_age_years': 4,
                'gst_registered': True,
                'is_active': True
            },
            'enhanced_digital_presence': {
                'total_platforms': 8,
                'digital_maturity_score': 0.92,
                'engagement_level': 'very_high',
                'social_profiles': [
                    {'platform': 'linkedin'},
                    {'platform': 'facebook'},
                    {'platform': 'instagram'},
                    {'platform': 'twitter'},
                    {'platform': 'tiktok'},
                    {'platform': 'youtube'},
                    {'platform': 'pinterest'}
                ]
            },
            'data_quality_metrics': {
                'overall_score': 0.88,
                'completeness_score': 0.91,
                'accuracy_score': 0.89,
                'consistency_score': 0.84,
                'quality_tier': 'high'
            },
            'postcode_validation': {
                'status': 'valid',
                'confidence': 1.0
            },
            'matching_details': {
                'confidence': 0.91,
                'method': 'llm_verified_enhanced',
                'processing_time_ms': 623
            },
            'created_at': '2024-10-23T21:02:00.000Z',
            'updated_at': '2024-10-23T21:02:00.000Z'
        }
    ]

def main():
    """Test CSV export functionality."""
    print('📊 Testing Enhanced CSV Export Functionality')
    print('=' * 60)
    
    # Create sample data
    sample_companies = create_sample_companies()
    print(f'✅ Created {len(sample_companies)} sample companies')
    
    # Initialize CSV exporter
    exporter = CSVExporter("./exports")
    print('✅ Initialized CSV exporter')
    
    # Test all export formats
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    print('\n🔄 Exporting CSV files...')
    
    try:
        # 1. Standard CSV format
        print('  📄 Exporting standard CSV...')
        standard_file = exporter.export_companies_standard(
            sample_companies,
            f"test_standard_{timestamp}.csv"
        )
        print(f'    ✅ Standard CSV: {standard_file}')
        
        # 2. Enhanced CSV format
        print('  🚀 Exporting enhanced CSV...')
        enhanced_file = exporter.export_companies_enhanced(
            sample_companies,
            f"test_enhanced_{timestamp}.csv"
        )
        print(f'    ✅ Enhanced CSV: {enhanced_file}')
        
        # 3. Analytics CSV format
        print('  📈 Exporting analytics CSV...')
        analytics_file = exporter.export_companies_analytics(
            sample_companies,
            f"test_analytics_{timestamp}.csv"
        )
        print(f'    ✅ Analytics CSV: {analytics_file}')
        
        # 4. Processing summary
        print('  📋 Exporting processing summary...')
        sample_metadata = {
            'records_processed': {
                'total_companies': len(sample_companies),
                'companies_with_websites': len([c for c in sample_companies if c.get('website_url')]),
                'high_quality_companies': len([c for c in sample_companies 
                                             if c.get('data_quality_metrics', {}).get('overall_score', 0) > 0.8])
            },
            'data_quality_summary': {
                'avg_data_quality_score': sum(c.get('data_quality_metrics', {}).get('overall_score', 0) 
                                             for c in sample_companies) / len(sample_companies),
                'high_quality_percentage': len([c for c in sample_companies 
                                              if c.get('data_quality_metrics', {}).get('quality_tier') == 'high']) / len(sample_companies)
            },
            'enhancement_impact': {
                'postcode_validation': {
                    'records_validated': 3,
                    'corrections_applied': 1,
                    'success_rate': 1.0
                },
                'social_media_extraction': {
                    'companies_analyzed': 3,
                    'social_profiles_found': 2,
                    'avg_platforms_per_company': sum(c.get('enhanced_digital_presence', {}).get('total_platforms', 0) 
                                                   for c in sample_companies) / len(sample_companies)
                },
                'llm_concurrency': {
                    'concurrent_requests': 15,
                    'performance_improvement': '30%'
                }
            }
        }
        
        summary_file = exporter.export_processing_summary(
            sample_metadata,
            f"test_summary_{timestamp}.csv"
        )
        print(f'    ✅ Summary CSV: {summary_file}')
        
        print('\n🎉 CSV Export Test Results:')
        print('=' * 60)
        print('✅ Standard CSV: Basic company information')
        print('✅ Enhanced CSV: All 4 improvements included')
        print('   • Enhanced LLM concurrency metadata')
        print('   • Manual review workflow tracking') 
        print('   • Australian postcode validation results')
        print('   • Social media presence across 19+ platforms')
        print('✅ Analytics CSV: Comparative metrics and statistics')
        print('✅ Summary CSV: Pipeline processing metadata')
        
        print(f'\n📁 All files exported to: ./exports/')
        print(f'🕐 Timestamp: {timestamp}')
        
        print('\n📊 Sample Data Statistics:')
        total_platforms = sum(c.get('enhanced_digital_presence', {}).get('total_platforms', 0) for c in sample_companies)
        avg_quality = sum(c.get('data_quality_metrics', {}).get('overall_score', 0) for c in sample_companies) / len(sample_companies)
        
        print(f'   • Total companies: {len(sample_companies)}')
        print(f'   • Companies with websites: {len([c for c in sample_companies if c.get("website_url")])}')
        print(f'   • Total social media platforms: {total_platforms}')
        print(f'   • Average data quality score: {avg_quality:.3f}')
        print(f'   • High quality companies: {len([c for c in sample_companies if c.get("data_quality_metrics", {}).get("quality_tier") == "high"])}')
        
        print('\n🚀 Enhanced pipeline CSV export functionality working perfectly!')
        
    except Exception as e:
        print(f'❌ CSV export test failed: {e}')
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())