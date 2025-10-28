"""
CSV Exporter for Australian Company Data Pipeline.
Exports processed company data to CSV format for analysis and sharing.
"""

import csv
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
from pathlib import Path
import pandas as pd

logger = logging.getLogger(__name__)


class CSVExporter:
    """
    Export processed company data to CSV format.
    Supports both standard and enhanced formats with all pipeline enhancements.
    """
    
    def __init__(self, output_directory: str = "./exports"):
        """Initialize CSV exporter with output directory."""
        self.output_directory = Path(output_directory)
        self.output_directory.mkdir(exist_ok=True)
        
    def export_companies_standard(self, 
                                companies: List[Dict[str, Any]], 
                                filename: Optional[str] = None) -> str:
        """
        Export companies to standard CSV format.
        
        Args:
            companies: List of company records
            filename: Optional custom filename
            
        Returns:
            Path to exported CSV file
        """
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"australian_companies_{timestamp}.csv"
        
        filepath = self.output_directory / filename
        
        # Define standard CSV columns
        fieldnames = [
            'company_id',
            'abn',
            'company_name',
            'normalized_name',
            'website_url',
            'industry',
            'industry_category',
            'entity_type',
            'entity_status',
            'address_line_1',
            'address_line_2',
            'suburb',
            'state',
            'postcode',
            'email',
            'phone',
            'start_date',
            'gst_registered',
            'data_quality_score',
            'matching_confidence',
            'matching_method',
            'created_at',
            'updated_at'
        ]
        
        with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            for company in companies:
                # Flatten nested data structures
                row = self._flatten_company_record(company, fieldnames)
                writer.writerow(row)
        
        logger.info(f"Exported {len(companies)} companies to {filepath}")
        return str(filepath)
    
    def export_companies_enhanced(self, 
                                companies: List[Dict[str, Any]], 
                                filename: Optional[str] = None) -> str:
        """
        Export companies to enhanced CSV format with all improvements.
        
        Args:
            companies: List of company records
            filename: Optional custom filename
            
        Returns:
            Path to exported CSV file
        """
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"australian_companies_enhanced_{timestamp}.csv"
        
        filepath = self.output_directory / filename
        
        # Define enhanced CSV columns (includes all improvements)
        fieldnames = [
            # Basic company info
            'company_id',
            'abn',
            'company_name',
            'normalized_name',
            'website_url',
            'industry',
            'industry_category',
            'entity_type',
            'entity_status',
            
            # Address with enhanced validation
            'address_line_1',
            'address_line_2',
            'suburb',
            'state',
            'postcode',
            'postcode_validation_status',
            'postcode_corrected',
            'postcode_confidence',
            
            # Contact information
            'primary_email',
            'secondary_email',
            'primary_phone',
            'secondary_phone',
            
            # Business details
            'start_date',
            'business_age_years',
            'gst_registered',
            'dgr_endorsed',
            'is_active',
            
            # Enhanced digital presence (19+ platforms)
            'has_website',
            'has_linkedin',
            'has_facebook',
            'has_instagram',
            'has_twitter',
            'has_youtube',
            'has_tiktok',
            'has_github',
            'has_pinterest',
            'social_platforms_count',
            'digital_maturity_score',
            'digital_presence_level',
            
            # Data quality and matching
            'data_quality_score',
            'completeness_score',
            'accuracy_score',
            'consistency_score',
            'quality_tier',
            'matching_confidence',
            'matching_method',
            'llm_reasoning_summary',
            'manual_review_required',
            
            # Enhanced processing metadata
            'processing_time_ms',
            'llm_provider',
            'llm_model',
            'enhancement_version',
            'created_at',
            'updated_at'
        ]
        
        with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            for company in companies:
                # Flatten enhanced data structures
                row = self._flatten_enhanced_company_record(company, fieldnames)
                writer.writerow(row)
        
        logger.info(f"Exported {len(companies)} enhanced companies to {filepath}")
        return str(filepath)
    
    def export_companies_analytics(self, 
                                 companies: List[Dict[str, Any]], 
                                 filename: Optional[str] = None) -> str:
        """
        Export companies with analytics and summary statistics.
        
        Args:
            companies: List of company records
            filename: Optional custom filename
            
        Returns:
            Path to exported CSV file
        """
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"australian_companies_analytics_{timestamp}.csv"
        
        filepath = self.output_directory / filename
        
        # Create pandas DataFrame for analytics
        df = pd.DataFrame([
            self._flatten_enhanced_company_record(company, []) 
            for company in companies
        ])
        
        # Add analytics columns
        df['industry_company_count'] = df.groupby('industry')['company_id'].transform('count')
        df['state_company_count'] = df.groupby('state')['company_id'].transform('count')
        df['avg_quality_in_industry'] = df.groupby('industry')['data_quality_score'].transform('mean')
        df['avg_digital_maturity_in_state'] = df.groupby('state')['digital_maturity_score'].transform('mean')
        df['is_above_avg_quality'] = df['data_quality_score'] > df['data_quality_score'].mean()
        df['is_above_avg_digital'] = df['digital_maturity_score'] > df['digital_maturity_score'].mean()
        
        # Export to CSV
        df.to_csv(filepath, index=False)
        
        logger.info(f"Exported {len(companies)} companies with analytics to {filepath}")
        return str(filepath)
    
    def export_processing_summary(self, 
                                pipeline_metadata: Dict[str, Any], 
                                filename: Optional[str] = None) -> str:
        """
        Export pipeline processing summary and statistics.
        
        Args:
            pipeline_metadata: Pipeline execution metadata
            filename: Optional custom filename
            
        Returns:
            Path to exported CSV file
        """
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"pipeline_summary_{timestamp}.csv"
        
        filepath = self.output_directory / filename
        
        # Create summary data
        summary_data = []
        
        # Basic processing stats
        if 'records_processed' in pipeline_metadata:
            for key, value in pipeline_metadata['records_processed'].items():
                summary_data.append({
                    'category': 'processing',
                    'metric': key,
                    'value': value,
                    'description': f'Count of {key.replace("_", " ")}'
                })
        
        # Performance metrics
        if 'performance_metrics' in pipeline_metadata:
            for key, value in pipeline_metadata['performance_metrics'].items():
                summary_data.append({
                    'category': 'performance',
                    'metric': key,
                    'value': value,
                    'description': f'Performance metric: {key.replace("_", " ")}'
                })
        
        # Quality metrics
        if 'data_quality_summary' in pipeline_metadata:
            for key, value in pipeline_metadata['data_quality_summary'].items():
                summary_data.append({
                    'category': 'quality',
                    'metric': key,
                    'value': value,
                    'description': f'Data quality: {key.replace("_", " ")}'
                })
        
        # Enhancement impact
        if 'enhancement_impact' in pipeline_metadata:
            for enhancement, metrics in pipeline_metadata['enhancement_impact'].items():
                for key, value in metrics.items():
                    summary_data.append({
                        'category': f'enhancement_{enhancement}',
                        'metric': key,
                        'value': value,
                        'description': f'{enhancement}: {key.replace("_", " ")}'
                    })
        
        # Write summary CSV
        with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['category', 'metric', 'value', 'description']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(summary_data)
        
        logger.info(f"Exported processing summary to {filepath}")
        return str(filepath)
    
    def _flatten_company_record(self, company: Dict[str, Any], fieldnames: List[str]) -> Dict[str, Any]:
        """Flatten nested company record for standard CSV format."""
        row = {}
        
        # Basic fields
        row['company_id'] = company.get('company_id', '')
        row['abn'] = company.get('abn', '')
        row['company_name'] = company.get('company_name', '')
        row['normalized_name'] = company.get('normalized_name', '')
        row['website_url'] = company.get('website_url', '')
        row['industry'] = company.get('industry', '')
        row['industry_category'] = company.get('industry_category', '')
        row['entity_type'] = company.get('entity_type', '')
        row['entity_status'] = company.get('entity_status', '')
        
        # Address
        address = company.get('address', {})
        row['address_line_1'] = address.get('line_1', '')
        row['address_line_2'] = address.get('line_2', '')
        row['suburb'] = address.get('suburb', '')
        row['state'] = address.get('state', '')
        row['postcode'] = address.get('postcode', '')
        
        # Contact
        contact = company.get('contact', {}) or company.get('contact_information', {})
        emails = contact.get('emails', []) if isinstance(contact.get('emails'), list) else [contact.get('email', '')]
        phones = contact.get('phones', []) if isinstance(contact.get('phones'), list) else [contact.get('phone', '')]
        
        row['email'] = emails[0] if emails else ''
        row['phone'] = phones[0] if phones else ''
        
        # Business details
        business = company.get('business_details', {})
        row['start_date'] = business.get('start_date', '')
        row['gst_registered'] = business.get('gst_registered', '')
        
        # Quality metrics
        quality = company.get('data_quality_metrics', {})
        row['data_quality_score'] = quality.get('overall_score', '')
        
        # Matching info
        matching = company.get('data_lineage', {}) or company.get('matching_details', {})
        row['matching_confidence'] = matching.get('matching_confidence', '') or matching.get('confidence', '')
        row['matching_method'] = matching.get('matching_method', '')
        
        # Metadata
        metadata = company.get('metadata', {})
        row['created_at'] = metadata.get('created_at', '') or company.get('created_at', '')
        row['updated_at'] = metadata.get('updated_at', '') or company.get('updated_at', '')
        
        # Ensure all fieldnames are present (only if fieldnames provided)
        if fieldnames:
            for field in fieldnames:
                if field not in row:
                    row[field] = ''
            # Remove any fields not in fieldnames
            row = {k: v for k, v in row.items() if k in fieldnames}
        
        return row
    
    def _flatten_enhanced_company_record(self, company: Dict[str, Any], fieldnames: List[str]) -> Dict[str, Any]:
        """Flatten enhanced company record with all improvements."""
        # Start with standard flattening - pass fieldnames to avoid field conflicts
        row = self._flatten_company_record(company, fieldnames)
        
        # Enhanced postcode validation
        postcode_validation = company.get('postcode_validation', {})
        row['postcode_validation_status'] = postcode_validation.get('status', '')
        row['postcode_corrected'] = postcode_validation.get('corrected_postcode', '')
        row['postcode_confidence'] = postcode_validation.get('confidence', '')
        
        # Multiple contact methods
        contact = company.get('contact', {}) or company.get('contact_information', {})
        emails = contact.get('emails', []) if isinstance(contact.get('emails'), list) else [contact.get('email', '')]
        phones = contact.get('phones', []) if isinstance(contact.get('phones'), list) else [contact.get('phone', '')]
        
        row['primary_email'] = emails[0] if emails else ''
        row['secondary_email'] = emails[1] if len(emails) > 1 else ''
        row['primary_phone'] = phones[0] if phones else ''
        row['secondary_phone'] = phones[1] if len(phones) > 1 else ''
        
        # Enhanced business details
        business = company.get('business_details', {})
        row['business_age_years'] = business.get('business_age_years', '')
        row['dgr_endorsed'] = business.get('dgr_endorsed', '')
        row['is_active'] = business.get('is_active', '')
        
        # Enhanced digital presence
        digital = company.get('enhanced_digital_presence', {})
        row['social_platforms_count'] = digital.get('total_platforms', 0)
        row['digital_maturity_score'] = digital.get('digital_maturity_score', '')
        row['digital_presence_level'] = digital.get('engagement_level', '')
        
        # Social media presence flags
        social_profiles = digital.get('social_profiles', [])
        platforms = [profile.get('platform', '') for profile in social_profiles]
        
        row['has_website'] = bool(company.get('website_url'))
        row['has_linkedin'] = 'linkedin' in platforms
        row['has_facebook'] = 'facebook' in platforms
        row['has_instagram'] = 'instagram' in platforms
        row['has_twitter'] = 'twitter' in platforms
        row['has_youtube'] = 'youtube' in platforms
        row['has_tiktok'] = 'tiktok' in platforms
        row['has_github'] = 'github' in platforms
        row['has_pinterest'] = 'pinterest' in platforms
        
        # Enhanced quality metrics
        quality = company.get('data_quality_metrics', {})
        row['completeness_score'] = quality.get('completeness_score', '')
        row['accuracy_score'] = quality.get('accuracy_score', '')
        row['consistency_score'] = quality.get('consistency_score', '')
        row['quality_tier'] = quality.get('quality_tier', '')
        
        # Enhanced matching info
        matching = company.get('data_lineage', {}) or company.get('matching_details', {})
        row['llm_reasoning_summary'] = (matching.get('llm_reasoning', '') or matching.get('matching_reasoning', ''))[:200]  # Truncate for CSV
        row['manual_review_required'] = matching.get('manual_review_required', '')
        
        # Processing metadata
        row['processing_time_ms'] = matching.get('processing_time_ms', '')
        row['llm_provider'] = 'anthropic'
        row['llm_model'] = 'gpt-4-turbo-preview'
        row['enhancement_version'] = '2.0'
        
        # Clean up boolean values for CSV
        for key, value in row.items():
            if isinstance(value, bool):
                row[key] = 'Yes' if value else 'No'
            elif value is None:
                row[key] = ''
        
        # Filter to fieldnames if provided
        if fieldnames:
            row = {k: v for k, v in row.items() if k in fieldnames}
        
        return row


# Example usage and integration
if __name__ == "__main__":
    # Demo CSV export functionality
    sample_companies = [
        {
            'company_id': 'enhanced_001',
            'abn': '53004085616',
            'company_name': 'Sydney Tech Solutions Pty Ltd',
            'website_url': 'https://sydneytech.com.au',
            'industry': 'Technology',
            'address': {
                'suburb': 'Sydney',
                'state': 'NSW',
                'postcode': '2000'
            },
            'contact': {
                'emails': ['info@sydneytech.com.au'],
                'phones': ['(02) 8765 4321']
            },
            'data_quality_metrics': {
                'overall_score': 0.94
            },
            'enhanced_digital_presence': {
                'total_platforms': 5,
                'digital_maturity_score': 0.87,
                'social_profiles': [
                    {'platform': 'linkedin'},
                    {'platform': 'facebook'},
                    {'platform': 'twitter'}
                ]
            }
        }
    ]
    
    # Test CSV export
    exporter = CSVExporter()
    
    # Export standard format
    standard_file = exporter.export_companies_standard(sample_companies)
    print(f"✅ Standard CSV exported: {standard_file}")
    
    # Export enhanced format
    enhanced_file = exporter.export_companies_enhanced(sample_companies)
    print(f"✅ Enhanced CSV exported: {enhanced_file}")
    
    print("🚀 CSV export functionality ready!")