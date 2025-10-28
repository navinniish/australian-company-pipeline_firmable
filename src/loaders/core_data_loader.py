"""
Core data loader for loading transformed data into core PostgreSQL tables.
"""

import asyncio
import logging
from typing import List, Dict, Optional, Any
import json
from datetime import datetime

from ..utils.database import DatabaseManager

logger = logging.getLogger(__name__)


class CoreDataLoader:
    """
    Loads transformed company data into core PostgreSQL tables.
    Handles data lineage, deduplication, and incremental updates.
    """
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.batch_size = 1000
    
    async def load_companies(self) -> int:
        """
        Load transformed companies into core.companies table.
        
        Returns:
            Number of companies loaded
        """
        logger.info("Starting core company data loading")
        
        # Get transformed companies from a staging/temp table or process directly
        companies = await self._get_transformed_companies()
        
        if not companies:
            logger.info("No companies to load")
            return 0
        
        # Load companies in batches
        total_loaded = 0
        for i in range(0, len(companies), self.batch_size):
            batch = companies[i:i + self.batch_size]
            loaded_count = await self._load_company_batch(batch)
            total_loaded += loaded_count
            
            logger.debug(f"Loaded batch {i//self.batch_size + 1}: {loaded_count} companies")
        
        logger.info(f"Successfully loaded {total_loaded} companies")
        return total_loaded
    
    async def load_related_data(self) -> int:
        """
        Load related data (alternative names, contacts, etc.) for companies.
        
        Returns:
            Total number of related records loaded
        """
        logger.info("Loading related company data")
        
        total_loaded = 0
        
        # Load alternative names
        names_loaded = await self._load_company_names()
        total_loaded += names_loaded
        
        # Load contact information
        contacts_loaded = await self._load_company_contacts()
        total_loaded += contacts_loaded
        
        # Load industry classifications
        industries_loaded = await self._load_industry_classifications()
        total_loaded += industries_loaded
        
        # Load data lineage
        lineage_loaded = await self._load_data_lineage()
        total_loaded += lineage_loaded
        
        logger.info(f"Successfully loaded {total_loaded} related records")
        return total_loaded
    
    async def _get_transformed_companies(self) -> List[Dict]:
        """
        Get transformed companies ready for loading.
        In production, this might read from a staging table or be passed directly.
        """
        # For this implementation, we'll assume the data transformer has saved
        # records to a temporary staging table
        query = """
        SELECT * FROM staging.transformed_companies 
        WHERE processing_status = 'ready_for_load'
        ORDER BY data_quality_score DESC
        """
        
        try:
            return await self.db_manager.fetch_all(query)
        except:
            # If staging table doesn't exist, return empty list
            # In a real implementation, you'd coordinate this better
            logger.warning("No staging table found - using empty dataset")
            return []
    
    async def _load_company_batch(self, companies: List[Dict]) -> int:
        """Load a batch of companies using upsert logic."""
        if not companies:
            return 0
        
        # Prepare records for core.companies table
        company_records = []
        for company in companies:
            record = {
                'company_id': company['company_id'],
                'abn': company.get('abn'),
                'company_name': company['company_name'],
                'normalized_name': company['normalized_name'],
                'website_url': company.get('website_url'),
                'entity_type': company.get('entity_type'),
                'entity_status': company.get('entity_status'),
                'industry': company.get('industry'),
                'address_line_1': company.get('address_line_1'),
                'address_line_2': company.get('address_line_2'),
                'address_suburb': company.get('address_suburb'),
                'address_state': company.get('address_state'),
                'address_postcode': company.get('address_postcode'),
                'start_date': company.get('start_date'),
                'gst_registered': company.get('gst_registered'),
                'dgr_endorsed': company.get('dgr_endorsed'),
                'is_active': company.get('is_active', True),
                'data_quality_score': company.get('data_quality_score'),
                'data_source': company.get('data_source', []),
                'created_at': company.get('created_at', datetime.now()),
                'updated_at': datetime.now()
            }
            company_records.append(record)
        
        # Use upsert to handle duplicates
        conflict_columns = ['company_id']
        update_columns = [
            'company_name', 'normalized_name', 'website_url', 'entity_type',
            'entity_status', 'industry', 'address_line_1', 'address_line_2',
            'address_suburb', 'address_state', 'address_postcode', 'start_date',
            'gst_registered', 'dgr_endorsed', 'is_active', 'data_quality_score',
            'data_source', 'updated_at'
        ]
        
        return await self.db_manager.bulk_upsert(
            'core.companies',
            company_records,
            conflict_columns,
            update_columns
        )
    
    async def _load_company_names(self) -> int:
        """Load alternative company names."""
        # First, clear existing names for companies we're reloading
        await self.db_manager.execute("""
            DELETE FROM core.company_names 
            WHERE company_id IN (
                SELECT company_id FROM staging.transformed_companies 
                WHERE processing_status = 'ready_for_load'
            )
        """)
        
        # Get companies with their alternative names
        companies = await self._get_transformed_companies()
        
        name_records = []
        for company in companies:
            company_id = company['company_id']
            alternative_names = company.get('alternative_names', [])
            
            # Mark first name as primary if no primary exists
            primary_set = False
            
            for name_info in alternative_names:
                if isinstance(name_info, dict):
                    name = name_info.get('name')
                    name_type = name_info.get('type', 'trading')
                else:
                    name = name_info
                    name_type = 'trading'
                
                if name and name.strip():
                    is_primary = not primary_set and name_type in ['legal', 'trading']
                    if is_primary:
                        primary_set = True
                    
                    name_records.append({
                        'company_id': company_id,
                        'name': name.strip(),
                        'name_type': name_type,
                        'is_primary': is_primary,
                        'created_at': datetime.now()
                    })
        
        if name_records:
            return await self.db_manager.bulk_insert('core.company_names', name_records)
        
        return 0
    
    async def _load_company_contacts(self) -> int:
        """Load company contact information."""
        # Clear existing contacts
        await self.db_manager.execute("""
            DELETE FROM core.company_contacts 
            WHERE company_id IN (
                SELECT company_id FROM staging.transformed_companies 
                WHERE processing_status = 'ready_for_load'
            )
        """)
        
        companies = await self._get_transformed_companies()
        
        contact_records = []
        for company in companies:
            company_id = company['company_id']
            contact_details = company.get('contact_details', {})
            social_links = company.get('social_links', {})
            
            # Add email contacts
            for email in contact_details.get('emails', []):
                if email and email.strip():
                    contact_records.append({
                        'company_id': company_id,
                        'contact_type': 'email',
                        'contact_value': email.strip(),
                        'is_verified': False,
                        'created_at': datetime.now()
                    })
            
            # Add phone contacts
            for phone in contact_details.get('phones', []):
                if phone and phone.strip():
                    contact_records.append({
                        'company_id': company_id,
                        'contact_type': 'phone',
                        'contact_value': phone.strip(),
                        'is_verified': False,
                        'created_at': datetime.now()
                    })
            
            # Add social media contacts
            for platform, url in social_links.items():
                if url and url.strip():
                    contact_records.append({
                        'company_id': company_id,
                        'contact_type': platform,
                        'contact_value': url.strip(),
                        'is_verified': False,
                        'created_at': datetime.now()
                    })
        
        if contact_records:
            return await self.db_manager.bulk_insert('core.company_contacts', contact_records)
        
        return 0
    
    async def _load_industry_classifications(self) -> int:
        """Load industry classification data."""
        # Clear existing classifications
        await self.db_manager.execute("""
            DELETE FROM core.industry_classifications 
            WHERE company_id IN (
                SELECT company_id FROM staging.transformed_companies 
                WHERE processing_status = 'ready_for_load'
            )
        """)
        
        companies = await self._get_transformed_companies()
        
        classification_records = []
        for company in companies:
            company_id = company['company_id']
            industry = company.get('industry')
            
            if industry:
                # Map industry to ANZSIC-like classification
                industry_code = self._map_industry_to_code(industry)
                
                classification_records.append({
                    'company_id': company_id,
                    'classification_system': 'ANZSIC_MAPPED',
                    'code': industry_code,
                    'description': industry,
                    'level': 1,  # Division level
                    'confidence_score': 0.8,  # Moderate confidence for LLM-derived
                    'created_at': datetime.now()
                })
        
        if classification_records:
            return await self.db_manager.bulk_insert('core.industry_classifications', classification_records)
        
        return 0
    
    def _map_industry_to_code(self, industry: str) -> str:
        """Map industry description to a simplified ANZSIC-like code."""
        # Simplified mapping - in production you'd use full ANZSIC codes
        industry_mapping = {
            'Manufacturing': 'C',
            'Construction': 'E',
            'Professional Services': 'M',
            'Technology': 'J',
            'Retail Trade': 'G',
            'Financial Services': 'K',
            'Healthcare': 'Q',
            'Education': 'P',
            'Transport & Logistics': 'I',
            'Agriculture': 'A',
            'Mining': 'B'
        }
        
        return industry_mapping.get(industry, 'Z')  # Z for Other Services
    
    async def _load_data_lineage(self) -> int:
        """Load data lineage information."""
        companies = await self._get_transformed_companies()
        
        lineage_records = []
        for company in companies:
            company_id = company['company_id']
            data_sources = company.get('data_source', [])
            matching_metadata = company.get('matching_metadata', {})
            
            for source in data_sources:
                lineage_records.append({
                    'company_id': company_id,
                    'source_system': source,
                    'source_record_id': None,  # Could be enhanced to track specific record IDs
                    'contribution_fields': ['all'],  # Simplified - could track specific fields
                    'extraction_date': datetime.now(),
                    'confidence_score': matching_metadata.get('llm_confidence', 0.5),
                    'created_at': datetime.now()
                })
        
        if lineage_records:
            return await self.db_manager.bulk_insert('core.data_lineage', lineage_records)
        
        return 0
    
    async def update_analytics_tables(self):
        """Update analytics tables with current data."""
        logger.info("Updating analytics tables")
        
        # Update companies by state
        await self._update_companies_by_state()
        
        # Update industry distribution
        await self._update_industry_distribution()
        
        logger.info("Analytics tables updated")
    
    async def _update_companies_by_state(self):
        """Update the companies by state analytics table."""
        query = """
        INSERT INTO analytics.companies_by_state (
            state, total_companies, active_companies, 
            gst_registered_companies, avg_data_quality_score, last_updated
        )
        SELECT 
            address_state,
            COUNT(*) as total_companies,
            COUNT(*) FILTER (WHERE is_active = true) as active_companies,
            COUNT(*) FILTER (WHERE gst_registered = true) as gst_registered_companies,
            AVG(data_quality_score) as avg_data_quality_score,
            CURRENT_TIMESTAMP as last_updated
        FROM core.companies 
        WHERE address_state IS NOT NULL
        GROUP BY address_state
        ON CONFLICT (state) DO UPDATE SET
            total_companies = EXCLUDED.total_companies,
            active_companies = EXCLUDED.active_companies,
            gst_registered_companies = EXCLUDED.gst_registered_companies,
            avg_data_quality_score = EXCLUDED.avg_data_quality_score,
            last_updated = EXCLUDED.last_updated
        """
        
        await self.db_manager.execute(query)
    
    async def _update_industry_distribution(self):
        """Update the industry distribution analytics table."""
        # First calculate totals
        total_query = "SELECT COUNT(*) as total FROM core.companies WHERE industry IS NOT NULL"
        total_result = await self.db_manager.fetch_one(total_query)
        total_companies = total_result['total'] if total_result else 1
        
        query = """
        INSERT INTO analytics.industry_distribution (
            industry_code, industry_description, company_count, 
            percentage_of_total, avg_data_quality_score, last_updated
        )
        SELECT 
            ic.code as industry_code,
            c.industry as industry_description,
            COUNT(*) as company_count,
            (COUNT(*) * 100.0 / $1) as percentage_of_total,
            AVG(c.data_quality_score) as avg_data_quality_score,
            CURRENT_TIMESTAMP as last_updated
        FROM core.companies c
        LEFT JOIN core.industry_classifications ic ON c.company_id = ic.company_id
        WHERE c.industry IS NOT NULL
        GROUP BY ic.code, c.industry
        ON CONFLICT (industry_code) DO UPDATE SET
            company_count = EXCLUDED.company_count,
            percentage_of_total = EXCLUDED.percentage_of_total,
            avg_data_quality_score = EXCLUDED.avg_data_quality_score,
            last_updated = EXCLUDED.last_updated
        """
        
        await self.db_manager.execute(query, {'total': total_companies})
    
    async def cleanup_staging_data(self):
        """Clean up staging data after successful load."""
        logger.info("Cleaning up staging data")
        
        # Mark processed records
        await self.db_manager.execute("""
            UPDATE staging.transformed_companies 
            SET processing_status = 'loaded', updated_at = CURRENT_TIMESTAMP
            WHERE processing_status = 'ready_for_load'
        """)
        
        # Optionally archive old staging data
        cutoff_date = datetime.now().replace(day=1)  # Keep current month
        
        tables_to_cleanup = [
            'staging.common_crawl_raw',
            'staging.abr_raw',
            'staging.entity_matching_candidates'
        ]
        
        for table in tables_to_cleanup:
            cleanup_query = f"""
            DELETE FROM {table} 
            WHERE created_at < $1
            """
            try:
                await self.db_manager.execute(cleanup_query, {'cutoff': cutoff_date})
                logger.info(f"Cleaned up old data from {table}")
            except Exception as e:
                logger.warning(f"Failed to cleanup {table}: {e}")
    
    async def get_load_statistics(self) -> Dict[str, Any]:
        """Get statistics about loaded data."""
        stats = {}
        
        # Company counts
        company_stats_query = """
        SELECT 
            COUNT(*) as total_companies,
            COUNT(*) FILTER (WHERE is_active = true) as active_companies,
            COUNT(*) FILTER (WHERE abn IS NOT NULL) as companies_with_abn,
            COUNT(*) FILTER (WHERE website_url IS NOT NULL) as companies_with_website,
            AVG(data_quality_score) as avg_quality_score
        FROM core.companies
        """
        company_stats = await self.db_manager.fetch_one(company_stats_query)
        stats.update(company_stats)
        
        # Alternative names count
        names_query = "SELECT COUNT(*) as alternative_names FROM core.company_names"
        names_result = await self.db_manager.fetch_one(names_query)
        stats['alternative_names'] = names_result['alternative_names']
        
        # Contact information count
        contacts_query = "SELECT COUNT(*) as contact_records FROM core.company_contacts"
        contacts_result = await self.db_manager.fetch_one(contacts_query)
        stats['contact_records'] = contacts_result['contact_records']
        
        # Industry classifications count
        industries_query = "SELECT COUNT(*) as industry_classifications FROM core.industry_classifications"
        industries_result = await self.db_manager.fetch_one(industries_query)
        stats['industry_classifications'] = industries_result['industry_classifications']
        
        return stats


if __name__ == "__main__":
    import asyncio
    from ..utils.config import Config
    
    async def main():
        config = Config()
        db_manager = DatabaseManager(config.database_url)
        
        loader = CoreDataLoader(db_manager)
        
        # Test loading
        companies_loaded = await loader.load_companies()
        print(f"Loaded {companies_loaded} companies")
        
        related_loaded = await loader.load_related_data()
        print(f"Loaded {related_loaded} related records")
        
        # Get statistics
        stats = await loader.get_load_statistics()
        print(f"Load statistics: {json.dumps(stats, indent=2, default=str)}")
        
        await db_manager.close()
    
    asyncio.run(main())