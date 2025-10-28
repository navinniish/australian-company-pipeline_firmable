"""
Data transformation module for cleaning and merging company data.
"""

import asyncio
import logging
from typing import List, Dict, Optional, Any, Tuple
import json
import re
from datetime import datetime
import uuid

from ..utils.database import DatabaseManager
from ..utils.llm_client import LLMClient
from ..utils.text_processing import (
    normalize_company_name, extract_company_info, 
    validate_abn, standardize_address, extract_industry_keywords
)

logger = logging.getLogger(__name__)


class DataTransformer:
    """
    Transforms and cleans matched entity data for loading into core tables.
    Handles data quality, normalization, and intelligent merging.
    """
    
    def __init__(self, db_manager: DatabaseManager, llm_client: LLMClient):
        self.db_manager = db_manager
        self.llm_client = llm_client
    
    async def transform_matched_entities(self) -> List[Dict]:
        """
        Transform matched entities from staging into cleaned company records.
        
        Returns:
            List of transformed company records
        """
        logger.info("Starting entity transformation")
        
        # Get matched entities with their source data
        matches = await self._get_entity_matches()
        
        transformed_companies = []
        
        for match in matches:
            try:
                company_record = await self._merge_entity_data(match)
                if company_record:
                    transformed_companies.append(company_record)
            except Exception as e:
                logger.warning(f"Failed to transform match {match['id']}: {e}")
                continue
        
        logger.info(f"Transformed {len(transformed_companies)} company records")
        return transformed_companies
    
    async def _get_entity_matches(self) -> List[Dict]:
        """Get entity matches with source data."""
        query = """
        SELECT 
            em.id,
            em.common_crawl_id,
            em.abr_id,
            em.similarity_score,
            em.llm_confidence,
            em.llm_reasoning,
            em.manual_review_required,
            
            -- Common Crawl data
            cc.website_url,
            cc.company_name as cc_company_name,
            cc.industry as cc_industry,
            cc.meta_description,
            cc.title,
            cc.contact_info as cc_contact_info,
            cc.social_links,
            cc.extraction_confidence,
            
            -- ABR data
            abr.abn,
            abr.entity_name as abr_entity_name,
            abr.entity_type,
            abr.entity_status,
            abr.address_line_1,
            abr.address_line_2,
            abr.address_suburb,
            abr.address_state,
            abr.address_postcode,
            abr.start_date,
            abr.gst_status,
            abr.dgr_status,
            abr.acn,
            abr.trading_names,
            abr.business_names
            
        FROM staging.entity_matching_candidates em
        LEFT JOIN staging.common_crawl_raw cc ON em.common_crawl_id = cc.id
        LEFT JOIN staging.abr_raw abr ON em.abr_id = abr.id
        WHERE em.llm_confidence >= 0.4  -- Only process reasonable confidence matches
        ORDER BY em.llm_confidence DESC, em.similarity_score DESC
        """
        
        return await self.db_manager.fetch_all(query)
    
    async def _merge_entity_data(self, match: Dict) -> Optional[Dict]:
        """
        Merge data from Common Crawl and ABR sources into a unified company record.
        
        Args:
            match: Dictionary containing matched entity data
            
        Returns:
            Merged company record
        """
        company_id = str(uuid.uuid4())
        
        # Determine best company name using LLM assistance
        company_name = await self._determine_best_name(match)
        
        # Merge address information
        address_info = self._merge_address_data(match)
        
        # Extract and merge contact information
        contact_info = self._merge_contact_data(match)
        
        # Determine industry
        industry = await self._determine_industry(match)
        
        # Calculate data quality score
        quality_score = self._calculate_quality_score(match)
        
        # Create merged record
        company_record = {
            'company_id': company_id,
            'abn': match.get('abn'),
            'company_name': company_name,
            'normalized_name': normalize_company_name(company_name),
            'website_url': match.get('website_url'),
            'entity_type': match.get('entity_type'),
            'entity_status': match.get('entity_status'),
            'industry': industry,
            'address_line_1': address_info.get('line_1'),
            'address_line_2': address_info.get('line_2'),
            'address_suburb': address_info.get('suburb'),
            'address_state': address_info.get('state'),
            'address_postcode': address_info.get('postcode'),
            'start_date': match.get('start_date'),
            'gst_registered': self._parse_gst_status(match.get('gst_status')),
            'dgr_endorsed': self._parse_dgr_status(match.get('dgr_status')),
            'is_active': self._determine_active_status(match),
            'data_quality_score': quality_score,
            'data_source': self._get_data_sources(match),
            'created_at': datetime.now(),
            'updated_at': datetime.now(),
            
            # Additional data for related tables
            'alternative_names': self._get_alternative_names(match),
            'contact_details': contact_info,
            'social_links': self._parse_social_links(match.get('social_links')),
            'matching_metadata': {
                'similarity_score': match.get('similarity_score'),
                'llm_confidence': match.get('llm_confidence'),
                'manual_review_required': match.get('manual_review_required')
            }
        }
        
        return company_record
    
    async def _determine_best_name(self, match: Dict) -> str:
        """
        Use LLM to determine the best company name from available sources.
        
        Args:
            match: Match data containing name options
            
        Returns:
            Best company name
        """
        cc_name = match.get('cc_company_name', '')
        abr_name = match.get('abr_entity_name', '')
        trading_names = match.get('trading_names', []) or []
        
        # If names are very similar, use the more complete one
        if cc_name and abr_name:
            similarity = self._calculate_name_similarity(cc_name, abr_name)
            if similarity > 0.9:
                return abr_name if len(abr_name) > len(cc_name) else cc_name
        
        # Use LLM for complex cases
        if cc_name and abr_name and len(trading_names) > 0:
            prompt = f"""
            Determine the best official company name from these options:
            
            Website name: {cc_name}
            ABR registered name: {abr_name}
            Trading names: {', '.join(trading_names)}
            
            Return only the best official company name (no explanation needed).
            Prefer the most official/legal name that would be used in business registration.
            """
            
            try:
                best_name = await self.llm_client.chat_completion(prompt)
                return best_name.strip().strip('"\'')
            except:
                pass
        
        # Fallback logic
        return abr_name if abr_name else cc_name if cc_name else 'Unknown Company'
    
    def _merge_address_data(self, match: Dict) -> Dict[str, Optional[str]]:
        """Merge and standardize address information."""
        address = {
            'line_1': match.get('address_line_1'),
            'line_2': match.get('address_line_2'),
            'suburb': match.get('address_suburb'),
            'state': match.get('address_state'),
            'postcode': match.get('address_postcode')
        }
        
        # Standardize address components
        if address['line_1']:
            address['line_1'] = standardize_address(address['line_1'])
        
        return address
    
    def _merge_contact_data(self, match: Dict) -> Dict:
        """Extract and merge contact information from all sources."""
        contacts = {
            'emails': [],
            'phones': [],
            'website': match.get('website_url')
        }
        
        # Extract from Common Crawl contact info
        cc_contact_info = match.get('cc_contact_info')
        if cc_contact_info:
            if isinstance(cc_contact_info, str):
                try:
                    cc_contact_info = json.loads(cc_contact_info)
                except:
                    cc_contact_info = {}
            
            if isinstance(cc_contact_info, dict):
                if cc_contact_info.get('email'):
                    contacts['emails'].append(cc_contact_info['email'])
                if cc_contact_info.get('phone'):
                    contacts['phones'].append(cc_contact_info['phone'])
        
        # Deduplicate
        contacts['emails'] = list(set(contacts['emails']))
        contacts['phones'] = list(set(contacts['phones']))
        
        return contacts
    
    async def _determine_industry(self, match: Dict) -> Optional[str]:
        """Determine the most appropriate industry classification."""
        cc_industry = match.get('cc_industry', '')
        entity_type = match.get('entity_type', '')
        meta_description = match.get('meta_description', '')
        
        # Extract keywords from various sources
        text_for_analysis = f"{cc_industry} {meta_description} {entity_type}".strip()
        
        if not text_for_analysis:
            return None
        
        # Use keyword extraction first
        keywords = extract_industry_keywords(text_for_analysis)
        if keywords:
            # Map keywords to standard industry categories
            industry_mapping = {
                'manufacturing': 'Manufacturing',
                'construction': 'Construction',
                'technology': 'Technology',
                'professional': 'Professional Services',
                'retail': 'Retail Trade',
                'finance': 'Financial Services',
                'healthcare': 'Healthcare',
                'education': 'Education',
                'transport': 'Transport & Logistics'
            }
            
            for keyword in keywords:
                if keyword in industry_mapping:
                    return industry_mapping[keyword]
        
        # Use LLM for complex cases
        if len(text_for_analysis) > 20:
            prompt = f"""
            Classify this business into one of these Australian industry categories:
            - Manufacturing
            - Construction  
            - Professional Services
            - Technology
            - Retail Trade
            - Financial Services
            - Healthcare
            - Education
            - Transport & Logistics
            - Agriculture
            - Mining
            - Other
            
            Business information: {text_for_analysis[:500]}
            
            Return only the category name.
            """
            
            try:
                industry = await self.llm_client.chat_completion(prompt)
                return industry.strip()
            except:
                pass
        
        return cc_industry if cc_industry else None
    
    def _calculate_quality_score(self, match: Dict) -> float:
        """Calculate data quality score (0.0 to 1.0)."""
        score = 0.0
        
        # Core data presence (40% weight)
        if match.get('abn'):
            score += 0.15
        if match.get('cc_company_name') or match.get('abr_entity_name'):
            score += 0.15
        if match.get('website_url'):
            score += 0.10
        
        # Address completeness (25% weight)
        address_fields = ['address_line_1', 'address_suburb', 'address_state', 'address_postcode']
        address_completeness = sum(1 for field in address_fields if match.get(field)) / len(address_fields)
        score += address_completeness * 0.25
        
        # Contact information (15% weight)
        if match.get('cc_contact_info'):
            score += 0.15
        
        # Data source confidence (20% weight)
        extraction_confidence = match.get('extraction_confidence', 0.0)
        llm_confidence = match.get('llm_confidence', 0.0)
        avg_confidence = (extraction_confidence + llm_confidence) / 2
        score += avg_confidence * 0.20
        
        return min(score, 1.0)
    
    def _get_data_sources(self, match: Dict) -> List[str]:
        """Identify which data sources contributed to this record."""
        sources = []
        if match.get('common_crawl_id'):
            sources.append('common_crawl')
        if match.get('abr_id'):
            sources.append('abr')
        return sources
    
    def _get_alternative_names(self, match: Dict) -> List[Dict]:
        """Get alternative names for the company."""
        names = []
        
        cc_name = match.get('cc_company_name')
        abr_name = match.get('abr_entity_name')
        trading_names = match.get('trading_names', []) or []
        business_names = match.get('business_names', []) or []
        
        # Add names with types
        if cc_name:
            names.append({'name': cc_name, 'type': 'website'})
        if abr_name:
            names.append({'name': abr_name, 'type': 'legal'})
        
        for name in trading_names:
            if name:
                names.append({'name': name, 'type': 'trading'})
        
        for name in business_names:
            if name:
                names.append({'name': name, 'type': 'business'})
        
        # Deduplicate by normalized name
        seen_names = set()
        unique_names = []
        for name_info in names:
            normalized = normalize_company_name(name_info['name'])
            if normalized and normalized not in seen_names:
                seen_names.add(normalized)
                unique_names.append(name_info)
        
        return unique_names
    
    def _parse_social_links(self, social_links_json: Optional[str]) -> Dict:
        """Parse social links JSON."""
        if not social_links_json:
            return {}
        
        try:
            if isinstance(social_links_json, str):
                return json.loads(social_links_json)
            return social_links_json
        except:
            return {}
    
    def _parse_gst_status(self, gst_status: Optional[str]) -> Optional[bool]:
        """Parse GST registration status."""
        if not gst_status:
            return None
        
        gst_lower = gst_status.lower()
        if 'registered' in gst_lower or 'active' in gst_lower:
            return True
        elif 'not registered' in gst_lower or 'inactive' in gst_lower:
            return False
        
        return None
    
    def _parse_dgr_status(self, dgr_status: Optional[str]) -> Optional[bool]:
        """Parse DGR (Deductible Gift Recipient) status."""
        if not dgr_status:
            return None
        
        dgr_lower = dgr_status.lower()
        if 'endorsed' in dgr_lower or 'active' in dgr_lower:
            return True
        elif 'not endorsed' in dgr_lower or 'inactive' in dgr_lower:
            return False
        
        return None
    
    def _determine_active_status(self, match: Dict) -> bool:
        """Determine if company is currently active."""
        entity_status = match.get('entity_status', '').lower()
        
        # Active if ABR status is active
        if 'active' in entity_status:
            return True
        
        # Inactive if explicitly marked
        if any(term in entity_status for term in ['inactive', 'cancelled', 'deregistered']):
            return False
        
        # Default to active if we have recent web presence
        if match.get('website_url') and match.get('extraction_confidence', 0) > 0.5:
            return True
        
        return True  # Default to active
    
    def _calculate_name_similarity(self, name1: str, name2: str) -> float:
        """Calculate similarity between two company names."""
        if not name1 or not name2:
            return 0.0
        
        norm1 = normalize_company_name(name1)
        norm2 = normalize_company_name(name2)
        
        if norm1 == norm2:
            return 1.0
        
        # Simple token-based similarity
        tokens1 = set(norm1.split())
        tokens2 = set(norm2.split())
        
        if not tokens1 or not tokens2:
            return 0.0
        
        intersection = tokens1.intersection(tokens2)
        union = tokens1.union(tokens2)
        
        return len(intersection) / len(union)
    
    async def clean_and_validate(self, records: List[Dict]) -> List[Dict]:
        """
        Clean and validate transformed records.
        
        Args:
            records: List of company records to clean
            
        Returns:
            List of cleaned and validated records
        """
        logger.info(f"Cleaning and validating {len(records)} records")
        
        cleaned_records = []
        
        for record in records:
            try:
                cleaned_record = await self._clean_single_record(record)
                if self._validate_record(cleaned_record):
                    cleaned_records.append(cleaned_record)
                else:
                    logger.warning(f"Record validation failed for company: {record.get('company_name')}")
            except Exception as e:
                logger.warning(f"Failed to clean record: {e}")
                continue
        
        logger.info(f"Successfully cleaned {len(cleaned_records)} records")
        return cleaned_records
    
    async def _clean_single_record(self, record: Dict) -> Dict:
        """Clean and standardize a single company record."""
        cleaned = record.copy()
        
        # Clean text fields
        text_fields = ['company_name', 'address_line_1', 'address_line_2', 'address_suburb']
        for field in text_fields:
            if cleaned.get(field):
                cleaned[field] = self._clean_text_field(cleaned[field])
        
        # Validate and clean ABN
        if cleaned.get('abn'):
            abn = re.sub(r'\s', '', cleaned['abn'])
            if validate_abn(abn):
                cleaned['abn'] = abn
            else:
                logger.warning(f"Invalid ABN removed: {cleaned['abn']}")
                cleaned['abn'] = None
        
        # Clean postcode
        if cleaned.get('address_postcode'):
            postcode = re.sub(r'\D', '', str(cleaned['address_postcode']))
            if len(postcode) == 4:
                cleaned['address_postcode'] = postcode
            else:
                cleaned['address_postcode'] = None
        
        # Clean website URL
        if cleaned.get('website_url'):
            url = cleaned['website_url'].strip()
            if not url.startswith(('http://', 'https://')):
                url = 'https://' + url
            cleaned['website_url'] = url
        
        return cleaned
    
    def _clean_text_field(self, text: str) -> str:
        """Clean and standardize text field."""
        if not text:
            return text
        
        # Remove extra whitespace
        cleaned = re.sub(r'\s+', ' ', text.strip())
        
        # Remove control characters
        cleaned = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', cleaned)
        
        return cleaned
    
    def _validate_record(self, record: Dict) -> bool:
        """Validate that a record meets minimum quality requirements."""
        # Must have company name
        if not record.get('company_name') or len(record['company_name'].strip()) < 2:
            return False
        
        # Must have either ABN or website
        if not record.get('abn') and not record.get('website_url'):
            return False
        
        # Data quality score must be above minimum threshold
        if record.get('data_quality_score', 0) < 0.3:
            return False
        
        return True


if __name__ == "__main__":
    import asyncio
    from ..utils.config import Config
    
    async def main():
        config = Config()
        db_manager = DatabaseManager(config.database_url)
        llm_client = LLMClient(config)
        
        transformer = DataTransformer(db_manager, llm_client)
        
        # Test transformation
        records = await transformer.transform_matched_entities()
        print(f"Transformed {len(records)} records")
        
        if records:
            print("Sample record:")
            print(json.dumps(records[0], indent=2, default=str))
    
    asyncio.run(main())