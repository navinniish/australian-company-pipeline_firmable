"""
Advanced entity matching system using LLMs for improved accuracy.
Matches company records from Common Crawl and ABR data sources.
"""

import asyncio
import logging
from typing import List, Dict, Optional, Tuple, Any
from dataclasses import dataclass
import json
import re
from difflib import SequenceMatcher
from sentence_transformers import SentenceTransformer
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity

from ..utils.llm_client import LLMClient
from ..utils.database import DatabaseManager
from ..utils.text_processing import normalize_company_name

logger = logging.getLogger(__name__)

@dataclass
class EntityMatch:
    """Data structure for entity matching results."""
    common_crawl_id: int
    abr_id: int
    similarity_score: float
    matching_method: str
    llm_confidence: float
    llm_reasoning: str
    manual_review_required: bool


class LLMEntityMatcher:
    """
    Advanced entity matching system that uses multiple techniques including LLMs
    to achieve high-accuracy matching between Common Crawl and ABR datasets.
    """
    
    def __init__(self, llm_client: LLMClient, db_manager: DatabaseManager):
        self.llm_client = llm_client
        self.db_manager = db_manager
        
        # Load sentence transformer for semantic similarity
        self.sentence_model = SentenceTransformer('all-MiniLM-L6-v2')
        
        # Matching thresholds
        self.exact_match_threshold = 0.95
        self.high_confidence_threshold = 0.85
        self.llm_review_threshold = 0.60
        self.manual_review_threshold = 0.40
    
    async def match_entities(self, batch_size: int = 1000) -> List[EntityMatch]:
        """
        Main method to match entities between Common Crawl and ABR datasets.
        
        Args:
            batch_size: Number of records to process in each batch
            
        Returns:
            List of EntityMatch objects
        """
        logger.info("Starting entity matching process")
        
        # Get all records from staging tables
        cc_records = await self._get_common_crawl_records()
        abr_records = await self._get_abr_records()
        
        logger.info(f"Loaded {len(cc_records)} Common Crawl and {len(abr_records)} ABR records")
        
        matches = []
        
        # Process in batches to manage memory
        for i in range(0, len(cc_records), batch_size):
            cc_batch = cc_records[i:i + batch_size]
            batch_matches = await self._process_batch(cc_batch, abr_records)
            matches.extend(batch_matches)
            
            logger.info(f"Processed {i + len(cc_batch)}/{len(cc_records)} Common Crawl records. Found {len(batch_matches)} matches.")
            
            # Save progress
            if batch_matches:
                await self._save_matches_to_staging(batch_matches)
        
        logger.info(f"Entity matching complete. Total matches: {len(matches)}")
        return matches
    
    async def _get_common_crawl_records(self) -> List[Dict]:
        """Retrieve Common Crawl records from staging."""
        query = """
        SELECT id, website_url, company_name, industry, meta_description, title, extraction_confidence
        FROM staging.common_crawl_raw 
        WHERE company_name IS NOT NULL 
        AND extraction_confidence >= 0.3
        ORDER BY extraction_confidence DESC
        """
        return await self.db_manager.fetch_all(query)
    
    async def _get_abr_records(self) -> List[Dict]:
        """Retrieve ABR records from staging."""
        query = """
        SELECT id, abn, entity_name, entity_status, address_state, address_suburb, 
               address_postcode, trading_names, business_names
        FROM staging.abr_raw 
        WHERE entity_status_code = 'Active'
        ORDER BY entity_name
        """
        return await self.db_manager.fetch_all(query)
    
    async def _process_batch(self, cc_batch: List[Dict], abr_records: List[Dict]) -> List[EntityMatch]:
        """Process a batch of Common Crawl records against all ABR records."""
        batch_matches = []
        
        for cc_record in cc_batch:
            best_matches = await self._find_best_matches(cc_record, abr_records)
            
            for match in best_matches:
                if match.similarity_score >= self.manual_review_threshold:
                    batch_matches.append(match)
        
        return batch_matches
    
    async def _find_best_matches(self, cc_record: Dict, abr_records: List[Dict]) -> List[EntityMatch]:
        """
        Find the best matching ABR records for a given Common Crawl record.
        Uses multiple matching techniques and LLM for final decision.
        """
        # Step 1: Rule-based filtering for potential matches
        candidates = self._filter_candidates(cc_record, abr_records)
        
        if not candidates:
            return []
        
        # Step 2: Calculate similarity scores using multiple methods
        scored_candidates = []
        for abr_record in candidates[:50]:  # Limit to top 50 candidates for efficiency
            score = await self._calculate_similarity(cc_record, abr_record)
            if score >= self.manual_review_threshold:
                scored_candidates.append((abr_record, score))
        
        # Sort by similarity score
        scored_candidates.sort(key=lambda x: x[1], reverse=True)
        
        # Step 3: Use LLM for top candidates requiring review
        matches = []
        for abr_record, similarity_score in scored_candidates[:5]:  # Review top 5 candidates
            if similarity_score >= self.llm_review_threshold:
                llm_result = await self._llm_verify_match(cc_record, abr_record, similarity_score)
                
                match = EntityMatch(
                    common_crawl_id=cc_record['id'],
                    abr_id=abr_record['id'],
                    similarity_score=similarity_score,
                    matching_method='hybrid_llm',
                    llm_confidence=llm_result['confidence'],
                    llm_reasoning=llm_result['reasoning'],
                    manual_review_required=llm_result['confidence'] < self.high_confidence_threshold
                )
                
                if llm_result['is_match']:
                    matches.append(match)
                    break  # Take the first confirmed match
        
        return matches
    
    def _filter_candidates(self, cc_record: Dict, abr_records: List[Dict]) -> List[Dict]:
        """
        Filter ABR records to potential candidates using rule-based matching.
        
        Args:
            cc_record: Common Crawl record
            abr_records: List of ABR records
            
        Returns:
            List of potential ABR candidates
        """
        candidates = []
        cc_url = cc_record.get('website_url', '').lower()
        cc_name = normalize_company_name(cc_record.get('company_name', ''))
        
        # Extract domain for URL-based matching
        domain = self._extract_domain(cc_url)
        
        for abr_record in abr_records:
            # Skip inactive entities
            if abr_record.get('entity_status') != 'Active':
                continue
            
            abr_name = normalize_company_name(abr_record.get('entity_name', ''))
            trading_names = abr_record.get('trading_names', []) or []
            business_names = abr_record.get('business_names', []) or []
            
            # Check various name matches
            all_names = [abr_name] + [normalize_company_name(name) for name in trading_names + business_names]
            
            # Rule 1: Domain-based filtering (if domain contains company name components)
            if domain and any(self._domain_name_similarity(domain, name) for name in all_names):
                candidates.append(abr_record)
                continue
            
            # Rule 2: Direct name similarity
            for name in all_names:
                if self._quick_name_similarity(cc_name, name) >= 0.7:
                    candidates.append(abr_record)
                    break
        
        return candidates
    
    def _extract_domain(self, url: str) -> Optional[str]:
        """Extract clean domain name from URL."""
        try:
            from urllib.parse import urlparse
            parsed = urlparse(url)
            domain = parsed.netloc.lower()
            # Remove www. and common prefixes
            domain = re.sub(r'^www\.', '', domain)
            # Remove .com.au, .net.au etc.
            domain = re.sub(r'\.(com|net|org|edu|gov|asn)\.au$', '', domain)
            return domain
        except:
            return None
    
    def _domain_name_similarity(self, domain: str, company_name: str) -> bool:
        """Check if domain name is similar to company name."""
        if not domain or not company_name:
            return False
        
        # Remove common business suffixes
        clean_name = re.sub(r'\b(pty|ltd|limited|company|corp|corporation|inc|incorporated)\b', '', company_name.lower())
        clean_name = re.sub(r'[^a-z0-9]', '', clean_name)
        
        # Check if domain contains significant portion of company name
        return len(clean_name) >= 4 and clean_name in domain
    
    def _quick_name_similarity(self, name1: str, name2: str) -> float:
        """Quick similarity check using sequence matcher."""
        if not name1 or not name2:
            return 0.0
        return SequenceMatcher(None, name1.lower(), name2.lower()).ratio()
    
    async def _calculate_similarity(self, cc_record: Dict, abr_record: Dict) -> float:
        """
        Calculate comprehensive similarity score between two records.
        
        Args:
            cc_record: Common Crawl record
            abr_record: ABR record
            
        Returns:
            Overall similarity score (0.0 to 1.0)
        """
        scores = []
        
        # 1. Name similarity (weighted 50%)
        cc_name = normalize_company_name(cc_record.get('company_name', ''))
        abr_name = normalize_company_name(abr_record.get('entity_name', ''))
        
        name_similarity = self._calculate_name_similarity(cc_name, abr_name)
        
        # Also check against trading names
        trading_names = abr_record.get('trading_names', []) or []
        business_names = abr_record.get('business_names', []) or []
        
        max_alt_name_sim = 0.0
        for alt_name in trading_names + business_names:
            if alt_name:
                alt_sim = self._calculate_name_similarity(cc_name, normalize_company_name(alt_name))
                max_alt_name_sim = max(max_alt_name_sim, alt_sim)
        
        final_name_similarity = max(name_similarity, max_alt_name_sim)
        scores.append(('name', final_name_similarity, 0.5))
        
        # 2. Semantic similarity using embeddings (weighted 20%)
        semantic_sim = await self._calculate_semantic_similarity(cc_record, abr_record)
        scores.append(('semantic', semantic_sim, 0.2))
        
        # 3. Location similarity (weighted 15%)
        location_sim = self._calculate_location_similarity(cc_record, abr_record)
        scores.append(('location', location_sim, 0.15))
        
        # 4. Industry similarity (weighted 15%)
        industry_sim = self._calculate_industry_similarity(cc_record, abr_record)
        scores.append(('industry', industry_sim, 0.15))
        
        # Calculate weighted average
        total_weighted_score = sum(score * weight for _, score, weight in scores)
        
        return min(total_weighted_score, 1.0)
    
    def _calculate_name_similarity(self, name1: str, name2: str) -> float:
        """Calculate sophisticated name similarity."""
        if not name1 or not name2:
            return 0.0
        
        # Multiple similarity measures
        sequence_sim = SequenceMatcher(None, name1.lower(), name2.lower()).ratio()
        
        # Token-based similarity
        tokens1 = set(re.findall(r'\w+', name1.lower()))
        tokens2 = set(re.findall(r'\w+', name2.lower()))
        
        if tokens1 and tokens2:
            jaccard_sim = len(tokens1.intersection(tokens2)) / len(tokens1.union(tokens2))
        else:
            jaccard_sim = 0.0
        
        # Return the maximum of both measures
        return max(sequence_sim, jaccard_sim)
    
    async def _calculate_semantic_similarity(self, cc_record: Dict, abr_record: Dict) -> float:
        """Calculate semantic similarity using sentence embeddings."""
        try:
            # Create text representations
            cc_text = f"{cc_record.get('company_name', '')} {cc_record.get('meta_description', '')} {cc_record.get('industry', '')}"
            abr_text = f"{abr_record.get('entity_name', '')} {' '.join(abr_record.get('trading_names', []) or [])}"
            
            if not cc_text.strip() or not abr_text.strip():
                return 0.0
            
            # Generate embeddings
            embeddings = self.sentence_model.encode([cc_text, abr_text])
            
            # Calculate cosine similarity
            similarity = cosine_similarity([embeddings[0]], [embeddings[1]])[0][0]
            return max(0.0, similarity)  # Ensure non-negative
            
        except Exception as e:
            logger.warning(f"Error calculating semantic similarity: {e}")
            return 0.0
    
    def _calculate_location_similarity(self, cc_record: Dict, abr_record: Dict) -> float:
        """Calculate location-based similarity (if available)."""
        # This is a simplified implementation - could be enhanced with geocoding
        # For now, we don't have reliable location data from Common Crawl
        return 0.5  # Neutral score
    
    def _calculate_industry_similarity(self, cc_record: Dict, abr_record: Dict) -> float:
        """Calculate industry similarity."""
        cc_industry = cc_record.get('industry', '').lower()
        
        # ABR doesn't have direct industry field, so return neutral
        # Could be enhanced by mapping entity types to industries
        return 0.5 if cc_industry else 0.0
    
    async def _llm_verify_match(self, cc_record: Dict, abr_record: Dict, similarity_score: float) -> Dict:
        """
        Use LLM to verify and provide reasoning for potential matches.
        
        Args:
            cc_record: Common Crawl record
            abr_record: ABR record  
            similarity_score: Calculated similarity score
            
        Returns:
            Dictionary with LLM verification results
        """
        prompt = f"""
        You are an expert in entity matching for Australian business data. You need to determine if these two records represent the same company.

        COMMON CRAWL RECORD:
        - Website URL: {cc_record.get('website_url', 'N/A')}
        - Company Name: {cc_record.get('company_name', 'N/A')}
        - Industry: {cc_record.get('industry', 'N/A')}
        - Meta Description: {cc_record.get('meta_description', 'N/A')[:200]}
        - Page Title: {cc_record.get('title', 'N/A')[:100]}

        ABR RECORD:
        - ABN: {abr_record.get('abn', 'N/A')}
        - Entity Name: {abr_record.get('entity_name', 'N/A')}
        - Trading Names: {', '.join(abr_record.get('trading_names', []) or [])}
        - Business Names: {', '.join(abr_record.get('business_names', []) or [])}
        - Location: {abr_record.get('address_suburb', 'N/A')}, {abr_record.get('address_state', 'N/A')} {abr_record.get('address_postcode', 'N/A')}
        - Entity Status: {abr_record.get('entity_status', 'N/A')}

        Calculated Similarity Score: {similarity_score:.3f}

        Please analyze and return your response as JSON:
        {{
            "is_match": true/false,
            "confidence": 0.0-1.0,
            "reasoning": "Detailed explanation of your decision",
            "key_factors": ["list", "of", "key", "matching", "factors"]
        }}

        Consider:
        - Name variations (legal name vs trading name vs abbreviations)
        - Domain name alignment with business name
        - Industry consistency
        - Any obvious contradictions
        - Australian business naming conventions
        
        Be conservative - only mark as match if you're reasonably confident.
        """
        
        try:
            response = await self.llm_client.chat_completion(prompt)
            result = json.loads(response)
            
            # Validate response structure
            required_fields = ['is_match', 'confidence', 'reasoning']
            if not all(field in result for field in required_fields):
                raise ValueError("Missing required fields in LLM response")
            
            # Ensure confidence is within valid range
            result['confidence'] = max(0.0, min(1.0, float(result['confidence'])))
            
            return result
            
        except Exception as e:
            logger.error(f"LLM verification failed: {e}")
            return {
                'is_match': False,
                'confidence': 0.0,
                'reasoning': f'LLM verification failed: {str(e)}',
                'key_factors': []
            }
    
    async def _save_matches_to_staging(self, matches: List[EntityMatch]):
        """Save entity matches to staging table."""
        if not matches:
            return
        
        records = []
        for match in matches:
            records.append({
                'common_crawl_id': match.common_crawl_id,
                'abr_id': match.abr_id,
                'similarity_score': match.similarity_score,
                'matching_method': match.matching_method,
                'llm_confidence': match.llm_confidence,
                'llm_reasoning': match.llm_reasoning,
                'manual_review_required': match.manual_review_required
            })
        
        await self.db_manager.bulk_insert('staging.entity_matching_candidates', records)
        logger.info(f"Saved {len(records)} entity matches to staging")


# CLI interface for testing
if __name__ == "__main__":
    import asyncio
    from ..utils.config import Config
    
    async def main():
        config = Config()
        llm_client = LLMClient(config)
        db_manager = DatabaseManager(config.database_url)
        
        matcher = LLMEntityMatcher(llm_client, db_manager)
        
        # Run entity matching
        matches = await matcher.match_entities(batch_size=100)
        print(f"Found {len(matches)} entity matches")
        
        # Print sample matches
        for match in matches[:5]:
            print(f"Match: CC#{match.common_crawl_id} <-> ABR#{match.abr_id} (Score: {match.similarity_score:.3f})")
    
    asyncio.run(main())