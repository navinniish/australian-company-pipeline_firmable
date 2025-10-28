"""
Common Crawl extractor for Australian company websites.
Extracts company data from web pages using intelligent parsing and LLM assistance.
"""

import asyncio
import logging
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
from urllib.parse import urlparse, urljoin
import requests
from bs4 import BeautifulSoup
import pandas as pd
from sqlalchemy import create_engine
import json
import re
from datetime import datetime
import warcio
from io import BytesIO

from ..utils.text_processing import normalize_company_name, extract_company_info
from ..utils.llm_client import LLMClient
from ..utils.database import DatabaseManager

logger = logging.getLogger(__name__)

@dataclass
class CompanyWebsiteData:
    """Data structure for extracted company information from websites."""
    website_url: str
    company_name: Optional[str]
    industry: Optional[str]
    contact_info: Dict
    social_links: Dict
    raw_html_content: str
    meta_description: Optional[str]
    title: Optional[str]
    extraction_confidence: float


class CommonCrawlExtractor:
    """
    Extracts Australian company data from Common Crawl archives.
    Focuses on .au domains and uses LLM assistance for intelligent extraction.
    """
    
    def __init__(self, llm_client: LLMClient, db_manager: DatabaseManager):
        self.llm_client = llm_client
        self.db_manager = db_manager
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Australian-Company-Pipeline/1.0 (Research; contact@example.com)'
        })
        
        # Common Crawl index URL for March 2025
        self.cc_index_url = "https://index.commoncrawl.org/CC-MAIN-2025-10-index"
        
        # Australian domain patterns
        self.au_domain_patterns = [
            r'\.com\.au$', r'\.net\.au$', r'\.org\.au$', 
            r'\.edu\.au$', r'\.gov\.au$', r'\.asn\.au$'
        ]
        
    async def extract_australian_companies(self, max_records: int = 200000) -> List[CompanyWebsiteData]:
        """
        Main extraction method to get Australian company data from Common Crawl.
        
        Args:
            max_records: Maximum number of records to extract
            
        Returns:
            List of CompanyWebsiteData objects
        """
        logger.info(f"Starting Common Crawl extraction for max {max_records} Australian companies")
        
        # Step 1: Get Australian URLs from Common Crawl index
        au_urls = await self._get_australian_urls(max_records)
        logger.info(f"Found {len(au_urls)} Australian URLs")
        
        # Step 2: Extract company data from each URL
        company_data = []
        batch_size = 100
        
        for i in range(0, len(au_urls), batch_size):
            batch_urls = au_urls[i:i + batch_size]
            batch_data = await self._process_url_batch(batch_urls)
            company_data.extend(batch_data)
            
            logger.info(f"Processed {len(company_data)} companies so far")
            
            # Save progress periodically
            if len(company_data) % 1000 == 0:
                await self._save_batch_to_staging(company_data[-1000:])
        
        logger.info(f"Extraction complete. Total companies: {len(company_data)}")
        return company_data
    
    async def _get_australian_urls(self, max_records: int) -> List[str]:
        """
        Query Common Crawl index for Australian domain URLs.
        
        Returns:
            List of Australian website URLs
        """
        urls = []
        
        # Query Common Crawl index for Australian domains
        for domain_pattern in self.au_domain_patterns:
            query_url = f"{self.cc_index_url}?url={domain_pattern}&output=json&limit={max_records//len(self.au_domain_patterns)}"
            
            try:
                response = self.session.get(query_url, timeout=60)
                response.raise_for_status()
                
                for line in response.text.strip().split('\n'):
                    if line:
                        record = json.loads(line)
                        url = record.get('url', '')
                        if self._is_likely_company_url(url):
                            urls.append(url)
                            
            except Exception as e:
                logger.error(f"Error querying Common Crawl for pattern {domain_pattern}: {e}")
                continue
        
        # Deduplicate and sort by domain
        unique_urls = list(set(urls))
        return unique_urls[:max_records]
    
    def _is_likely_company_url(self, url: str) -> bool:
        """
        Filter URLs that are likely to be company websites.
        
        Args:
            url: URL to evaluate
            
        Returns:
            True if URL is likely a company website
        """
        parsed = urlparse(url)
        path = parsed.path.lower()
        
        # Exclude certain paths that are unlikely to be company pages
        excluded_paths = [
            '/blog/', '/news/', '/articles/', '/wp-content/', '/wp-admin/',
            '/user/', '/member/', '/profile/', '/forum/', '/category/',
            '/.well-known/', '/sitemap', '/robots.txt', '/feed'
        ]
        
        # Exclude file extensions
        excluded_extensions = [
            '.pdf', '.doc', '.docx', '.jpg', '.jpeg', '.png', '.gif', 
            '.zip', '.exe', '.xml', '.css', '.js'
        ]
        
        # Check exclusions
        for excluded in excluded_paths:
            if excluded in path:
                return False
                
        for ext in excluded_extensions:
            if path.endswith(ext):
                return False
        
        # Prefer home pages and about/contact pages
        preferred_paths = ['/', '/about', '/contact', '/home', '/company']
        return any(path.startswith(preferred) for preferred in preferred_paths) or path == '/'
    
    async def _process_url_batch(self, urls: List[str]) -> List[CompanyWebsiteData]:
        """
        Process a batch of URLs to extract company information.
        
        Args:
            urls: List of URLs to process
            
        Returns:
            List of extracted company data
        """
        tasks = [self._extract_company_from_url(url) for url in urls]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        company_data = []
        for result in results:
            if isinstance(result, CompanyWebsiteData):
                company_data.append(result)
            elif isinstance(result, Exception):
                logger.warning(f"Error processing URL: {result}")
        
        return company_data
    
    async def _extract_company_from_url(self, url: str) -> Optional[CompanyWebsiteData]:
        """
        Extract company information from a single URL.
        
        Args:
            url: Website URL to extract from
            
        Returns:
            CompanyWebsiteData object or None if extraction failed
        """
        try:
            # Fetch the webpage
            response = self.session.get(url, timeout=30, allow_redirects=True)
            response.raise_for_status()
            
            # Parse HTML
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Basic extraction
            title = self._extract_title(soup)
            meta_description = self._extract_meta_description(soup)
            
            # Use LLM for intelligent company information extraction
            company_info = await self._llm_extract_company_info(
                url, title, meta_description, soup.get_text()[:5000]
            )
            
            return CompanyWebsiteData(
                website_url=url,
                company_name=company_info.get('company_name'),
                industry=company_info.get('industry'),
                contact_info=company_info.get('contact_info', {}),
                social_links=self._extract_social_links(soup),
                raw_html_content=str(soup)[:10000],  # Limit size
                meta_description=meta_description,
                title=title,
                extraction_confidence=company_info.get('confidence', 0.5)
            )
            
        except Exception as e:
            logger.warning(f"Error extracting from {url}: {e}")
            return None
    
    def _extract_title(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract page title."""
        title_tag = soup.find('title')
        return title_tag.get_text().strip() if title_tag else None
    
    def _extract_meta_description(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract meta description."""
        meta_desc = soup.find('meta', attrs={'name': 'description'})
        return meta_desc.get('content', '').strip() if meta_desc else None
    
    def _extract_social_links(self, soup: BeautifulSoup) -> Dict[str, str]:
        """Extract social media links."""
        social_links = {}
        
        # Common social media patterns
        social_patterns = {
            'linkedin': r'linkedin\.com/company/',
            'facebook': r'facebook\.com/',
            'twitter': r'twitter\.com/',
            'instagram': r'instagram\.com/'
        }
        
        for link in soup.find_all('a', href=True):
            href = link.get('href', '')
            for platform, pattern in social_patterns.items():
                if re.search(pattern, href, re.IGNORECASE):
                    social_links[platform] = href
                    break
        
        return social_links
    
    async def _llm_extract_company_info(self, url: str, title: str, description: str, content: str) -> Dict:
        """
        Use LLM to extract company information intelligently.
        
        Args:
            url: Website URL
            title: Page title
            description: Meta description
            content: Page text content (truncated)
            
        Returns:
            Dictionary with extracted company information
        """
        prompt = f"""
        You are analyzing an Australian company website to extract key business information.
        
        Website URL: {url}
        Page Title: {title}
        Meta Description: {description}
        
        Page Content (first 5000 characters):
        {content}
        
        Please extract the following information and return as JSON:
        {{
            "company_name": "Official company name (string or null)",
            "industry": "Primary industry/business sector (string or null)", 
            "contact_info": {{
                "email": "Contact email if found (string or null)",
                "phone": "Phone number if found (string or null)",
                "address": "Physical address if found (string or null)"
            }},
            "confidence": "Confidence score 0.0-1.0 for extraction quality (float)"
        }}
        
        Guidelines:
        - If company name is unclear, return null
        - For industry, use broad categories like "Manufacturing", "Professional Services", "Technology", "Retail", etc.
        - Only include contact info if clearly visible on the page
        - Set confidence based on how clear and complete the information is
        - Higher confidence (0.8+) for clear company pages with complete info
        - Lower confidence (0.3-0.6) for unclear or personal websites
        - Return valid JSON only
        """
        
        try:
            response = await self.llm_client.chat_completion(prompt)
            return json.loads(response)
        except Exception as e:
            logger.warning(f"LLM extraction failed for {url}: {e}")
            return {
                "company_name": None,
                "industry": None, 
                "contact_info": {},
                "confidence": 0.1
            }
    
    async def _save_batch_to_staging(self, batch_data: List[CompanyWebsiteData]):
        """Save a batch of company data to staging table."""
        if not batch_data:
            return
            
        records = []
        for data in batch_data:
            records.append({
                'website_url': data.website_url,
                'company_name': data.company_name,
                'industry': data.industry,
                'raw_html_content': data.raw_html_content,
                'meta_description': data.meta_description,
                'title': data.title,
                'contact_info': json.dumps(data.contact_info),
                'social_links': json.dumps(data.social_links),
                'extraction_confidence': data.extraction_confidence
            })
        
        await self.db_manager.bulk_insert('staging.common_crawl_raw', records)
        logger.info(f"Saved {len(records)} records to staging")


# CLI interface for testing
if __name__ == "__main__":
    import asyncio
    from ..utils.config import Config
    
    async def main():
        config = Config()
        llm_client = LLMClient(config)
        db_manager = DatabaseManager(config.database_url)
        
        extractor = CommonCrawlExtractor(llm_client, db_manager)
        
        # Extract sample data
        companies = await extractor.extract_australian_companies(max_records=1000)
        print(f"Extracted {len(companies)} companies")
        
        # Save to staging
        await extractor._save_batch_to_staging(companies)
    
    asyncio.run(main())