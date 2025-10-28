"""
Enhanced social media extraction for comprehensive digital presence analysis.
Detects and analyzes social media profiles across all major platforms.
"""

import asyncio
import aiohttp
import re
import logging
from typing import Dict, List, Optional, Set, Tuple, Any
from dataclasses import dataclass
from datetime import datetime
from urllib.parse import urljoin, urlparse
import json

logger = logging.getLogger(__name__)


@dataclass
class SocialProfile:
    """Social media profile information."""
    platform: str
    url: str
    username: str
    verified: Optional[bool] = None
    followers_count: Optional[int] = None
    posts_count: Optional[int] = None
    last_activity: Optional[datetime] = None
    profile_type: str = "business"  # business, personal, brand
    engagement_score: Optional[float] = None
    confidence: float = 0.8


@dataclass
class DigitalPresenceProfile:
    """Complete digital presence analysis."""
    company_name: str
    website_url: Optional[str]
    social_profiles: List[SocialProfile]
    total_platforms: int
    verified_platforms: int
    total_followers: int
    engagement_level: str  # high, medium, low
    digital_maturity_score: float
    recommendations: List[str]


class SocialMediaExtractor:
    """
    Advanced social media extractor with platform-specific detection.
    Analyzes digital presence and engagement across all major platforms.
    """
    
    def __init__(self, session: Optional[aiohttp.ClientSession] = None):
        """Initialize with optional HTTP session."""
        self.session = session
        self.platform_patterns = self._load_platform_patterns()
        self.verification_patterns = self._load_verification_patterns()
        
    async def extract_social_profiles(self, 
                                    company_name: str,
                                    website_content: str,
                                    website_url: Optional[str] = None) -> DigitalPresenceProfile:
        """
        Extract comprehensive social media profiles for a company.
        
        Args:
            company_name: Name of the company
            website_content: HTML content of company website
            website_url: URL of the company website
            
        Returns:
            Complete digital presence profile
        """
        # Extract social links from website content
        extracted_profiles = []
        
        # Search for social media links in content
        for platform, patterns in self.platform_patterns.items():
            for pattern in patterns:
                matches = re.findall(pattern, website_content, re.IGNORECASE)
                for match in matches:
                    if isinstance(match, tuple):
                        match = match[0]
                    
                    profile = await self._create_social_profile(
                        platform, match, company_name
                    )
                    if profile:
                        extracted_profiles.append(profile)
        
        # Remove duplicates and validate profiles
        validated_profiles = await self._validate_profiles(extracted_profiles)
        
        # Search for additional profiles using company name
        discovered_profiles = await self._discover_profiles_by_name(company_name)
        
        # Combine and deduplicate
        all_profiles = self._merge_profiles(validated_profiles, discovered_profiles)
        
        # Analyze digital presence
        return self._analyze_digital_presence(
            company_name, website_url, all_profiles
        )
    
    async def batch_extract_social_profiles(self, 
                                          companies: List[Dict[str, Any]]) -> List[DigitalPresenceProfile]:
        """
        Extract social profiles for multiple companies in batch.
        
        Args:
            companies: List of company records with name and website info
            
        Returns:
            List of digital presence profiles
        """
        # Process in batches to avoid overwhelming APIs
        batch_size = 10
        results = []
        
        for i in range(0, len(companies), batch_size):
            batch = companies[i:i + batch_size]
            
            tasks = []
            for company in batch:
                name = company.get("company_name", "")
                website_content = company.get("website_content", "")
                website_url = company.get("website_url")
                
                task = self.extract_social_profiles(name, website_content, website_url)
                tasks.append(task)
            
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for result in batch_results:
                if isinstance(result, Exception):
                    logger.error(f"Social media extraction failed: {result}")
                    # Create empty profile for failed extractions
                    results.append(DigitalPresenceProfile(
                        company_name="Unknown",
                        website_url=None,
                        social_profiles=[],
                        total_platforms=0,
                        verified_platforms=0,
                        total_followers=0,
                        engagement_level="unknown",
                        digital_maturity_score=0.0,
                        recommendations=["Social media extraction failed"]
                    ))
                else:
                    results.append(result)
        
        return results
    
    async def _create_social_profile(self, 
                                   platform: str, 
                                   url: str, 
                                   company_name: str) -> Optional[SocialProfile]:
        """Create and validate a social profile."""
        try:
            # Clean and normalize URL
            clean_url = self._clean_social_url(url, platform)
            if not clean_url:
                return None
            
            # Extract username
            username = self._extract_username(clean_url, platform)
            if not username:
                return None
            
            # Basic profile creation
            profile = SocialProfile(
                platform=platform,
                url=clean_url,
                username=username,
                confidence=0.7
            )
            
            # Enhance with additional data if session available
            if self.session:
                enhanced_profile = await self._enhance_profile(profile, company_name)
                return enhanced_profile
            
            return profile
            
        except Exception as e:
            logger.error(f"Failed to create profile for {platform} {url}: {e}")
            return None
    
    async def _enhance_profile(self, 
                             profile: SocialProfile, 
                             company_name: str) -> SocialProfile:
        """Enhance profile with additional metadata."""
        try:
            # Platform-specific enhancement
            if profile.platform == "linkedin":
                return await self._enhance_linkedin_profile(profile, company_name)
            elif profile.platform == "facebook":
                return await self._enhance_facebook_profile(profile, company_name)
            elif profile.platform == "twitter":
                return await self._enhance_twitter_profile(profile, company_name)
            elif profile.platform == "instagram":
                return await self._enhance_instagram_profile(profile, company_name)
            else:
                # Generic enhancement
                return await self._enhance_generic_profile(profile, company_name)
                
        except Exception as e:
            logger.error(f"Profile enhancement failed for {profile.platform}: {e}")
            return profile
    
    async def _enhance_linkedin_profile(self, 
                                      profile: SocialProfile, 
                                      company_name: str) -> SocialProfile:
        """Enhance LinkedIn profile with company-specific data."""
        # LinkedIn profiles are generally high-confidence for businesses
        profile.confidence = 0.9
        profile.profile_type = "business"
        
        # Check if it's a company page vs personal profile
        if "/company/" in profile.url:
            profile.confidence = 0.95
        
        return profile
    
    async def _enhance_facebook_profile(self, 
                                      profile: SocialProfile, 
                                      company_name: str) -> SocialProfile:
        """Enhance Facebook profile with business page indicators."""
        # Facebook business pages vs personal profiles
        if "/pages/" in profile.url or any(word in profile.username.lower() 
                                         for word in ["business", "company", "corp"]):
            profile.profile_type = "business"
            profile.confidence = 0.85
        else:
            profile.confidence = 0.6
        
        return profile
    
    async def _enhance_twitter_profile(self, 
                                     profile: SocialProfile, 
                                     company_name: str) -> SocialProfile:
        """Enhance Twitter/X profile."""
        # Twitter business indicators
        business_indicators = ["official", "corp", "company", "business", "hq"]
        username_lower = profile.username.lower()
        
        if any(indicator in username_lower for indicator in business_indicators):
            profile.profile_type = "business"
            profile.confidence = 0.8
        
        return profile
    
    async def _enhance_instagram_profile(self, 
                                       profile: SocialProfile, 
                                       company_name: str) -> SocialProfile:
        """Enhance Instagram profile."""
        # Instagram business account indicators
        if any(word in profile.username.lower() 
               for word in ["official", "business", "company"]):
            profile.profile_type = "business"
            profile.confidence = 0.8
        
        return profile
    
    async def _enhance_generic_profile(self, 
                                     profile: SocialProfile, 
                                     company_name: str) -> SocialProfile:
        """Generic profile enhancement."""
        # Check username similarity with company name
        username_normalized = re.sub(r'[^\w]', '', profile.username.lower())
        company_normalized = re.sub(r'[^\w]', '', company_name.lower())
        
        # Simple similarity check
        if company_normalized in username_normalized or username_normalized in company_normalized:
            profile.confidence = min(profile.confidence + 0.2, 1.0)
        
        return profile
    
    async def _validate_profiles(self, profiles: List[SocialProfile]) -> List[SocialProfile]:
        """Validate and filter social profiles."""
        validated = []
        seen_urls = set()
        
        for profile in profiles:
            # Skip duplicates
            if profile.url in seen_urls:
                continue
            seen_urls.add(profile.url)
            
            # Skip low-confidence profiles
            if profile.confidence < 0.3:
                continue
            
            # Basic URL validation
            if not self._is_valid_social_url(profile.url, profile.platform):
                continue
            
            validated.append(profile)
        
        return validated
    
    async def _discover_profiles_by_name(self, company_name: str) -> List[SocialProfile]:
        """Discover additional social profiles by company name search."""
        # This would integrate with social media APIs or search engines
        # For now, return empty list as it requires API keys
        discovered = []
        
        # Placeholder for future API integration
        logger.info(f"Profile discovery for '{company_name}' - requires API integration")
        
        return discovered
    
    def _merge_profiles(self, 
                       primary_profiles: List[SocialProfile], 
                       discovered_profiles: List[SocialProfile]) -> List[SocialProfile]:
        """Merge and deduplicate profile lists."""
        all_profiles = primary_profiles[:]
        seen_platforms = {p.platform for p in primary_profiles}
        
        # Add discovered profiles for platforms not yet covered
        for profile in discovered_profiles:
            if profile.platform not in seen_platforms:
                all_profiles.append(profile)
                seen_platforms.add(profile.platform)
        
        return all_profiles
    
    def _analyze_digital_presence(self, 
                                 company_name: str,
                                 website_url: Optional[str],
                                 profiles: List[SocialProfile]) -> DigitalPresenceProfile:
        """Analyze overall digital presence."""
        total_platforms = len(profiles)
        verified_platforms = sum(1 for p in profiles if p.verified)
        total_followers = sum(p.followers_count or 0 for p in profiles)
        
        # Calculate digital maturity score
        maturity_score = self._calculate_digital_maturity_score(
            website_url, profiles
        )
        
        # Determine engagement level
        engagement_level = self._determine_engagement_level(profiles)
        
        # Generate recommendations
        recommendations = self._generate_recommendations(
            company_name, website_url, profiles
        )
        
        return DigitalPresenceProfile(
            company_name=company_name,
            website_url=website_url,
            social_profiles=profiles,
            total_platforms=total_platforms,
            verified_platforms=verified_platforms,
            total_followers=total_followers,
            engagement_level=engagement_level,
            digital_maturity_score=maturity_score,
            recommendations=recommendations
        )
    
    def _calculate_digital_maturity_score(self, 
                                         website_url: Optional[str],
                                         profiles: List[SocialProfile]) -> float:
        """Calculate overall digital maturity score."""
        score = 0.0
        
        # Base score for having a website
        if website_url:
            score += 0.3
        
        # Score for social media presence
        platform_weights = {
            "linkedin": 0.15,
            "facebook": 0.12,
            "instagram": 0.10,
            "twitter": 0.08,
            "youtube": 0.08,
            "tiktok": 0.05,
            "pinterest": 0.03,
            "others": 0.02
        }
        
        for profile in profiles:
            weight = platform_weights.get(profile.platform, platform_weights["others"])
            confidence_factor = profile.confidence
            score += weight * confidence_factor
        
        # Bonus for verification
        verified_count = sum(1 for p in profiles if p.verified)
        score += min(verified_count * 0.05, 0.15)
        
        # Cap at 1.0
        return min(score, 1.0)
    
    def _determine_engagement_level(self, profiles: List[SocialProfile]) -> str:
        """Determine overall engagement level."""
        if not profiles:
            return "none"
        
        # Calculate average engagement score
        engagement_scores = [p.engagement_score for p in profiles if p.engagement_score]
        
        if not engagement_scores:
            # Fallback based on platform diversity and follower count
            total_followers = sum(p.followers_count or 0 for p in profiles)
            platform_count = len(profiles)
            
            if total_followers > 10000 and platform_count >= 4:
                return "high"
            elif total_followers > 1000 and platform_count >= 2:
                return "medium"
            else:
                return "low"
        
        avg_engagement = sum(engagement_scores) / len(engagement_scores)
        
        if avg_engagement >= 0.7:
            return "high"
        elif avg_engagement >= 0.4:
            return "medium"
        else:
            return "low"
    
    def _generate_recommendations(self, 
                                 company_name: str,
                                 website_url: Optional[str],
                                 profiles: List[SocialProfile]) -> List[str]:
        """Generate digital presence improvement recommendations."""
        recommendations = []
        
        present_platforms = {p.platform for p in profiles}
        
        # Essential platforms missing
        essential_platforms = {"linkedin", "facebook", "instagram"}
        missing_essential = essential_platforms - present_platforms
        
        if missing_essential:
            recommendations.append(
                f"Consider establishing presence on {', '.join(missing_essential)} "
                "for better business visibility"
            )
        
        # Industry-specific recommendations
        if "youtube" not in present_platforms:
            recommendations.append(
                "YouTube channel could enhance brand storytelling and customer education"
            )
        
        if "tiktok" not in present_platforms and any("tech" in company_name.lower() 
                                                   or "digital" in company_name.lower() 
                                                   for _ in [1]):
            recommendations.append(
                "TikTok presence could help reach younger demographics"
            )
        
        # Verification recommendations
        unverified_profiles = [p for p in profiles if not p.verified]
        if unverified_profiles and len(profiles) >= 3:
            recommendations.append(
                "Consider verifying social media accounts to build trust and credibility"
            )
        
        # Website integration
        if website_url and profiles:
            recommendations.append(
                "Ensure social media links are prominently displayed on your website"
            )
        
        # Engagement recommendations
        low_engagement_profiles = [p for p in profiles 
                                 if p.engagement_score and p.engagement_score < 0.3]
        if len(low_engagement_profiles) >= 2:
            recommendations.append(
                "Focus on improving engagement rates through regular, quality content posting"
            )
        
        return recommendations
    
    def _clean_social_url(self, url: str, platform: str) -> Optional[str]:
        """Clean and normalize social media URL."""
        if not url:
            return None
        
        # Add https if missing
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
        
        # Platform-specific cleaning
        if platform == "linkedin":
            # Remove tracking parameters
            url = re.sub(r'\?.*$', '', url)
        elif platform == "facebook":
            # Normalize facebook URLs
            url = url.replace('www.facebook.com', 'facebook.com')
            url = re.sub(r'\?.*$', '', url)
        
        return url
    
    def _extract_username(self, url: str, platform: str) -> Optional[str]:
        """Extract username from social media URL."""
        try:
            parsed = urlparse(url)
            path = parsed.path.strip('/')
            
            if platform == "linkedin":
                if path.startswith('company/'):
                    return path.split('/', 1)[1]
                elif path.startswith('in/'):
                    return path.split('/', 1)[1]
            elif platform == "twitter":
                return path
            elif platform == "instagram":
                return path
            elif platform == "facebook":
                if path.startswith('pages/'):
                    parts = path.split('/')
                    return parts[-1] if len(parts) > 1 else path
                return path
            else:
                # Generic extraction
                return path.split('/')[-1] if path else None
                
        except Exception as e:
            logger.error(f"Username extraction failed for {url}: {e}")
            return None
    
    def _is_valid_social_url(self, url: str, platform: str) -> bool:
        """Validate social media URL."""
        try:
            parsed = urlparse(url)
            domain = parsed.netloc.lower()
            
            # Platform-specific domain validation
            platform_domains = {
                "linkedin": ["linkedin.com", "www.linkedin.com"],
                "facebook": ["facebook.com", "www.facebook.com", "fb.com"],
                "twitter": ["twitter.com", "www.twitter.com", "x.com", "www.x.com"],
                "instagram": ["instagram.com", "www.instagram.com"],
                "youtube": ["youtube.com", "www.youtube.com"],
                "tiktok": ["tiktok.com", "www.tiktok.com"],
                "pinterest": ["pinterest.com", "www.pinterest.com", "pinterest.com.au"],
            }
            
            expected_domains = platform_domains.get(platform, [])
            return any(domain == expected or domain.endswith('.' + expected) 
                      for expected in expected_domains)
                      
        except Exception:
            return False
    
    def _load_platform_patterns(self) -> Dict[str, List[str]]:
        """Load regex patterns for each social media platform."""
        return {
            "linkedin": [
                r'https?://(?:www\.)?linkedin\.com/company/[\w\-\.%]+/?',
                r'https?://(?:www\.)?linkedin\.com/in/[\w\-\.%]+/?',
                r'linkedin\.com/company/[\w\-\.%]+',
                r'linkedin\.com/in/[\w\-\.%]+',
            ],
            "facebook": [
                r'https?://(?:www\.)?facebook\.com/(?:pages/)?[\w\-\.%]+/?',
                r'https?://(?:www\.)?fb\.com/[\w\-\.%]+/?',
                r'facebook\.com/(?:pages/)?[\w\-\.%]+',
                r'fb\.com/[\w\-\.%]+',
            ],
            "twitter": [
                r'https?://(?:www\.)?twitter\.com/[\w\-\.%]+/?',
                r'https?://(?:www\.)?x\.com/[\w\-\.%]+/?',
                r'twitter\.com/[\w\-\.%]+',
                r'x\.com/[\w\-\.%]+',
            ],
            "instagram": [
                r'https?://(?:www\.)?instagram\.com/[\w\-\.%]+/?',
                r'instagram\.com/[\w\-\.%]+',
            ],
            "youtube": [
                r'https?://(?:www\.)?youtube\.com/(?:c/|user/|channel/|@)?[\w\-\.%]+/?',
                r'youtube\.com/(?:c/|user/|channel/|@)?[\w\-\.%]+',
            ],
            "tiktok": [
                r'https?://(?:www\.)?tiktok\.com/@?[\w\-\.%]+/?',
                r'tiktok\.com/@?[\w\-\.%]+',
            ],
            "pinterest": [
                r'https?://(?:www\.)?pinterest\.com(?:\.au)?/[\w\-\.%]+/?',
                r'pinterest\.com(?:\.au)?/[\w\-\.%]+',
            ],
            "github": [
                r'https?://(?:www\.)?github\.com/[\w\-\.%]+/?',
                r'github\.com/[\w\-\.%]+',
            ],
        }
    
    def _load_verification_patterns(self) -> Dict[str, List[str]]:
        """Load patterns that indicate verified accounts."""
        return {
            "verified_indicators": [
                r'verified[^\w]',
                r'official[^\w]',
                r'✓',
                r'☑',
                r'badge',
            ]
        }


# Example usage and testing
if __name__ == "__main__":
    async def demo():
        # Create session for HTTP requests
        async with aiohttp.ClientSession() as session:
            extractor = SocialMediaExtractor(session)
            
            # Test with sample company data
            sample_website_content = """
            <html>
            <body>
            <h1>Acme Corporation</h1>
            <p>Follow us on social media:</p>
            <a href="https://linkedin.com/company/acme-corp">LinkedIn</a>
            <a href="https://facebook.com/acmecorporation">Facebook</a>
            <a href="https://twitter.com/acmecorp">Twitter</a>
            <a href="https://instagram.com/acme_official">Instagram</a>
            <a href="https://youtube.com/c/acmecorporation">YouTube</a>
            </body>
            </html>
            """
            
            profile = await extractor.extract_social_profiles(
                "Acme Corporation",
                sample_website_content,
                "https://acme-corp.com"
            )
            
            print("🌐 Digital Presence Analysis")
            print("=" * 50)
            print(f"Company: {profile.company_name}")
            print(f"Total Platforms: {profile.total_platforms}")
            print(f"Digital Maturity Score: {profile.digital_maturity_score:.2f}")
            print(f"Engagement Level: {profile.engagement_level}")
            
            print("\n📱 Social Profiles:")
            for social_profile in profile.social_profiles:
                print(f"  {social_profile.platform.title()}: {social_profile.url}")
                print(f"    Username: @{social_profile.username}")
                print(f"    Confidence: {social_profile.confidence:.2f}")
            
            print(f"\n💡 Recommendations:")
            for rec in profile.recommendations:
                print(f"  • {rec}")
    
    # Run demo
    import asyncio
    asyncio.run(demo())