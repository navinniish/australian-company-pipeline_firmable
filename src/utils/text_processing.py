"""
Text processing utilities for company name normalization and information extraction.
"""

import re
import string
from typing import Dict, List, Optional, Tuple
import unicodedata


def normalize_company_name(name: str) -> str:
    """
    Normalize company name for better matching.
    
    Args:
        name: Raw company name
        
    Returns:
        Normalized company name
    """
    if not name:
        return ""
    
    # Convert to lowercase
    normalized = name.lower()
    
    # Remove extra whitespace
    normalized = re.sub(r'\s+', ' ', normalized).strip()
    
    # Remove/normalize common business suffixes
    suffixes = [
        r'\bpty\.?\s*ltd\.?',
        r'\blimited\b',
        r'\bcompany\b',
        r'\bcorp\.?\b',
        r'\bcorporation\b',
        r'\binc\.?\b',
        r'\bincorporated\b',
        r'\bllc\b',
        r'\bllp\b',
        r'\blp\b',
    ]
    
    for suffix in suffixes:
        normalized = re.sub(suffix, '', normalized)
    
    # Remove punctuation except hyphens and apostrophes
    normalized = re.sub(r'[^\w\s\-\']', '', normalized)
    
    # Clean up whitespace again
    normalized = re.sub(r'\s+', ' ', normalized).strip()
    
    return normalized


def extract_company_info(text: str) -> Dict:
    """
    Extract company information from text using regex patterns.
    
    Args:
        text: Text content to extract from
        
    Returns:
        Dictionary with extracted information
    """
    info = {
        'emails': [],
        'phones': [],
        'addresses': [],
        'social_links': {}
    }
    
    # Email extraction
    email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
    info['emails'] = list(set(re.findall(email_pattern, text)))
    
    # Phone number extraction (Australian formats)
    phone_patterns = [
        r'\b(?:\+61\s?)?(?:\(0\d\)\s?)?\d{4}\s?\d{4}\b',  # Standard format
        r'\b(?:\+61\s?)?0[2-9]\s?\d{4}\s?\d{4}\b',        # With area code
        r'\b1[38]00\s?\d{3}\s?\d{3}\b'                     # 1800/1300 numbers
    ]
    
    phones = []
    for pattern in phone_patterns:
        phones.extend(re.findall(pattern, text))
    info['phones'] = list(set(phones))
    
    # Enhanced social media links with broader pattern matching
    social_patterns = {
        'linkedin': r'(?:linkedin\.com/(?:company|in|pub)/[\w\-\.]+|linkedin\.com/[\w\-\.]+)',
        'facebook': r'(?:facebook\.com/(?:pages/)?[\w\-\.]+|fb\.com/[\w\-\.]+)',
        'twitter': r'(?:twitter\.com/[\w\-]+|x\.com/[\w\-]+)',
        'instagram': r'(?:instagram\.com/[\w\-\.]+|instagr\.am/[\w\-\.]+)',
        'youtube': r'(?:youtube\.com/(?:c/|user/|channel/|@)?[\w\-]+|youtu\.be/[\w\-]+)',
        'tiktok': r'tiktok\.com/@?[\w\-\.]+',
        'pinterest': r'(?:pinterest\.com(?:\.au)?/[\w\-\.]+|pin\.it/[\w\-]+)',
        'snapchat': r'snapchat\.com/add/[\w\-\.]+',
        'whatsapp': r'(?:wa\.me/[\d]+|whatsapp\.com/[\w\-]+)',
        'telegram': r'(?:t\.me/[\w\-]+|telegram\.me/[\w\-]+)',
        'discord': r'discord\.gg/[\w\-]+',
        'reddit': r'reddit\.com/r/[\w\-]+',
        'medium': r'(?:medium\.com/@[\w\-\.]+|[\w\-]+\.medium\.com)',
        'vimeo': r'vimeo\.com/[\w\-]+',
        'behance': r'behance\.net/[\w\-\.]+',
        'dribbble': r'dribbble\.com/[\w\-\.]+',
        'github': r'github\.com/[\w\-\.]+',
        'gitlab': r'gitlab\.com/[\w\-\.]+',
        'bitbucket': r'bitbucket\.org/[\w\-\.]+',
    }
    
    for platform, pattern in social_patterns.items():
        matches = re.findall(pattern, text, re.IGNORECASE)
        if matches:
            info['social_links'][platform] = matches[0]
    
    # Basic address extraction (Australian postcodes)
    address_pattern = r'\b\d{1,4}[A-Za-z]?\s+[A-Za-z\s]+(?:Street|St|Road|Rd|Avenue|Ave|Drive|Dr|Lane|Ln|Place|Pl|Court|Ct|Crescent|Cres|Close|Cl|Way|Parade|Pde),?\s+[A-Za-z\s]+,?\s+[A-Z]{2,3}\s+\d{4}\b'
    addresses = re.findall(address_pattern, text)
    info['addresses'] = list(set(addresses))
    
    return info


def calculate_string_similarity(s1: str, s2: str) -> float:
    """
    Calculate similarity between two strings using multiple methods.
    
    Args:
        s1: First string
        s2: Second string
        
    Returns:
        Similarity score (0.0 to 1.0)
    """
    if not s1 or not s2:
        return 0.0
    
    # Normalize strings
    s1_norm = normalize_company_name(s1)
    s2_norm = normalize_company_name(s2)
    
    if s1_norm == s2_norm:
        return 1.0
    
    # Jaccard similarity (token-based)
    tokens1 = set(s1_norm.split())
    tokens2 = set(s2_norm.split())
    
    if not tokens1 or not tokens2:
        return 0.0
    
    intersection = tokens1.intersection(tokens2)
    union = tokens1.union(tokens2)
    jaccard = len(intersection) / len(union)
    
    # Levenshtein similarity
    levenshtein = 1.0 - (levenshtein_distance(s1_norm, s2_norm) / max(len(s1_norm), len(s2_norm)))
    
    # Return weighted average
    return (jaccard * 0.6) + (levenshtein * 0.4)


def levenshtein_distance(s1: str, s2: str) -> int:
    """
    Calculate Levenshtein distance between two strings.
    
    Args:
        s1: First string
        s2: Second string
        
    Returns:
        Edit distance
    """
    if len(s1) < len(s2):
        return levenshtein_distance(s2, s1)

    if len(s2) == 0:
        return len(s1)

    previous_row = list(range(len(s2) + 1))
    for i, c1 in enumerate(s1):
        current_row = [i + 1]
        for j, c2 in enumerate(s2):
            insertions = previous_row[j + 1] + 1
            deletions = current_row[j] + 1
            substitutions = previous_row[j] + (c1 != c2)
            current_row.append(min(insertions, deletions, substitutions))
        previous_row = current_row
    
    return previous_row[-1]


def extract_australian_business_number(text: str) -> Optional[str]:
    """
    Extract Australian Business Number (ABN) from text.
    
    Args:
        text: Text to search
        
    Returns:
        ABN if found, None otherwise
    """
    # ABN pattern: 11 digits, optionally with spaces
    abn_pattern = r'\b(?:ABN:?\s*)?(\d{2}\s?\d{3}\s?\d{3}\s?\d{3})\b'
    match = re.search(abn_pattern, text, re.IGNORECASE)
    
    if match:
        abn = re.sub(r'\s', '', match.group(1))  # Remove spaces
        if len(abn) == 11 and validate_abn(abn):
            return abn
    
    return None


def validate_abn(abn: str) -> bool:
    """
    Validate Australian Business Number using the checksum algorithm.
    
    Args:
        abn: 11-digit ABN string
        
    Returns:
        True if valid ABN
    """
    if not abn or len(abn) != 11 or not abn.isdigit():
        return False
    
    # ABN validation algorithm
    weights = [10, 1, 3, 5, 7, 9, 11, 13, 15, 17, 19]
    
    # Subtract 1 from the first digit
    first_digit = int(abn[0]) - 1
    if first_digit < 0:
        return False
    
    # Calculate weighted sum
    total = first_digit * weights[0]
    for i in range(1, 11):
        total += int(abn[i]) * weights[i]
    
    # Check if divisible by 89
    return total % 89 == 0


def clean_html_text(html_content: str) -> str:
    """
    Clean HTML content and extract readable text.
    
    Args:
        html_content: Raw HTML content
        
    Returns:
        Clean text content
    """
    # Remove HTML tags
    text = re.sub(r'<[^>]+>', '', html_content)
    
    # Decode HTML entities
    import html
    text = html.unescape(text)
    
    # Normalize whitespace
    text = re.sub(r'\s+', ' ', text).strip()
    
    return text


def extract_industry_keywords(text: str) -> List[str]:
    """
    Extract industry-related keywords from text.
    
    Args:
        text: Text content
        
    Returns:
        List of industry keywords
    """
    # Common Australian industry keywords
    industry_keywords = [
        # Manufacturing
        'manufacturing', 'factory', 'production', 'assembly', 'fabrication',
        
        # Construction
        'construction', 'building', 'contractor', 'builder', 'renovation',
        
        # Professional Services
        'consulting', 'advisory', 'professional', 'services', 'legal', 'accounting',
        
        # Technology
        'technology', 'software', 'IT', 'digital', 'tech', 'development',
        
        # Healthcare
        'medical', 'healthcare', 'dental', 'clinic', 'hospital', 'pharmacy',
        
        # Retail
        'retail', 'shop', 'store', 'sales', 'merchandise', 'wholesale',
        
        # Finance
        'finance', 'banking', 'investment', 'insurance', 'financial',
        
        # Education
        'education', 'training', 'school', 'university', 'learning',
        
        # Transport
        'transport', 'logistics', 'freight', 'delivery', 'shipping',
        
        # Agriculture
        'agriculture', 'farming', 'agricultural', 'rural', 'livestock'
    ]
    
    text_lower = text.lower()
    found_keywords = []
    
    for keyword in industry_keywords:
        if keyword in text_lower:
            found_keywords.append(keyword)
    
    return found_keywords


def standardize_address(address: str) -> str:
    """
    Standardize Australian address format.
    
    Args:
        address: Raw address string
        
    Returns:
        Standardized address
    """
    if not address:
        return ""
    
    # Standardize common abbreviations
    abbreviations = {
        r'\bstreet\b': 'St',
        r'\broad\b': 'Rd',
        r'\bavenue\b': 'Ave',
        r'\bdrive\b': 'Dr',
        r'\blane\b': 'Ln',
        r'\bplace\b': 'Pl',
        r'\bcourt\b': 'Ct',
        r'\bcrescent\b': 'Cres',
        r'\bclose\b': 'Cl',
        r'\bparade\b': 'Pde',
    }
    
    standardized = address
    for full_form, abbrev in abbreviations.items():
        standardized = re.sub(full_form, abbrev, standardized, flags=re.IGNORECASE)
    
    # Normalize whitespace
    standardized = re.sub(r'\s+', ' ', standardized).strip()
    
    return standardized


if __name__ == "__main__":
    # Test functions
    test_name1 = "ABC Manufacturing Pty Ltd"
    test_name2 = "ABC Mfg Company"
    
    print(f"Normalized 1: {normalize_company_name(test_name1)}")
    print(f"Normalized 2: {normalize_company_name(test_name2)}")
    print(f"Similarity: {calculate_string_similarity(test_name1, test_name2):.3f}")
    
    test_text = """
    Welcome to ABC Manufacturing Pty Ltd. Contact us at info@abc-mfg.com.au 
    or call (02) 9876 5432. Visit our LinkedIn at linkedin.com/company/abc-manufacturing.
    Our address is 123 Industrial Road, Sydney NSW 2000.
    ABN: 12 345 678 901
    """
    
    info = extract_company_info(test_text)
    print(f"Extracted info: {info}")
    
    abn = extract_australian_business_number(test_text)
    print(f"ABN: {abn}, Valid: {validate_abn(abn) if abn else False}")