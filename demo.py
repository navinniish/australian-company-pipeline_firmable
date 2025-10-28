#!/usr/bin/env python3
"""
Australian Company Pipeline - Demo Script
Demonstrates the key functionality without requiring real data sources or database.
"""

import asyncio
import json
from datetime import datetime
from src.utils.config import Config
from src.utils.llm_client import LLMClient
from src.utils.text_processing import (
    normalize_company_name, 
    validate_abn, 
    extract_company_info,
    calculate_string_similarity
)
from src.pipeline.etl_pipeline import ETLPipeline

def print_banner(title):
    """Print a formatted banner."""
    print("\n" + "="*60)
    print(f"  {title}")
    print("="*60)

async def demo_configuration():
    """Demo configuration loading."""
    print_banner("CONFIGURATION DEMO")
    
    config = Config()
    print(f"✅ Configuration loaded successfully")
    print(f"   🗄️  Database: {config.database.database} @ {config.database.host}:{config.database.port}")
    print(f"   🤖 LLM Provider: {config.llm.provider}")
    print(f"   📊 Max CC Records: {config.extractor.common_crawl_max_records:,}")
    print(f"   📋 Max ABR Records: {config.extractor.abr_max_records:,}")
    print(f"   🎯 Batch Size: {config.extractor.batch_size}")
    
    return config

async def demo_text_processing():
    """Demo text processing capabilities."""
    print_banner("TEXT PROCESSING DEMO")
    
    # Company name normalization
    companies = [
        "ABC Manufacturing Pty Ltd",
        "XYZ Professional Services Company", 
        "Tech Innovations Incorporated",
        "Sydney Building Contractors Pty Limited"
    ]
    
    print("🏢 Company Name Normalization:")
    for company in companies:
        normalized = normalize_company_name(company)
        print(f"   {company:<40} → {normalized}")
    
    # ABN validation
    print(f"\n🔢 ABN Validation:")
    test_abns = [
        "53004085616",  # Valid ABN
        "12345678901",  # Invalid ABN
        "51824753556",  # Valid ABN
        "1234567890"    # Invalid format
    ]
    
    for abn in test_abns:
        is_valid = validate_abn(abn)
        status = "✅ Valid" if is_valid else "❌ Invalid"
        print(f"   {abn} → {status}")
    
    # Contact extraction
    print(f"\n📧 Contact Information Extraction:")
    sample_text = """
    Welcome to Sydney Tech Solutions Pty Ltd! We're Australia's leading technology consultancy.
    
    Contact us:
    📧 Email: hello@sydneytech.com.au
    📞 Phone: (02) 8765 4321 
    🏢 Address: Level 15, 100 George Street, Sydney NSW 2000
    
    Follow us:
    LinkedIn: linkedin.com/company/sydney-tech
    Twitter: twitter.com/sydneytech
    
    ABN: 53 004 085 616
    """
    
    extracted = extract_company_info(sample_text)
    print(f"   📧 Emails: {extracted['emails']}")
    print(f"   📞 Phones: {extracted['phones']}")
    print(f"   🏢 Addresses: {len(extracted['addresses'])} found")
    print(f"   📱 Social Links: {list(extracted['social_links'].keys())}")
    
    # String similarity
    print(f"\n🎯 Company Name Matching:")
    name_pairs = [
        ("ABC Manufacturing Pty Ltd", "ABC Mfg Company"),
        ("Sydney Tech Solutions", "Sydney Technology Solutions Pty Ltd"),
        ("XYZ Corp", "123 Industries")
    ]
    
    for name1, name2 in name_pairs:
        similarity = calculate_string_similarity(name1, name2)
        print(f"   '{name1}' vs '{name2}' → {similarity:.2f}")

async def demo_llm_integration(config):
    """Demo LLM integration for entity matching."""
    print_banner("LLM INTEGRATION DEMO")
    
    client = LLMClient(config)
    
    # Company information extraction
    print("🤖 Company Information Extraction:")
    website_prompt = """
    You are analyzing an Australian company website to extract key business information.
    
    Website URL: https://precisionengineering.com.au
    Page Title: Precision Engineering Solutions - Custom Manufacturing Since 1985
    Meta Description: Leading Australian precision engineering company specializing in custom manufacturing, CNC machining, and industrial automation solutions.
    
    Please extract the following information and return as JSON:
    {
        "company_name": "Official company name",
        "industry": "Primary industry/business sector", 
        "contact_info": {
            "email": "Contact email if found",
            "phone": "Phone number if found",
            "address": "Physical address if found"
        },
        "confidence": "Confidence score 0.0-1.0"
    }
    """
    
    extraction_result = await client.chat_completion(website_prompt)
    print(f"   Response: {extraction_result}")
    
    # Entity matching verification
    print(f"\n🔍 Entity Matching Verification:")
    matching_prompt = """
    You are an expert in entity matching for Australian business data.
    
    COMMON CRAWL RECORD:
    - Website URL: https://precisionengineering.com.au
    - Company Name: Precision Engineering Solutions
    - Industry: Manufacturing
    
    ABR RECORD:
    - ABN: 53004085616
    - Entity Name: Precision Engineering Solutions Pty Ltd
    - Trading Names: Precision Eng, PES
    - Location: Melbourne VIC 3000
    
    Please analyze and return your response as JSON:
    {
        "is_match": true/false,
        "confidence": 0.0-1.0,
        "reasoning": "Detailed explanation",
        "key_factors": ["list", "of", "matching", "factors"]
    }
    """
    
    matching_result = await client.chat_completion(matching_prompt)
    print(f"   Response: {matching_result}")

async def demo_pipeline_architecture(config):
    """Demo pipeline architecture and components."""
    print_banner("PIPELINE ARCHITECTURE DEMO")
    
    pipeline = ETLPipeline(config)
    
    # Pipeline status
    status = await pipeline.get_pipeline_status()
    print(f"📊 Current Pipeline Status: {status['status']}")
    
    # Component overview
    print(f"\n🏗️ Pipeline Components:")
    print(f"   📥 Extractors:")
    print(f"      • Common Crawl Extractor (Web scraping)")
    print(f"      • ABR Extractor (XML processing)")
    print(f"   ⚙️  Transformers:")
    print(f"      • Data Transformer (LLM-powered)")
    print(f"      • Entity Matcher (Multi-strategy)")
    print(f"   📤 Loaders:")
    print(f"      • Core Data Loader")
    print(f"      • Analytics Generator")
    
    # Sample processing workflow
    print(f"\n🔄 Sample Processing Workflow:")
    
    # Mock some sample data
    sample_cc_record = {
        "website_url": "https://melbournetech.com.au",
        "company_name": "Melbourne Tech Solutions",
        "industry": "Technology",
        "extraction_confidence": 0.85
    }
    
    sample_abr_record = {
        "abn": "51824753556", 
        "entity_name": "Melbourne Technology Solutions Pty Ltd",
        "entity_status": "Active",
        "address_state": "VIC"
    }
    
    print(f"   1. 📊 Common Crawl Data: {sample_cc_record['company_name']}")
    print(f"   2. 🏛️  ABR Data: {sample_abr_record['entity_name']}")
    
    # Demonstrate similarity calculation
    similarity = calculate_string_similarity(
        sample_cc_record['company_name'], 
        sample_abr_record['entity_name']
    )
    print(f"   3. 🎯 Name Similarity: {similarity:.3f}")
    
    # Mock entity matching result
    if similarity > 0.8:
        print(f"   4. ✅ High confidence match - Auto-merged")
    elif similarity > 0.6:
        print(f"   4. 🤖 Medium confidence - LLM verification needed")
    else:
        print(f"   4. ❌ Low confidence - Manual review required")
    
    print(f"   5. 🔄 Data transformation and cleaning")
    print(f"   6. 📥 Load to core database tables")

def demo_data_quality_metrics():
    """Demo data quality assessment."""
    print_banner("DATA QUALITY DEMO")
    
    # Sample company records with different quality levels
    sample_companies = [
        {
            "name": "High Quality Corp Pty Ltd",
            "abn": "53004085616",
            "website": "https://highquality.com.au",
            "address": "Level 10, 123 Collins St, Melbourne VIC 3000",
            "email": "info@highquality.com.au",
            "phone": "(03) 9876 5432"
        },
        {
            "name": "Medium Quality Ltd", 
            "abn": "51824753556",
            "website": "https://mediumquality.com.au",
            "address": "Sydney NSW",
            "email": None,
            "phone": None
        },
        {
            "name": "Low Quality Business",
            "abn": None,
            "website": None,
            "address": None,
            "email": None,
            "phone": None
        }
    ]
    
    print("🎯 Data Quality Scoring:")
    
    for i, company in enumerate(sample_companies):
        # Calculate quality score
        score = 0.0
        
        # Core data (40%)
        if company.get("abn"):
            score += 0.15
        if company.get("name"):
            score += 0.15
        if company.get("website"):
            score += 0.10
        
        # Contact info (30%)
        if company.get("email"):
            score += 0.15
        if company.get("phone"):
            score += 0.15
        
        # Address (30%)
        if company.get("address") and "VIC" in company.get("address", ""):
            score += 0.30
        elif company.get("address"):
            score += 0.15
        
        # Quality tier
        if score >= 0.8:
            tier = "🟢 High"
        elif score >= 0.5:
            tier = "🟡 Medium"
        else:
            tier = "🔴 Low"
        
        print(f"   Company {i+1}: {company['name']}")
        print(f"      Quality Score: {score:.2f} ({tier})")
        print(f"      Has ABN: {'✅' if company.get('abn') else '❌'}")
        print(f"      Has Website: {'✅' if company.get('website') else '❌'}")
        print(f"      Has Contact: {'✅' if company.get('email') or company.get('phone') else '❌'}")
        print()

async def main():
    """Main demo function."""
    print("🇦🇺 Australian Company Data Pipeline - Demo")
    print("=" * 60)
    print("This demo showcases the key features and capabilities")
    print("of the Australian Company Data Pipeline project.")
    
    # Configuration demo
    config = await demo_configuration()
    
    # Text processing demo
    await demo_text_processing()
    
    # LLM integration demo
    await demo_llm_integration(config)
    
    # Pipeline architecture demo
    await demo_pipeline_architecture(config)
    
    # Data quality demo
    demo_data_quality_metrics()
    
    # Final summary
    print_banner("DEMO SUMMARY")
    print("✅ All components demonstrated successfully!")
    print("📋 Key Features Shown:")
    print("   • Configuration management with environment variables")
    print("   • Text processing and normalization utilities")
    print("   • ABN validation with checksum algorithm")
    print("   • LLM integration for intelligent entity matching")
    print("   • Pipeline architecture and workflow")
    print("   • Data quality scoring and assessment")
    print()
    print("🚀 Ready for production deployment with:")
    print("   • Real PostgreSQL database")
    print("   • Valid OpenAI/Anthropic API keys")
    print("   • Common Crawl and ABR data sources")
    print()
    print("📖 For full setup instructions, see README.md")
    print("🐙 Repository: https://github.com/navinniish/australian-company-pipeline")

if __name__ == "__main__":
    asyncio.run(main())