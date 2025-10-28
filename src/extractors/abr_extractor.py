"""
Australian Business Register (ABR) data extractor.
Processes bulk extract XML files from data.gov.au to extract company information.
"""

import asyncio
import logging
from typing import List, Dict, Optional, Iterator
from dataclasses import dataclass
from pathlib import Path
import xml.etree.ElementTree as ET
import zipfile
import requests
from datetime import datetime, date
import pandas as pd
from sqlalchemy import create_engine
import json

from ..utils.database import DatabaseManager
from ..utils.text_processing import normalize_company_name

logger = logging.getLogger(__name__)

@dataclass
class ABREntityData:
    """Data structure for ABR entity information."""
    abn: str
    entity_name: str
    entity_type: Optional[str]
    entity_status: Optional[str]
    entity_type_code: Optional[str]
    entity_status_code: Optional[str]
    address_state_code: Optional[str]
    address_postcode: Optional[str]
    address_line_1: Optional[str]
    address_line_2: Optional[str]
    address_suburb: Optional[str]
    address_state: Optional[str]
    start_date: Optional[date]
    registration_date: Optional[date]
    last_updated_date: Optional[date]
    gst_status: Optional[str]
    dgr_status: Optional[str]
    acn: Optional[str]
    trading_names: List[str]
    business_names: List[str]
    raw_xml: str


class ABRExtractor:
    """
    Extracts company data from Australian Business Register bulk XML files.
    Handles large volumes (100k+ records) efficiently with streaming processing.
    """
    
    def __init__(self, db_manager: DatabaseManager, download_dir: str = "./data/abr"):
        self.db_manager = db_manager
        self.download_dir = Path(download_dir)
        self.download_dir.mkdir(parents=True, exist_ok=True)
        
        # ABR bulk extract URLs
        self.abr_base_url = "https://data.gov.au/data/dataset/5bd7fcab-e315-42cb-8dcc-22b8f8460c07"
        self.bulk_extract_urls = [
            "https://data.gov.au/data/dataset/5bd7fcab-e315-42cb-8dcc-22b8f8460c07/resource/3c6d7c6c-7db3-4fb2-8a30-8b71c7ccdfec/download/20241201_bulk_extract.zip",
            # Add more URLs as needed for comprehensive coverage
        ]
        
        # XML namespace for ABR data
        self.ns = {
            'abr': 'http://abr.business.gov.au/abrxmlsearch/',
            'dt': 'http://abr.business.gov.au/abrxmlsearch/datatypes'
        }
    
    async def extract_abr_data(self, max_records: int = 1000000) -> List[ABREntityData]:
        """
        Main extraction method for ABR data.
        
        Args:
            max_records: Maximum number of records to process
            
        Returns:
            List of ABREntityData objects
        """
        logger.info(f"Starting ABR extraction for max {max_records} records")
        
        all_entities = []
        records_processed = 0
        
        # Download and process each bulk extract file
        for url in self.bulk_extract_urls:
            if records_processed >= max_records:
                break
                
            logger.info(f"Processing bulk extract: {url}")
            
            # Download file
            file_path = await self._download_bulk_extract(url)
            if not file_path:
                continue
            
            # Process XML files in the archive
            entities = await self._process_bulk_extract(
                file_path, 
                max_records - records_processed
            )
            
            all_entities.extend(entities)
            records_processed += len(entities)
            
            logger.info(f"Processed {records_processed} total records")
            
            # Save progress periodically
            if len(entities) > 0:
                await self._save_batch_to_staging(entities)
        
        logger.info(f"ABR extraction complete. Total entities: {len(all_entities)}")
        return all_entities
    
    async def _download_bulk_extract(self, url: str) -> Optional[Path]:
        """
        Download ABR bulk extract file.
        
        Args:
            url: Download URL
            
        Returns:
            Path to downloaded file or None if failed
        """
        try:
            filename = Path(url).name
            file_path = self.download_dir / filename
            
            # Skip if already downloaded
            if file_path.exists():
                logger.info(f"File already exists: {file_path}")
                return file_path
            
            logger.info(f"Downloading {url}")
            response = requests.get(url, stream=True, timeout=300)
            response.raise_for_status()
            
            with open(file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            logger.info(f"Downloaded: {file_path} ({file_path.stat().st_size / 1024 / 1024:.1f} MB)")
            return file_path
            
        except Exception as e:
            logger.error(f"Failed to download {url}: {e}")
            return None
    
    async def _process_bulk_extract(self, zip_path: Path, max_records: int) -> List[ABREntityData]:
        """
        Process XML files within a bulk extract ZIP file.
        
        Args:
            zip_path: Path to ZIP file
            max_records: Maximum records to process
            
        Returns:
            List of extracted ABR entities
        """
        entities = []
        
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_file:
                xml_files = [f for f in zip_file.namelist() if f.endswith('.xml')]
                
                for xml_filename in xml_files:
                    if len(entities) >= max_records:
                        break
                        
                    logger.info(f"Processing XML file: {xml_filename}")
                    
                    with zip_file.open(xml_filename) as xml_file:
                        file_entities = self._parse_xml_file(
                            xml_file, 
                            max_records - len(entities)
                        )
                        entities.extend(file_entities)
                        
                    logger.info(f"Processed {len(entities)} entities so far")
                    
        except Exception as e:
            logger.error(f"Error processing {zip_path}: {e}")
        
        return entities
    
    def _parse_xml_file(self, xml_file, max_records: int) -> List[ABREntityData]:
        """
        Parse individual XML file using streaming to handle large files.
        
        Args:
            xml_file: XML file object
            max_records: Maximum records to extract
            
        Returns:
            List of ABR entities from the file
        """
        entities = []
        
        try:
            # Use iterparse for memory-efficient processing of large XML files
            for event, elem in ET.iterparse(xml_file, events=('start', 'end')):
                if event == 'end' and elem.tag.endswith('ABN'):
                    if len(entities) >= max_records:
                        break
                        
                    entity = self._parse_abn_element(elem)
                    if entity:
                        entities.append(entity)
                    
                    # Clear element to free memory
                    elem.clear()
                    
        except ET.ParseError as e:
            logger.error(f"XML parsing error: {e}")
        except Exception as e:
            logger.error(f"Error parsing XML file: {e}")
        
        return entities
    
    def _parse_abn_element(self, abn_elem) -> Optional[ABREntityData]:
        """
        Parse individual ABN XML element into ABREntityData.
        
        Args:
            abn_elem: XML element for an ABN record
            
        Returns:
            ABREntityData object or None if parsing failed
        """
        try:
            # Extract ABN
            abn = self._get_text(abn_elem, './/dt:identifierValue')
            if not abn or len(abn) != 11:
                return None
            
            # Extract entity details
            entity_details = abn_elem.find('.//abr:entityDetails', self.ns)
            if entity_details is None:
                return None
            
            entity_name = self._get_text(entity_details, './/abr:entityName')
            entity_type = self._get_text(entity_details, './/abr:entityTypeText')
            entity_status = self._get_text(entity_details, './/abr:entityStatusText')
            entity_type_code = self._get_text(entity_details, './/abr:entityTypeCode')
            entity_status_code = self._get_text(entity_details, './/abr:entityStatusCode')
            
            # Extract ACN if present
            acn = self._get_text(abn_elem, './/dt:identifierValue[../dt:identifierType="ACN"]')
            
            # Extract address information
            address = self._parse_address(entity_details)
            
            # Extract dates
            start_date = self._parse_date(self._get_text(entity_details, './/abr:effectiveFrom'))
            registration_date = self._parse_date(self._get_text(abn_elem, './/dt:recordLastUpdatedDate'))
            last_updated_date = self._parse_date(self._get_text(abn_elem, './/dt:recordLastUpdatedDate'))
            
            # Extract GST and DGR status
            gst_status = self._get_text(abn_elem, './/abr:gstStatusText')
            dgr_status = self._get_text(abn_elem, './/abr:dgrStatusText')
            
            # Extract trading names and business names
            trading_names = self._extract_names(abn_elem, './/abr:tradingName')
            business_names = self._extract_names(abn_elem, './/abr:businessName')
            
            return ABREntityData(
                abn=abn,
                entity_name=entity_name,
                entity_type=entity_type,
                entity_status=entity_status,
                entity_type_code=entity_type_code,
                entity_status_code=entity_status_code,
                address_state_code=address.get('state_code'),
                address_postcode=address.get('postcode'),
                address_line_1=address.get('line_1'),
                address_line_2=address.get('line_2'),
                address_suburb=address.get('suburb'),
                address_state=address.get('state'),
                start_date=start_date,
                registration_date=registration_date,
                last_updated_date=last_updated_date,
                gst_status=gst_status,
                dgr_status=dgr_status,
                acn=acn,
                trading_names=trading_names,
                business_names=business_names,
                raw_xml=ET.tostring(abn_elem, encoding='unicode')
            )
            
        except Exception as e:
            logger.warning(f"Error parsing ABN element: {e}")
            return None
    
    def _parse_address(self, entity_details) -> Dict[str, Optional[str]]:
        """Extract address information from entity details."""
        address = {}
        
        # Find main business physical address
        address_elem = entity_details.find('.//abr:mainBusinessPhysicalAddress', self.ns)
        if address_elem is not None:
            address = {
                'line_1': self._get_text(address_elem, './/abr:addressLine1'),
                'line_2': self._get_text(address_elem, './/abr:addressLine2'),
                'suburb': self._get_text(address_elem, './/abr:suburb'),
                'state': self._get_text(address_elem, './/abr:stateText'),
                'state_code': self._get_text(address_elem, './/abr:stateCode'),
                'postcode': self._get_text(address_elem, './/abr:postcode')
            }
        
        return address
    
    def _extract_names(self, abn_elem, xpath: str) -> List[str]:
        """Extract list of names (trading names or business names)."""
        names = []
        name_elements = abn_elem.findall(xpath, self.ns)
        
        for elem in name_elements:
            name = self._get_text(elem, './/abr:organisationName')
            if name:
                names.append(name)
        
        return names
    
    def _get_text(self, parent_elem, xpath: str) -> Optional[str]:
        """Safely get text content from XML element."""
        if parent_elem is None:
            return None
            
        elem = parent_elem.find(xpath, self.ns)
        if elem is not None and elem.text:
            return elem.text.strip()
        return None
    
    def _parse_date(self, date_str: Optional[str]) -> Optional[date]:
        """Parse date string into date object."""
        if not date_str:
            return None
            
        try:
            # Handle various date formats
            for fmt in ['%Y-%m-%d', '%d/%m/%Y', '%Y-%m-%d %H:%M:%S']:
                try:
                    return datetime.strptime(date_str, fmt).date()
                except ValueError:
                    continue
        except Exception:
            pass
        
        return None
    
    async def _save_batch_to_staging(self, batch_data: List[ABREntityData]):
        """Save a batch of ABR data to staging table."""
        if not batch_data:
            return
            
        records = []
        for entity in batch_data:
            records.append({
                'abn': entity.abn,
                'entity_name': entity.entity_name,
                'entity_type': entity.entity_type,
                'entity_status': entity.entity_status,
                'entity_type_code': entity.entity_type_code,
                'entity_status_code': entity.entity_status_code,
                'address_state_code': entity.address_state_code,
                'address_postcode': entity.address_postcode,
                'address_line_1': entity.address_line_1,
                'address_line_2': entity.address_line_2,
                'address_suburb': entity.address_suburb,
                'address_state': entity.address_state,
                'start_date': entity.start_date,
                'registration_date': entity.registration_date,
                'last_updated_date': entity.last_updated_date,
                'gst_status': entity.gst_status,
                'dgr_status': entity.dgr_status,
                'acn': entity.acn,
                'trading_names': entity.trading_names,
                'business_names': entity.business_names,
                'raw_xml': entity.raw_xml
            })
        
        await self.db_manager.bulk_insert('staging.abr_raw', records)
        logger.info(f"Saved {len(records)} ABR records to staging")


# CLI interface for testing
if __name__ == "__main__":
    import asyncio
    from ..utils.config import Config
    
    async def main():
        config = Config()
        db_manager = DatabaseManager(config.database_url)
        
        extractor = ABRExtractor(db_manager)
        
        # Extract sample data
        entities = await extractor.extract_abr_data(max_records=10000)
        print(f"Extracted {len(entities)} ABR entities")
    
    asyncio.run(main())