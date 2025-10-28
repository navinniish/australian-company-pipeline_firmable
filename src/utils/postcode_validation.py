"""
Enhanced postcode validation for Australian addresses.
Provides comprehensive validation, correction, and geographic insights.
"""

import re
import logging
from typing import Dict, List, Optional, Tuple, Set
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)


class PostcodeStatus(Enum):
    """Postcode validation status."""
    VALID = "valid"
    INVALID_FORMAT = "invalid_format"
    INVALID_RANGE = "invalid_range"
    DEPRECATED = "deprecated"
    STATE_MISMATCH = "state_mismatch"
    CORRECTED = "corrected"


@dataclass
class PostcodeValidation:
    """Result of postcode validation."""
    original_postcode: str
    corrected_postcode: Optional[str]
    status: PostcodeStatus
    state: Optional[str]
    locality: Optional[str]
    confidence: float
    suggestions: List[str]
    error_message: Optional[str] = None


class AustralianPostcodeValidator:
    """
    Enhanced validator for Australian postcodes with correction capabilities.
    Includes comprehensive state mapping, common error detection, and suggestions.
    """
    
    def __init__(self):
        """Initialize with Australian postcode data."""
        self.state_ranges = self._load_state_ranges()
        self.special_postcodes = self._load_special_postcodes()
        self.common_corrections = self._load_common_corrections()
        self.deprecated_postcodes = self._load_deprecated_postcodes()
        
    def validate_postcode(self, postcode: str, state: Optional[str] = None) -> PostcodeValidation:
        """
        Comprehensive postcode validation with correction suggestions.
        
        Args:
            postcode: Postcode to validate
            state: Optional state to check against
            
        Returns:
            PostcodeValidation result
        """
        if not postcode:
            return PostcodeValidation(
                original_postcode="",
                corrected_postcode=None,
                status=PostcodeStatus.INVALID_FORMAT,
                state=None,
                locality=None,
                confidence=0.0,
                suggestions=[],
                error_message="Empty postcode"
            )
        
        # Clean and normalize postcode
        cleaned = self._clean_postcode(postcode)
        
        # Check format
        if not self._is_valid_format(cleaned):
            suggestions = self._generate_format_suggestions(postcode)
            return PostcodeValidation(
                original_postcode=postcode,
                corrected_postcode=None,
                status=PostcodeStatus.INVALID_FORMAT,
                state=None,
                locality=None,
                confidence=0.0,
                suggestions=suggestions,
                error_message=f"Invalid format: '{postcode}' should be 4 digits"
            )
        
        # Check if deprecated
        if cleaned in self.deprecated_postcodes:
            replacement = self.deprecated_postcodes[cleaned]
            return PostcodeValidation(
                original_postcode=postcode,
                corrected_postcode=replacement,
                status=PostcodeStatus.DEPRECATED,
                state=self._get_state_for_postcode(replacement),
                locality=None,
                confidence=0.9,
                suggestions=[replacement],
                error_message=f"Deprecated postcode, use {replacement}"
            )
        
        # Check common corrections
        if cleaned in self.common_corrections:
            corrected = self.common_corrections[cleaned]
            return PostcodeValidation(
                original_postcode=postcode,
                corrected_postcode=corrected,
                status=PostcodeStatus.CORRECTED,
                state=self._get_state_for_postcode(corrected),
                locality=None,
                confidence=0.8,
                suggestions=[corrected],
                error_message=f"Common error detected, likely meant {corrected}"
            )
        
        # Check range validity
        postcode_state = self._get_state_for_postcode(cleaned)
        if not postcode_state:
            suggestions = self._generate_range_suggestions(cleaned)
            return PostcodeValidation(
                original_postcode=postcode,
                corrected_postcode=None,
                status=PostcodeStatus.INVALID_RANGE,
                state=None,
                locality=None,
                confidence=0.0,
                suggestions=suggestions,
                error_message=f"Postcode {cleaned} not in valid Australian range"
            )
        
        # Check state consistency if provided
        if state and state.upper() != postcode_state:
            # Try to find correct postcode for the state
            suggested_postcodes = self._find_postcodes_for_state(state.upper())
            return PostcodeValidation(
                original_postcode=postcode,
                corrected_postcode=None,
                status=PostcodeStatus.STATE_MISMATCH,
                state=postcode_state,
                locality=None,
                confidence=0.2,
                suggestions=suggested_postcodes[:5],  # Top 5 suggestions
                error_message=f"Postcode {cleaned} belongs to {postcode_state}, not {state.upper()}"
            )
        
        # Valid postcode
        return PostcodeValidation(
            original_postcode=postcode,
            corrected_postcode=None,
            status=PostcodeStatus.VALID,
            state=postcode_state,
            locality=None,  # Would need Australia Post API for localities
            confidence=1.0,
            suggestions=[],
            error_message=None
        )
    
    def batch_validate_postcodes(self, postcodes: List[str], 
                                states: Optional[List[str]] = None) -> List[PostcodeValidation]:
        """
        Validate multiple postcodes in batch.
        
        Args:
            postcodes: List of postcodes to validate
            states: Optional list of corresponding states
            
        Returns:
            List of validation results
        """
        if states and len(states) != len(postcodes):
            states = None
        
        results = []
        for i, postcode in enumerate(postcodes):
            state = states[i] if states else None
            result = self.validate_postcode(postcode, state)
            results.append(result)
        
        return results
    
    def generate_validation_report(self, postcodes: List[str], 
                                 states: Optional[List[str]] = None) -> Dict:
        """
        Generate comprehensive validation report.
        
        Args:
            postcodes: List of postcodes to validate
            states: Optional list of corresponding states
            
        Returns:
            Validation report with statistics
        """
        results = self.batch_validate_postcodes(postcodes, states)
        
        # Calculate statistics
        total = len(results)
        status_counts = {}
        state_distribution = {}
        corrections_made = []
        
        for result in results:
            # Count statuses
            status = result.status.value
            status_counts[status] = status_counts.get(status, 0) + 1
            
            # Count states
            if result.state:
                state_distribution[result.state] = state_distribution.get(result.state, 0) + 1
            
            # Track corrections
            if result.corrected_postcode:
                corrections_made.append({
                    "original": result.original_postcode,
                    "corrected": result.corrected_postcode,
                    "reason": result.status.value
                })
        
        # Calculate rates
        valid_rate = status_counts.get("valid", 0) / total if total > 0 else 0
        error_rate = (total - status_counts.get("valid", 0)) / total if total > 0 else 0
        
        return {
            "summary": {
                "total_postcodes": total,
                "valid_postcodes": status_counts.get("valid", 0),
                "invalid_postcodes": total - status_counts.get("valid", 0),
                "valid_rate": round(valid_rate, 3),
                "error_rate": round(error_rate, 3)
            },
            "status_breakdown": status_counts,
            "state_distribution": state_distribution,
            "corrections_applied": len(corrections_made),
            "correction_details": corrections_made,
            "top_errors": self._get_top_error_types(results),
            "recommendations": self._generate_recommendations(results)
        }
    
    def _clean_postcode(self, postcode: str) -> str:
        """Clean and normalize postcode."""
        if not postcode:
            return ""
        
        # Remove whitespace and non-digits
        cleaned = re.sub(r'[^\d]', '', str(postcode))
        
        # Pad with zeros if needed (for postcodes like 800 -> 0800)
        if len(cleaned) == 3 and cleaned.startswith(('1', '2', '3', '4', '5', '6', '7', '8')):
            cleaned = '0' + cleaned
        
        return cleaned
    
    def _is_valid_format(self, postcode: str) -> bool:
        """Check if postcode has valid format."""
        return bool(postcode and len(postcode) == 4 and postcode.isdigit())
    
    def _get_state_for_postcode(self, postcode: str) -> Optional[str]:
        """Get state for a given postcode."""
        if not self._is_valid_format(postcode):
            return None
        
        postcode_int = int(postcode)
        
        for state, ranges in self.state_ranges.items():
            for start, end in ranges:
                if start <= postcode_int <= end:
                    return state
        
        # Check special postcodes
        if postcode in self.special_postcodes:
            return self.special_postcodes[postcode]
        
        return None
    
    def _find_postcodes_for_state(self, state: str) -> List[str]:
        """Find common postcodes for a given state."""
        if state not in self.state_ranges:
            return []
        
        # Return some common postcodes for the state
        common_postcodes = []
        for start, end in self.state_ranges[state]:
            # Add some representative postcodes from each range
            for pc in range(start, min(start + 10, end + 1)):
                common_postcodes.append(f"{pc:04d}")
            if len(common_postcodes) >= 20:
                break
        
        return common_postcodes[:10]
    
    def _generate_format_suggestions(self, postcode: str) -> List[str]:
        """Generate suggestions for format errors."""
        suggestions = []
        
        # Extract digits
        digits = re.sub(r'[^\d]', '', str(postcode))
        
        if len(digits) == 3:
            # Might be missing leading zero
            suggestions.append('0' + digits)
        elif len(digits) == 5:
            # Might have extra digit
            suggestions.append(digits[:4])
            suggestions.append(digits[1:])
        elif len(digits) > 5:
            # Try different 4-digit combinations
            suggestions.append(digits[:4])
            suggestions.append(digits[-4:])
        
        return suggestions[:3]
    
    def _generate_range_suggestions(self, postcode: str) -> List[str]:
        """Generate suggestions for range errors."""
        if not self._is_valid_format(postcode):
            return []
        
        postcode_int = int(postcode)
        suggestions = []
        
        # Find closest valid ranges
        min_distance = float('inf')
        closest_ranges = []
        
        for state, ranges in self.state_ranges.items():
            for start, end in ranges:
                if start <= postcode_int <= end:
                    continue  # Already checked
                
                distance = min(abs(start - postcode_int), abs(end - postcode_int))
                if distance < min_distance:
                    min_distance = distance
                    closest_ranges = [(start, end, state)]
                elif distance == min_distance:
                    closest_ranges.append((start, end, state))
        
        # Generate suggestions from closest ranges
        for start, end, state in closest_ranges[:3]:
            if postcode_int < start:
                suggestions.append(f"{start:04d}")
            else:
                suggestions.append(f"{end:04d}")
        
        return suggestions
    
    def _get_top_error_types(self, results: List[PostcodeValidation]) -> List[Dict]:
        """Get most common error types."""
        error_counts = {}
        
        for result in results:
            if result.status != PostcodeStatus.VALID:
                error_type = result.status.value
                error_counts[error_type] = error_counts.get(error_type, 0) + 1
        
        # Sort by frequency
        sorted_errors = sorted(error_counts.items(), key=lambda x: x[1], reverse=True)
        
        return [
            {"error_type": error_type, "count": count, "percentage": round(count/len(results)*100, 1)}
            for error_type, count in sorted_errors
        ]
    
    def _generate_recommendations(self, results: List[PostcodeValidation]) -> List[str]:
        """Generate recommendations based on validation results."""
        recommendations = []
        
        total = len(results)
        invalid_count = sum(1 for r in results if r.status != PostcodeStatus.VALID)
        
        if invalid_count > 0:
            error_rate = invalid_count / total
            
            if error_rate > 0.1:
                recommendations.append(f"High error rate ({error_rate:.1%}). Consider implementing input validation.")
            
            # Check for common patterns
            format_errors = sum(1 for r in results if r.status == PostcodeStatus.INVALID_FORMAT)
            if format_errors > total * 0.05:
                recommendations.append("Many format errors detected. Implement client-side format checking.")
            
            range_errors = sum(1 for r in results if r.status == PostcodeStatus.INVALID_RANGE)
            if range_errors > total * 0.02:
                recommendations.append("Invalid range errors found. Consider using postcode lookup service.")
            
            state_mismatches = sum(1 for r in results if r.status == PostcodeStatus.STATE_MISMATCH)
            if state_mismatches > total * 0.03:
                recommendations.append("State-postcode mismatches detected. Implement cross-validation.")
        
        return recommendations
    
    def _load_state_ranges(self) -> Dict[str, List[Tuple[int, int]]]:
        """Load Australian postcode ranges by state."""
        return {
            "NSW": [(1000, 1999), (2000, 2599), (2619, 2898), (2921, 2999)],
            "ACT": [(200, 299), (2600, 2618), (2900, 2920)],
            "VIC": [(3000, 3999), (8000, 8999)],
            "QLD": [(4000, 4999), (9000, 9999)],
            "SA": [(5000, 5999)],
            "WA": [(6000, 6797), (6800, 6999)],
            "TAS": [(7000, 7799), (7800, 7999)],
            "NT": [(800, 899), (900, 999)]
        }
    
    def _load_special_postcodes(self) -> Dict[str, str]:
        """Load special postcodes (PO Boxes, overseas, etc.)."""
        return {
            # Major city special postcodes
            "0872": "NT",  # Darwin
            "0909": "NT",  # Palmerston
            # Add more special cases as needed
        }
    
    def _load_common_corrections(self) -> Dict[str, str]:
        """Load common postcode corrections."""
        return {
            # Common typos and corrections
            "2OOO": "2000",  # O instead of 0
            "3OOO": "3000",
            "4OOO": "4000",
            "5OOO": "5000",
            "6OOO": "6000",
            "7OOO": "7000",
            # More common corrections can be added based on data analysis
        }
    
    def _load_deprecated_postcodes(self) -> Dict[str, str]:
        """Load deprecated postcodes and their replacements."""
        return {
            # Example deprecated postcodes
            # These would be based on Australia Post updates
        }


# Integration with existing pipeline
class PostcodeEnhancer:
    """
    Enhance existing company data with improved postcode validation.
    Integrates with the main data pipeline to fix postcode errors.
    """
    
    def __init__(self):
        self.validator = AustralianPostcodeValidator()
    
    async def enhance_company_data(self, companies: List[Dict]) -> List[Dict]:
        """
        Enhance company data with validated postcodes.
        
        Args:
            companies: List of company records
            
        Returns:
            Enhanced company records
        """
        enhanced_companies = []
        
        for company in companies:
            enhanced = company.copy()
            
            # Extract postcode and state
            address = company.get("address", {})
            postcode = address.get("postcode")
            state = address.get("state")
            
            if postcode:
                validation = self.validator.validate_postcode(postcode, state)
                
                # Update postcode if correction available
                if validation.corrected_postcode:
                    enhanced["address"]["postcode"] = validation.corrected_postcode
                    enhanced["data_quality_notes"] = enhanced.get("data_quality_notes", [])
                    enhanced["data_quality_notes"].append({
                        "field": "postcode",
                        "original": postcode,
                        "corrected": validation.corrected_postcode,
                        "reason": validation.status.value
                    })
                
                # Update state if determined from postcode
                if validation.state and not state:
                    enhanced["address"]["state"] = validation.state
                    enhanced["data_quality_notes"] = enhanced.get("data_quality_notes", [])
                    enhanced["data_quality_notes"].append({
                        "field": "state",
                        "inferred_from": "postcode",
                        "value": validation.state
                    })
                
                # Add validation metadata
                enhanced["postcode_validation"] = {
                    "status": validation.status.value,
                    "confidence": validation.confidence,
                    "error_message": validation.error_message
                }
                
                # Adjust data quality score based on postcode validation
                if validation.status == PostcodeStatus.VALID:
                    enhanced["postcode_quality_bonus"] = 0.05
                elif validation.status == PostcodeStatus.CORRECTED:
                    enhanced["postcode_quality_bonus"] = 0.02
                else:
                    enhanced["postcode_quality_penalty"] = -0.03
            
            enhanced_companies.append(enhanced)
        
        return enhanced_companies


# Example usage and testing
if __name__ == "__main__":
    import asyncio
    
    async def demo():
        validator = AustralianPostcodeValidator()
        
        # Test various postcodes
        test_postcodes = [
            ("2000", "NSW"),   # Valid Sydney
            ("3000", "VIC"),   # Valid Melbourne
            ("2OOO", "NSW"),   # Common typo
            ("9999", "QLD"),   # Valid range
            ("1234", None),    # Invalid range
            ("12345", None),   # Invalid format
            ("800", "NT"),     # Missing zero
            ("2000", "VIC"),   # State mismatch
        ]
        
        print("🔍 Postcode Validation Results:")
        print("=" * 60)
        
        for postcode, state in test_postcodes:
            result = validator.validate_postcode(postcode, state)
            print(f"Postcode: {postcode:>6} State: {state or 'None':>3} → Status: {result.status.value:>15}")
            if result.corrected_postcode:
                print(f"         Correction: {result.corrected_postcode}")
            if result.suggestions:
                print(f"         Suggestions: {', '.join(result.suggestions)}")
            if result.error_message:
                print(f"         Error: {result.error_message}")
            print()
        
        # Generate report
        postcodes_only = [pc for pc, _ in test_postcodes]
        report = validator.generate_validation_report(postcodes_only)
        
        print("📊 Validation Report Summary:")
        print(f"Total: {report['summary']['total_postcodes']}")
        print(f"Valid: {report['summary']['valid_postcodes']} ({report['summary']['valid_rate']:.1%})")
        print(f"Invalid: {report['summary']['invalid_postcodes']} ({report['summary']['error_rate']:.1%})")
        
        if report['correction_details']:
            print("\n🔧 Corrections Applied:")
            for correction in report['correction_details']:
                print(f"  {correction['original']} → {correction['corrected']} ({correction['reason']})")
    
    asyncio.run(demo())