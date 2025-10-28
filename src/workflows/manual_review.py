"""
Manual Review Workflow for Medium-Confidence Entity Matches
Provides tools for human reviewers to assess and approve/reject company matches.
"""

import asyncio
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from enum import Enum
import json
import uuid

logger = logging.getLogger(__name__)


class ReviewStatus(Enum):
    """Status of manual review."""
    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"
    NEEDS_INFO = "needs_additional_info"


class ReviewPriority(Enum):
    """Priority level for manual review."""
    HIGH = "high"        # Confidence 0.4-0.6, significant business value
    MEDIUM = "medium"    # Confidence 0.6-0.75, moderate business value
    LOW = "low"         # Confidence 0.75-0.85, low business value


@dataclass
class ReviewItem:
    """Single item requiring manual review."""
    id: str
    common_crawl_record: Dict[str, Any]
    abr_record: Dict[str, Any]
    matching_confidence: float
    llm_reasoning: str
    key_factors: List[str]
    priority: ReviewPriority
    status: ReviewStatus
    created_at: datetime
    reviewer_id: Optional[str] = None
    reviewed_at: Optional[datetime] = None
    reviewer_notes: Optional[str] = None
    final_decision: Optional[bool] = None
    estimated_business_value: Optional[float] = None


class ManualReviewWorkflow:
    """
    Manages the manual review process for medium-confidence entity matches.
    Provides prioritization, assignment, and decision tracking.
    """
    
    def __init__(self, database_client=None):
        """Initialize manual review workflow."""
        self.database_client = database_client
        self.pending_reviews: List[ReviewItem] = []
        self.completed_reviews: List[ReviewItem] = []
        
    async def queue_for_review(self, 
                             common_crawl_record: Dict[str, Any],
                             abr_record: Dict[str, Any],
                             matching_confidence: float,
                             llm_reasoning: str,
                             key_factors: List[str]) -> str:
        """
        Add a medium-confidence match to the manual review queue.
        
        Args:
            common_crawl_record: Company data from Common Crawl
            abr_record: Business registration data from ABR
            matching_confidence: LLM confidence score (0.0-1.0)
            llm_reasoning: LLM's reasoning for the match
            key_factors: List of key matching factors
            
        Returns:
            Review item ID
        """
        # Determine priority based on confidence and business value
        priority = self._calculate_priority(
            matching_confidence, 
            common_crawl_record, 
            abr_record
        )
        
        # Estimate business value
        business_value = self._estimate_business_value(
            common_crawl_record, 
            abr_record
        )
        
        review_item = ReviewItem(
            id=str(uuid.uuid4()),
            common_crawl_record=common_crawl_record,
            abr_record=abr_record,
            matching_confidence=matching_confidence,
            llm_reasoning=llm_reasoning,
            key_factors=key_factors,
            priority=priority,
            status=ReviewStatus.PENDING,
            created_at=datetime.utcnow(),
            estimated_business_value=business_value
        )
        
        self.pending_reviews.append(review_item)
        
        # Store in database if available
        if self.database_client:
            await self._store_review_item(review_item)
        
        logger.info(f"Queued review item {review_item.id} with priority {priority.value}")
        return review_item.id
    
    def get_pending_reviews(self, 
                           priority: Optional[ReviewPriority] = None,
                           limit: int = 50) -> List[ReviewItem]:
        """
        Get pending reviews, optionally filtered by priority.
        
        Args:
            priority: Optional priority filter
            limit: Maximum number of items to return
            
        Returns:
            List of pending review items
        """
        reviews = self.pending_reviews
        
        if priority:
            reviews = [r for r in reviews if r.priority == priority]
        
        # Sort by priority (high first) then by creation date
        priority_order = {ReviewPriority.HIGH: 0, ReviewPriority.MEDIUM: 1, ReviewPriority.LOW: 2}
        reviews.sort(key=lambda x: (priority_order[x.priority], x.created_at))
        
        return reviews[:limit]
    
    async def submit_review_decision(self,
                                   review_id: str,
                                   reviewer_id: str,
                                   decision: bool,
                                   notes: Optional[str] = None) -> bool:
        """
        Submit a review decision for a pending item.
        
        Args:
            review_id: ID of the review item
            reviewer_id: ID of the reviewer
            decision: True for approve, False for reject
            notes: Optional reviewer notes
            
        Returns:
            Success status
        """
        # Find the pending review item
        review_item = None
        for i, item in enumerate(self.pending_reviews):
            if item.id == review_id:
                review_item = self.pending_reviews.pop(i)
                break
        
        if not review_item:
            logger.error(f"Review item {review_id} not found")
            return False
        
        # Update review item with decision
        review_item.status = ReviewStatus.APPROVED if decision else ReviewStatus.REJECTED
        review_item.reviewer_id = reviewer_id
        review_item.reviewed_at = datetime.utcnow()
        review_item.reviewer_notes = notes
        review_item.final_decision = decision
        
        self.completed_reviews.append(review_item)
        
        # Update database if available
        if self.database_client:
            await self._update_review_item(review_item)
        
        logger.info(f"Review {review_id} {('approved' if decision else 'rejected')} by {reviewer_id}")
        return True
    
    def generate_review_summary(self) -> Dict[str, Any]:
        """
        Generate summary statistics for the review workflow.
        
        Returns:
            Summary statistics
        """
        total_pending = len(self.pending_reviews)
        total_completed = len(self.completed_reviews)
        
        # Pending by priority
        pending_by_priority = {
            ReviewPriority.HIGH: len([r for r in self.pending_reviews if r.priority == ReviewPriority.HIGH]),
            ReviewPriority.MEDIUM: len([r for r in self.pending_reviews if r.priority == ReviewPriority.MEDIUM]),
            ReviewPriority.LOW: len([r for r in self.pending_reviews if r.priority == ReviewPriority.LOW])
        }
        
        # Completed decisions
        approved_count = len([r for r in self.completed_reviews if r.final_decision])
        rejected_count = len([r for r in self.completed_reviews if not r.final_decision])
        
        # Average confidence scores
        avg_pending_confidence = sum(r.matching_confidence for r in self.pending_reviews) / max(total_pending, 1)
        avg_completed_confidence = sum(r.matching_confidence for r in self.completed_reviews) / max(total_completed, 1)
        
        return {
            "total_pending": total_pending,
            "total_completed": total_completed,
            "pending_by_priority": {
                "high": pending_by_priority[ReviewPriority.HIGH],
                "medium": pending_by_priority[ReviewPriority.MEDIUM],
                "low": pending_by_priority[ReviewPriority.LOW]
            },
            "completed_decisions": {
                "approved": approved_count,
                "rejected": rejected_count,
                "approval_rate": approved_count / max(total_completed, 1)
            },
            "confidence_scores": {
                "avg_pending": round(avg_pending_confidence, 3),
                "avg_completed": round(avg_completed_confidence, 3)
            },
            "estimated_business_value": {
                "total_pending": sum(r.estimated_business_value or 0 for r in self.pending_reviews),
                "total_completed": sum(r.estimated_business_value or 0 for r in self.completed_reviews)
            }
        }
    
    def export_review_report(self, include_completed: bool = True) -> Dict[str, Any]:
        """
        Export comprehensive review report for analysis.
        
        Args:
            include_completed: Whether to include completed reviews
            
        Returns:
            Detailed review report
        """
        report = {
            "generated_at": datetime.utcnow().isoformat(),
            "summary": self.generate_review_summary(),
            "pending_reviews": []
        }
        
        # Add pending reviews with details
        for review in self.pending_reviews:
            report["pending_reviews"].append({
                "id": review.id,
                "priority": review.priority.value,
                "confidence": review.matching_confidence,
                "business_value": review.estimated_business_value,
                "created_at": review.created_at.isoformat(),
                "common_crawl_company": review.common_crawl_record.get("company_name"),
                "abr_company": review.abr_record.get("entity_name"),
                "llm_reasoning": review.llm_reasoning,
                "key_factors": review.key_factors
            })
        
        if include_completed:
            report["completed_reviews"] = []
            for review in self.completed_reviews:
                report["completed_reviews"].append({
                    "id": review.id,
                    "decision": review.final_decision,
                    "reviewer_id": review.reviewer_id,
                    "reviewed_at": review.reviewed_at.isoformat() if review.reviewed_at else None,
                    "confidence": review.matching_confidence,
                    "business_value": review.estimated_business_value,
                    "reviewer_notes": review.reviewer_notes
                })
        
        return report
    
    def _calculate_priority(self,
                           confidence: float,
                           common_crawl_record: Dict[str, Any],
                           abr_record: Dict[str, Any]) -> ReviewPriority:
        """Calculate priority based on confidence and business factors."""
        
        # Base priority on confidence score
        if confidence <= 0.6:
            base_priority = ReviewPriority.HIGH
        elif confidence <= 0.75:
            base_priority = ReviewPriority.MEDIUM
        else:
            base_priority = ReviewPriority.LOW
        
        # Adjust based on business value indicators
        has_website = bool(common_crawl_record.get("website_url"))
        has_contact_info = bool(common_crawl_record.get("emails") or common_crawl_record.get("phones"))
        is_large_company = "pty ltd" in abr_record.get("entity_name", "").lower()
        
        # Upgrade priority if high-value company
        if (has_website and has_contact_info and is_large_company and 
            base_priority == ReviewPriority.MEDIUM):
            return ReviewPriority.HIGH
        
        return base_priority
    
    def _estimate_business_value(self,
                                common_crawl_record: Dict[str, Any],
                                abr_record: Dict[str, Any]) -> float:
        """Estimate business value of the potential match."""
        
        value = 0.0
        
        # Base value
        value += 10.0
        
        # Website presence adds value
        if common_crawl_record.get("website_url"):
            value += 25.0
        
        # Contact information adds value
        if common_crawl_record.get("emails"):
            value += 15.0
        if common_crawl_record.get("phones"):
            value += 10.0
        
        # Company size indicator
        entity_name = abr_record.get("entity_name", "").lower()
        if "pty ltd" in entity_name:
            value += 20.0
        elif "limited" in entity_name:
            value += 15.0
        
        # Industry factors (technology companies often more valuable)
        industry = common_crawl_record.get("industry", "").lower()
        if any(tech_word in industry for tech_word in ["technology", "software", "digital", "tech"]):
            value += 30.0
        elif any(prof_word in industry for prof_word in ["professional", "consulting", "services"]):
            value += 20.0
        
        return round(value, 2)
    
    async def _store_review_item(self, review_item: ReviewItem):
        """Store review item in database."""
        # Implementation would store to actual database
        logger.info(f"Storing review item {review_item.id} to database")
        pass
    
    async def _update_review_item(self, review_item: ReviewItem):
        """Update review item in database."""
        # Implementation would update in actual database
        logger.info(f"Updating review item {review_item.id} in database")
        pass


# CLI Interface for reviewers
class ReviewCLI:
    """Command-line interface for manual reviewers."""
    
    def __init__(self, workflow: ManualReviewWorkflow):
        self.workflow = workflow
    
    async def interactive_review_session(self, reviewer_id: str):
        """
        Start an interactive review session.
        
        Args:
            reviewer_id: ID of the reviewer
        """
        print("🔍 Manual Review Session Started")
        print("=" * 50)
        
        while True:
            pending = self.workflow.get_pending_reviews(limit=1)
            if not pending:
                print("✅ No pending reviews remaining!")
                break
            
            review = pending[0]
            self._display_review_item(review)
            
            # Get reviewer decision
            while True:
                decision = input("\nDecision ([a]pprove/[r]eject/[s]kip/[q]uit): ").lower().strip()
                
                if decision == 'q':
                    return
                elif decision == 's':
                    break
                elif decision in ['a', 'r']:
                    notes = input("Notes (optional): ").strip() or None
                    await self.workflow.submit_review_decision(
                        review.id,
                        reviewer_id,
                        decision == 'a',
                        notes
                    )
                    print(f"✅ Review {'approved' if decision == 'a' else 'rejected'}")
                    break
                else:
                    print("Invalid input. Please enter 'a', 'r', 's', or 'q'.")
            
            if decision == 'q':
                break
    
    def _display_review_item(self, review: ReviewItem):
        """Display a review item for assessment."""
        print(f"\n📋 Review Item: {review.id}")
        print(f"🎯 Priority: {review.priority.value.upper()}")
        print(f"📊 Confidence: {review.matching_confidence:.3f}")
        print(f"💰 Est. Value: ${review.estimated_business_value:.2f}")
        print("-" * 50)
        
        print("🌐 Common Crawl Record:")
        cc = review.common_crawl_record
        print(f"   Company: {cc.get('company_name', 'N/A')}")
        print(f"   Website: {cc.get('website_url', 'N/A')}")
        print(f"   Industry: {cc.get('industry', 'N/A')}")
        
        print("\n🏛️  ABR Record:")
        abr = review.abr_record
        print(f"   Entity: {abr.get('entity_name', 'N/A')}")
        print(f"   ABN: {abr.get('abn', 'N/A')}")
        print(f"   Status: {abr.get('status', 'N/A')}")
        
        print(f"\n🤖 LLM Reasoning:")
        print(f"   {review.llm_reasoning}")
        
        print(f"\n🔑 Key Factors: {', '.join(review.key_factors)}")


# Example usage
if __name__ == "__main__":
    import asyncio
    
    async def demo():
        workflow = ManualReviewWorkflow()
        
        # Simulate adding items to review queue
        sample_cc = {
            "company_name": "Sydney Tech Solutions",
            "website_url": "https://sydneytech.com.au",
            "industry": "Technology",
            "emails": ["info@sydneytech.com.au"]
        }
        
        sample_abr = {
            "entity_name": "Sydney Technology Solutions Pty Ltd",
            "abn": "12345678901",
            "status": "Active"
        }
        
        review_id = await workflow.queue_for_review(
            sample_cc,
            sample_abr,
            0.68,
            "Moderate confidence match with name variation",
            ["name_similarity", "domain_alignment", "industry_consistency"]
        )
        
        # Generate summary
        summary = workflow.generate_review_summary()
        print("Review Summary:", json.dumps(summary, indent=2))
        
        # Demo CLI interface
        cli = ReviewCLI(workflow)
        # await cli.interactive_review_session("reviewer_001")
    
    asyncio.run(demo())