"""
Data Collection Agent for Dealership Scanner
Guides scrapers to collect enhanced, accurate vehicle data from dealership websites.
"""
from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field

from pydantic import BaseModel
from llm.client import LLMClient, LLMResponseError

logger = logging.getLogger("agents.data_collection")

SYSTEM_PROMPT = """You are an expert automotive data collection assistant. Your task is to analyze dealership websites and provide specific guidance on what data to extract to maximize accuracy and completeness.

For each dealer_website_url you receive, analyze the HTML structure and provide detailed instructions on:
1. What additional vehicle specification fields to collect beyond the basic ones
2. Which specific elements contain valuable data (engine specs, features, condition info)
3. How to extract information from complex HTML structures or JavaScript-rendered content
4. What fallback strategies to use when primary data sources are missing
5. How to handle different website layouts for the same dealership group

Instructions:
- Analyze the website's HTML structure to identify data sources
- Prioritize collecting complete vehicle specifications (engine, transmission, fuel economy)
- Identify additional information like warranty details, services, special offers
- Recommend best practices for image extraction and gallery collection
- Provide specific selectors or patterns to look for in the HTML
- Consider the dealership's specific offerings or specializations

Return only valid JSON matching this schema:
{
  "dealer_id": string,
  "website_analysis": {
    "primary_data_sources": [string],
    "additional_fields": [string],
    "special_considerations": [string],
    "image_collection_tips": [string],
    "javascript_handling": [string],
    "fallback_strategies": [string]
  },
  "recommended_data_fields": [
    {
      "field_name": string,
      "data_type": "string" | "number" | "boolean" | "array",
      "source": string,
      "priority": "high" | "medium" | "low",
      "notes": string
    }
  ],
  "enhanced_extraction_tips": [
    {
      "tip": string,
      "example": string,
      "website_section": string
    }
  ],
  "data_quality_indicators": [
    {
      "indicator": string,
      "importance": "high" | "medium" | "low",
      "how_to_verify": string
    }
  ]
}"""

PROMPT_VERSION = "data_collection_agent_v1"

class VehicleDataField(BaseModel):
    field_name: str
    data_type: str  # "string" | "number" | "boolean" | "array"
    source: str
    priority: str  # "high" | "medium" | "low"
    notes: str

class DataCollectionRecommendation(BaseModel):
    dealer_id: str
    website_analysis: Dict[str, List[str]]
    recommended_data_fields: List[VehicleDataField]
    enhanced_extraction_tips: List[Dict[str, str]]
    data_quality_indicators: List[Dict[str, str]]

def _user_prompt(dealer_id: str, website_content: str) -> str:
    return f"""Analyze this dealership website and provide data collection guidance.

Dealer ID: {dealer_id}
Website Content: {website_content[:2000] if website_content else 'No content provided'}

Provide detailed guidance on what additional data to collect to increase accuracy and completeness."""

def analyze_dealer_website(
    dealer_id: str,
    website_content: str,
    client: LLMClient,
    *,
    model: str | None = None,
) -> DataCollectionRecommendation:
    """
    Analyze a dealership website and provide data collection guidance.
    """
    try:
        raw = client.complete_json(
            system=SYSTEM_PROMPT,
            user=_user_prompt(dealer_id, website_content),
            model=model,
            temperature=0.3,  # Slightly higher temperature for creativity in recommendations
        )
        
        # Ensure the response has the required structure
        raw.setdefault("dealer_id", dealer_id)
        raw.setdefault("website_analysis", {
            "primary_data_sources": [],
            "additional_fields": [],
            "special_considerations": [],
            "image_collection_tips": [],
            "javascript_handling": [],
            "fallback_strategies": []
        })
        raw.setdefault("recommended_data_fields", [])
        raw.setdefault("enhanced_extraction_tips", [])
        raw.setdefault("data_quality_indicators", [])
        
        # Validate and return the structured data
        return DataCollectionRecommendation(**raw)
        
    except Exception as e:
        logger.warning(f"Data collection agent failed for {dealer_id}: {e}")
        # Return fallback recommendation
        return DataCollectionRecommendation(
            dealer_id=dealer_id,
            website_analysis={
                "primary_data_sources": ["Basic vehicle listing data"],
                "additional_fields": ["Basic specifications (engine, transmission)"],
                "special_considerations": ["Check for vehicle history reports"],
                "image_collection_tips": ["Capture all gallery images"],
                "javascript_handling": ["Handle dynamic content loading"],
                "fallback_strategies": ["Use fallback HTML parsing"]
            },
            recommended_data_fields=[
                VehicleDataField(
                    field_name="engine_displacement",
                    data_type="number",
                    source="website",
                    priority="high",
                    notes="Engine size in liters"
                ),
                VehicleDataField(
                    field_name="fuel_efficiency",
                    data_type="number",
                    source="website",
                    priority="medium",
                    notes="City/Highway MPG ratings"
                ),
                VehicleDataField(
                    field_name="warranty_info",
                    data_type="string",
                    source="website",
                    priority="medium",
                    notes="Remaining warranty details"
                )
            ],
            enhanced_extraction_tips=[
                {
                    "tip": "Look for technical specifications sections",
                    "example": "Search for 'Vehicle Specifications' or 'Features' sections",
                    "website_section": "Vehicle detail page"
                }
            ],
            data_quality_indicators=[
                {
                    "indicator": "Complete specification set",
                    "importance": "high",
                    "how_to_verify": "Compare against manufacturer data"
                }
            ]
        )

class DataCollectionAgent:
    """Agent that guides dealership scrapers to collect enhanced data."""

    def __init__(self, client: LLMClient) -> None:
        self.client = client

    def run(self, dealer_id: str, website_content: str, *, model: str | None = None) -> DataCollectionRecommendation:
        """
        Run data collection analysis for a specific dealership.
        """
        return analyze_dealer_website(dealer_id, website_content, self.client, model=model)

    def get_enhanced_data_fields(self, dealer_id: str, website_content: str) -> List[VehicleDataField]:
        """
        Get recommended enhanced data fields for a dealership.
        """
        recommendation = self.run(dealer_id, website_content)
        return recommendation.recommended_data_fields

    def get_extraction_tips(self, dealer_id: str, website_content: str) -> List[Dict[str, str]]:
        """
        Get specific extraction tips for a dealership.
        """
        recommendation = self.run(dealer_id, website_content)
        return recommendation.enhanced_extraction_tips

    def get_quality_indicators(self, dealer_id: str, website_content: str) -> List[Dict[str, str]]:
        """
        Get data quality indicators for a dealership.
        """
        recommendation = self.run(dealer_id, website_content)
        return recommendation.data_quality_indicators

# Utility function to process dealer website and generate recommendations
def process_dealer_scraping_guidance(
    dealer_id: str,
    website_url: str,
    content: str,
    agent_client: LLMClient,
    model_name: str = "llama3.2:1b"
) -> Dict[str, Any]:
    """
    Process a dealer's website and return enhanced scraping guidance.
    This can be called by scrapers to get specific recommendations.
    """
    agent = DataCollectionAgent(agent_client)
    recommendation = agent.run(dealer_id, content, model=model_name)
    
    return {
        "dealer_id": recommendation.dealer_id,
        "website_analysis": recommendation.website_analysis,
        "recommended_data_fields": [
            field.model_dump() for field in recommendation.recommended_data_fields
        ],
        "enhanced_extraction_tips": recommendation.enhanced_extraction_tips,
        "data_quality_indicators": recommendation.data_quality_indicators
    }