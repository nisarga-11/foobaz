#!/usr/bin/env python3
"""
Intelligent Restore Recommendation System

Uses Ollama LLM to analyze backup data and recommend optimal restore points
for coordinated multi-database restoration.
"""

import asyncio
import json
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import httpx
from pydantic import BaseModel

logger = logging.getLogger(__name__)

class BackupRecommendation(BaseModel):
    """A recommended backup for restoration."""
    database: str
    server: str
    backup_id: str
    backup_type: str
    timestamp: str
    confidence_score: float
    reason: str

class RestoreRecommendationSet(BaseModel):
    """A set of coordinated restore recommendations."""
    recommendation_id: str
    target_timestamp: str
    description: str
    total_databases: int
    total_confidence: float
    recommendations: List[BackupRecommendation]

class IntelligentRestoreRecommender:
    """AI-powered restore recommendation system using Ollama."""
    
    def __init__(self, ollama_base_url: str = "http://localhost:11434", model: str = "llama3.1"):
        self.ollama_base_url = ollama_base_url
        self.model = model
        self.system_prompt = self._get_system_prompt()
        
    def _get_system_prompt(self) -> str:
        """Get the system prompt for restore recommendations."""
        return """You are an intelligent PostgreSQL backup analysis expert specializing in coordinated database restoration.

Your task is to analyze backup data across multiple databases and servers to recommend optimal restore points that achieve consistency across all databases.

ANALYSIS CRITERIA:
1. **Temporal Proximity**: Find backups with timestamps as close as possible to each other
2. **Backup Quality**: Prefer full backups over incremental when available
3. **Data Consistency**: Ensure all databases can be restored to a coherent point in time
4. **Completeness**: Include all requested databases in recommendations

RESPONSE FORMAT:
You must return a JSON object with this exact structure:

{
  "recommendations": [
    {
      "recommendation_id": "rec_001",
      "target_timestamp": "2025-09-17T18:45:00Z",
      "description": "Coordinated restore to consistent state around 6:45 PM",
      "total_databases": 6,
      "total_confidence": 0.92,
      "recommendations": [
        {
          "database": "customer_db",
          "server": "PG1", 
          "backup_id": "customer_db_base_20250917_184500",
          "backup_type": "full",
          "timestamp": "2025-09-17T18:45:00Z",
          "confidence_score": 0.95,
          "reason": "Full backup closest to target time"
        }
      ]
    }
  ]
}

SCORING RULES:
- Full backups: Base confidence 0.9
- Incremental backups: Base confidence 0.7  
- Timestamp proximity bonus: +0.1 for each hour closer
- Consistency bonus: +0.05 for each database in same time window
- Completeness penalty: -0.2 for each missing database

IMPORTANT:
- Always return exactly 3 recommendation sets
- Each set should target a different time period
- Include ALL available databases in each recommendation
- Provide clear reasoning for each choice
- Ensure JSON is valid and parseable"""

    async def analyze_and_recommend(
        self, 
        all_backup_data: Dict[str, Dict[str, List[Dict]]], 
        num_recommendations: int = 3,
        target_timestamp: Optional[str] = None
    ) -> List[RestoreRecommendationSet]:
        """
        Analyze backup data and generate intelligent restore recommendations.
        
        Args:
            all_backup_data: Nested dict of {server: {database: [backups]}}
            num_recommendations: Number of recommendation sets to generate
            target_timestamp: Optional target timestamp for recommendations
            
        Returns:
            List of restore recommendation sets
        """
        try:
            logger.info(f"🧠 Analyzing backup data for {num_recommendations} recommendations")
            
            # Prepare data for LLM analysis
            analysis_data = self._prepare_analysis_data(all_backup_data, target_timestamp)
            
            # Generate recommendations using Ollama
            recommendations = await self._generate_recommendations(analysis_data, num_recommendations)
            
            # Validate and rank recommendations
            validated_recommendations = self._validate_recommendations(recommendations, all_backup_data)
            
            logger.info(f"✅ Generated {len(validated_recommendations)} validated recommendations")
            return validated_recommendations
            
        except Exception as e:
            logger.error(f"❌ Failed to generate recommendations: {e}")
            # Return fallback recommendations
            return self._generate_fallback_recommendations(all_backup_data, num_recommendations)
    
    def _prepare_analysis_data(
        self, 
        all_backup_data: Dict[str, Dict[str, List[Dict]]], 
        target_timestamp: Optional[str]
    ) -> Dict[str, Any]:
        """Prepare backup data for LLM analysis."""
        
        # Flatten and summarize backup data
        backup_summary = []
        all_databases = set()
        
        for server, databases in all_backup_data.items():
            for database, backups in databases.items():
                all_databases.add(database)
                
                # Get latest 10 backups for each database
                recent_backups = sorted(backups, key=lambda x: x.get('timestamp', ''), reverse=True)[:10]
                
                for backup in recent_backups:
                    backup_summary.append({
                        "server": server,
                        "database": database,
                        "backup_id": backup.get('backup_id'),
                        "backup_type": backup.get('type', backup.get('backup_type', 'unknown')),
                        "timestamp": backup.get('timestamp', backup.get('completed_at_iso', '')),
                        "path": backup.get('path', ''),
                        "size": backup.get('size_mb', backup.get('size_bytes', 0))
                    })
        
        analysis_data = {
            "total_servers": len(all_backup_data),
            "total_databases": len(all_databases),
            "database_list": list(all_databases),
            "backup_summary": backup_summary,
            "target_timestamp": target_timestamp,
            "analysis_timestamp": datetime.now().isoformat(),
            "request": {
                "num_recommendations": 3,
                "goal": "Find coordinated restore points for all databases"
            }
        }
        
        return analysis_data
    
    async def _generate_recommendations(
        self, 
        analysis_data: Dict[str, Any], 
        num_recommendations: int
    ) -> List[RestoreRecommendationSet]:
        """Generate recommendations using Ollama LLM."""
        
        prompt = f"""Analyze the following backup data and generate {num_recommendations} coordinated restore recommendations:

BACKUP DATA:
{json.dumps(analysis_data, indent=2)}

REQUIREMENTS:
1. Generate exactly {num_recommendations} different recommendation sets
2. Each set should target a different time period for variety
3. Include ALL {analysis_data['total_databases']} databases in each recommendation
4. Prioritize backups that are temporally close to each other
5. Prefer full backups when available
6. Provide confidence scores and reasoning

Return the recommendations in the specified JSON format."""

        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                response = await client.post(
                    f"{self.ollama_base_url}/api/chat",
                    json={
                        "model": self.model,
                        "messages": [
                            {"role": "system", "content": self.system_prompt},
                            {"role": "user", "content": prompt}
                        ],
                        "stream": False,
                        "options": {
                            "temperature": 0.3,
                            "num_ctx": 16384
                        }
                    }
                )
                
                if response.status_code == 200:
                    result = response.json()
                    content = result.get("message", {}).get("content", "")
                    
                    # Extract JSON from response
                    recommendations_data = self._extract_json_from_response(content)
                    
                    # Parse into recommendation objects
                    recommendations = []
                    for rec_data in recommendations_data.get("recommendations", []):
                        try:
                            recommendation = RestoreRecommendationSet(**rec_data)
                            recommendations.append(recommendation)
                        except Exception as e:
                            logger.warning(f"⚠️ Failed to parse recommendation: {e}")
                    
                    return recommendations
                else:
                    logger.error(f"❌ Ollama request failed: {response.status_code}")
                    return []
                    
        except Exception as e:
            logger.error(f"❌ Ollama communication failed: {e}")
            return []
    
    def _extract_json_from_response(self, content: str) -> Dict[str, Any]:
        """Extract JSON from LLM response content."""
        try:
            # Try to find JSON block
            import re
            json_match = re.search(r'```json\s*(.*?)\s*```', content, re.DOTALL)
            if json_match:
                json_str = json_match.group(1)
            else:
                # Try to find JSON object directly
                json_match = re.search(r'\{.*\}', content, re.DOTALL)
                if json_match:
                    json_str = json_match.group(0)
                else:
                    # Fallback: assume entire content is JSON
                    json_str = content
            
            return json.loads(json_str)
            
        except Exception as e:
            logger.error(f"❌ Failed to extract JSON from response: {e}")
            return {"recommendations": []}
    
    def _validate_recommendations(
        self, 
        recommendations: List[RestoreRecommendationSet],
        all_backup_data: Dict[str, Dict[str, List[Dict]]]
    ) -> List[RestoreRecommendationSet]:
        """Validate recommendations against actual backup data."""
        
        validated = []
        
        for rec in recommendations:
            # Check if all backup IDs exist
            valid_recommendations = []
            
            for backup_rec in rec.recommendations:
                # Verify backup exists
                server_data = all_backup_data.get(backup_rec.server, {})
                db_data = server_data.get(backup_rec.database, [])
                
                backup_exists = any(
                    b.get('backup_id') == backup_rec.backup_id 
                    for b in db_data
                )
                
                if backup_exists:
                    valid_recommendations.append(backup_rec)
                else:
                    logger.warning(f"⚠️ Backup {backup_rec.backup_id} not found, skipping")
            
            # Only include recommendation if it has valid backups
            if valid_recommendations:
                rec.recommendations = valid_recommendations
                rec.total_databases = len(valid_recommendations)
                validated.append(rec)
        
        return validated
    
    def _generate_fallback_recommendations(
        self, 
        all_backup_data: Dict[str, Dict[str, List[Dict]]],
        num_recommendations: int
    ) -> List[RestoreRecommendationSet]:
        """Generate basic fallback recommendations when AI analysis fails."""
        
        logger.info("🔄 Generating fallback recommendations")
        
        recommendations = []
        
        # Get all backups and group by time periods
        all_backups = []
        for server, databases in all_backup_data.items():
            for database, backups in databases.items():
                for backup in backups:
                    all_backups.append({
                        "server": server,
                        "database": database,
                        "backup": backup
                    })
        
        # Sort by timestamp
        all_backups.sort(key=lambda x: x['backup'].get('timestamp', ''), reverse=True)
        
        # Create recommendations for different time periods
        for i in range(min(num_recommendations, 3)):
            rec_id = f"fallback_{i+1:03d}"
            
            # Find backups for this time period
            period_backups = []
            target_time = None
            
            if i == 0:  # Latest backups
                target_time = "Latest available backups"
                period_backups = self._get_latest_backups_per_db(all_backup_data)
            elif i == 1:  # 1 hour ago
                target_time = "Backups from ~1 hour ago"
                period_backups = self._get_backups_around_time(all_backup_data, hours_ago=1)
            else:  # 6 hours ago
                target_time = "Backups from ~6 hours ago"
                period_backups = self._get_backups_around_time(all_backup_data, hours_ago=6)
            
            if period_backups:
                backup_recommendations = [
                    BackupRecommendation(
                        database=b['database'],
                        server=b['server'],
                        backup_id=b['backup']['backup_id'],
                        backup_type=b['backup'].get('type', 'unknown'),
                        timestamp=b['backup'].get('timestamp', ''),
                        confidence_score=0.7,
                        reason="Fallback recommendation"
                    )
                    for b in period_backups
                ]
                
                recommendation = RestoreRecommendationSet(
                    recommendation_id=rec_id,
                    target_timestamp=target_time,
                    description=f"Fallback recommendation: {target_time}",
                    total_databases=len(backup_recommendations),
                    total_confidence=0.7,
                    recommendations=backup_recommendations
                )
                
                recommendations.append(recommendation)
        
        return recommendations
    
    def _get_latest_backups_per_db(self, all_backup_data: Dict) -> List[Dict]:
        """Get the latest backup for each database."""
        latest_backups = []
        
        for server, databases in all_backup_data.items():
            for database, backups in databases.items():
                if backups:
                    latest = max(backups, key=lambda x: x.get('timestamp', ''))
                    latest_backups.append({
                        "server": server,
                        "database": database,
                        "backup": latest
                    })
        
        return latest_backups
    
    def _get_backups_around_time(self, all_backup_data: Dict, hours_ago: int) -> List[Dict]:
        """Get backups from around a specific time ago."""
        target_time = datetime.now() - timedelta(hours=hours_ago)
        target_str = target_time.strftime("%Y%m%d_%H%M%S")
        
        backups_around_time = []
        
        for server, databases in all_backup_data.items():
            for database, backups in databases.items():
                # Find backup closest to target time
                closest_backup = None
                min_diff = float('inf')
                
                for backup in backups:
                    backup_time_str = backup.get('timestamp', '')
                    if backup_time_str:
                        try:
                            # Calculate time difference
                            diff = abs(len(backup_time_str) - len(target_str))  # Simple approximation
                            if diff < min_diff:
                                min_diff = diff
                                closest_backup = backup
                        except:
                            continue
                
                if closest_backup:
                    backups_around_time.append({
                        "server": server,
                        "database": database,
                        "backup": closest_backup
                    })
        
        return backups_around_time

# Example usage and testing
async def test_recommender():
    """Test the restore recommender with sample data."""
    
    # Sample backup data structure
    sample_data = {
        "PG1": {
            "customer_db": [
                {
                    "backup_id": "customer_db_base_20250917_180000",
                    "type": "full",
                    "timestamp": "20250917_180000"
                },
                {
                    "backup_id": "customer_db_wal_20250917_180500",
                    "type": "incremental", 
                    "timestamp": "20250917_180500"
                }
            ],
            "inventory_db": [
                {
                    "backup_id": "inventory_db_base_20250917_180100",
                    "type": "full",
                    "timestamp": "20250917_180100"
                }
            ]
        },
        "PG2": {
            "hr_db": [
                {
                    "backup_id": "hr_db_base_20250917_180000",
                    "type": "full",
                    "timestamp": "20250917_180000"
                }
            ]
        }
    }
    
    recommender = IntelligentRestoreRecommender()
    recommendations = await recommender.analyze_and_recommend(sample_data)
    
    print("Generated Recommendations:")
    for i, rec in enumerate(recommendations, 1):
        print(f"\n{i}. {rec.description}")
        print(f"   Confidence: {rec.total_confidence:.2f}")
        print(f"   Databases: {rec.total_databases}")
        for backup_rec in rec.recommendations:
            print(f"   - {backup_rec.database} ({backup_rec.server}): {backup_rec.backup_id}")

if __name__ == "__main__":
    asyncio.run(test_recommender())
