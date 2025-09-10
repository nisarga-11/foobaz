import json
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
import requests
from config.settings import settings

logger = logging.getLogger(__name__)

class BackupRecommendationEngine:
    """Engine for recommending optimal backups using LLM analysis."""
    
    def __init__(self):
        self.ollama_base_url = settings.OLLAMA_BASE_URL
        self.ollama_model = settings.OLLAMA_MODEL
    
    def recommend_backup_restore(self, database_name: str, available_backups: List[Dict[str, Any]], 
                                user_context: str = "") -> Dict[str, Any]:
        """Recommend the best backup for restore using LLM analysis."""
        try:
            # Prepare context for LLM
            context = self._prepare_backup_context(database_name, available_backups, user_context)
            
            # Generate recommendation prompt
            prompt = self._generate_recommendation_prompt(context)
            
            # Get LLM recommendation
            recommendation = self._get_llm_recommendation(prompt)
            
            return {
                "status": "success",
                "recommendation": recommendation,
                "available_backups": available_backups,
                "context": context
            }
            
        except Exception as e:
            logger.error(f"Error generating backup recommendation: {e}")
            return {
                "status": "error",
                "message": str(e),
                "recommendation": None
            }
    
    def recommend_coordinated_backup_restore(self, databases: List[str], 
                                           backup_data: Dict[str, List[Dict[str, Any]]], 
                                           user_context: str = "") -> Dict[str, Any]:
        """Recommend coordinated backup restore for multiple databases."""
        try:
            # Prepare coordinated context
            context = self._prepare_coordinated_context(databases, backup_data, user_context)
            
            # Generate coordinated recommendation prompt
            prompt = self._generate_coordinated_prompt(context)
            
            # Get LLM recommendation
            recommendation = self._get_llm_recommendation(prompt)
            
            return {
                "status": "success",
                "recommendation": recommendation,
                "databases": databases,
                "backup_data": backup_data
            }
            
        except Exception as e:
            logger.error(f"Error generating coordinated backup recommendation: {e}")
            return self._create_fallback_coordinated_recommendation(backup_data, {"user_context": user_context})
    
    def _prepare_backup_context(self, database_name: str, available_backups: List[Dict[str, Any]], 
                               user_context: str) -> Dict[str, Any]:
        """Prepare context for backup recommendation."""
        # Sort backups by timestamp (newest first)
        sorted_backups = sorted(available_backups, 
                              key=lambda x: x.get("timestamp", ""), reverse=True)
        
        # Categorize backups
        full_backups = [b for b in sorted_backups if b.get("backup_type") == "full"]
        incremental_backups = [b for b in sorted_backups if b.get("backup_type") == "incremental"]
        
        # Find latest backup of each type
        latest_full = full_backups[0] if full_backups else None
        latest_incremental = incremental_backups[0] if incremental_backups else None
        
        # Calculate time differences
        now = datetime.now()
        time_analysis = {}
        
        for backup in sorted_backups[:5]:  # Analyze top 5 most recent
            if backup.get("timestamp"):
                try:
                    # Parse timestamp (assuming ISO format)
                    backup_time = datetime.fromisoformat(backup["timestamp"].replace('Z', '+00:00'))
                    time_diff = now - backup_time.replace(tzinfo=None)
                    time_analysis[backup["backup_id"]] = {
                        "hours_ago": time_diff.total_seconds() / 3600,
                        "days_ago": time_diff.days,
                        "is_recent": time_diff.total_seconds() < 24 * 3600  # Within 24 hours
                    }
                except Exception as e:
                    logger.warning(f"Could not parse timestamp for backup {backup.get('backup_id')}: {e}")
        
        return {
            "database_name": database_name,
            "total_backups": len(available_backups),
            "full_backups": len(full_backups),
            "incremental_backups": len(incremental_backups),
            "latest_full": latest_full,
            "latest_incremental": latest_incremental,
            "recent_backups": sorted_backups[:5],
            "time_analysis": time_analysis,
            "user_context": user_context,
            "current_time": now.isoformat()
        }
    
    def _prepare_coordinated_context(self, databases: List[str], 
                                   backup_data: Dict[str, List[Dict[str, Any]]], 
                                   user_context: str) -> Dict[str, Any]:
        """Prepare context for coordinated backup recommendation."""
        coordinated_context = {
            "databases": databases,
            "user_context": user_context,
            "current_time": datetime.now().isoformat(),
            "database_analysis": {}
        }
        
        for db_name in databases:
            if db_name in backup_data:
                backups = backup_data[db_name]
                context = self._prepare_backup_context(db_name, backups, user_context)
                coordinated_context["database_analysis"][db_name] = context
        
        return coordinated_context
    
    def _generate_recommendation_prompt(self, context: Dict[str, Any]) -> str:
        """Generate prompt for backup recommendation."""
        prompt = f"""
You are a database backup expert. Analyze the following backup information and recommend the best backup for restore.

Database: {context['database_name']}
Total Backups Available: {context['total_backups']}
Full Backups: {context['full_backups']}
Incremental Backups: {context['incremental_backups']}
Current Time: {context['current_time']}

Recent Backups (top 5):
"""
        
        for i, backup in enumerate(context['recent_backups'], 1):
            backup_id = backup.get('backup_id', 'Unknown')
            backup_type = backup.get('backup_type', 'Unknown')
            timestamp = backup.get('timestamp', 'Unknown')
            size = backup.get('size', 'Unknown')
            
            time_info = context['time_analysis'].get(backup_id, {})
            hours_ago = time_info.get('hours_ago', 'Unknown')
            
            prompt += f"""
{i}. Backup ID: {backup_id}
   Type: {backup_type}
   Timestamp: {timestamp}
   Size: {size}
   Age: {hours_ago:.1f} hours ago
"""
        
        prompt += f"""
User Context: {context['user_context']}

Please provide a recommendation in the following JSON format:
{{
    "recommended_backup": "backup_id",
    "reasoning": "Detailed explanation of why this backup was chosen",
    "confidence": "high/medium/low",
    "alternative_options": ["backup_id1", "backup_id2"],
    "restore_command": "pgbackrest --stanza=demo --delta --db-path=/var/lib/postgresql/15/main restore --set=backup_id",
    "warnings": ["Any warnings or considerations"]
}}

Considerations:
- For corruption recovery, prefer the most recent backup before the corruption
- For point-in-time recovery, consider the timestamp closest to the desired restore point
- Full backups are more reliable but may be older
- Incremental backups are more recent but depend on the base full backup
- Consider the size and completeness of the backup
"""
        
        return prompt
    
    def _generate_coordinated_prompt(self, context: Dict[str, Any]) -> str:
        """Generate prompt for coordinated backup recommendation."""
        prompt = f"""
You are a database backup expert. Analyze the following coordinated backup information for multiple databases and recommend the best restore strategy.

Databases: {', '.join(context['databases'])}
Current Time: {context['current_time']}
User Context: {context['user_context']}

Database Analysis:
"""
        
        for db_name, db_context in context['database_analysis'].items():
            prompt += f"""
Database: {db_name}
- Total Backups: {db_context['total_backups']}
- Latest Full: {db_context['latest_full']['backup_id'] if db_context['latest_full'] else 'None'}
- Latest Incremental: {db_context['latest_incremental']['backup_id'] if db_context['latest_incremental'] else 'None'}
"""
        
        prompt += """
Please provide a coordinated recommendation in the following JSON format:
{
    "recommended_strategy": "sequential/parallel/coordinated",
    "reasoning": "Detailed explanation of the strategy",
    "confidence": "high/medium/low",
    "recommended_backups": {
        "database1": "backup_id1",
        "database2": "backup_id2"
    },
    "restore_commands": [
        "pgbackrest --stanza=demo --delta --db-path=/var/lib/postgresql/15/main restore --set=backup_id1",
        "pgbackrest --stanza=demo --delta --db-path=/var/lib/postgresql/15/main restore --set=backup_id2"
    ],
    "warnings": ["Any warnings or considerations"],
    "estimated_downtime": "estimated time for restore operations"
}

Considerations:
- Ensure backups are from similar timeframes to maintain data consistency
- Consider dependencies between databases
- Plan for minimal downtime
- Ensure all required backups are available
"""
        
        return prompt
    
    def _get_llm_recommendation(self, prompt: str) -> Dict[str, Any]:
        """Get recommendation from Ollama LLM."""
        try:
            # Prepare request payload
            payload = {
                "model": self.ollama_model,
                "prompt": prompt,
                "stream": False,
                "options": {
                    "temperature": 0.1,  # Low temperature for consistent recommendations
                    "top_p": 0.9
                }
            }
            
            # Make request to Ollama
            response = requests.post(
                f"{self.ollama_base_url}/api/generate",
                json=payload,
                timeout=300  # 5 minute timeout for restore operations
            )
            
            if response.status_code == 200:
                result = response.json()
                llm_response = result.get("response", "")
                
                # Try to parse JSON from response
                try:
                    # Extract JSON from response (handle cases where LLM adds extra text)
                    json_start = llm_response.find('{')
                    json_end = llm_response.rfind('}') + 1
                    
                    if json_start != -1 and json_end > json_start:
                        json_str = llm_response[json_start:json_end]
                        recommendation = json.loads(json_str)
                        return recommendation
                    else:
                        # Fallback: create basic recommendation
                        return self._create_fallback_recommendation(llm_response)
                        
                except json.JSONDecodeError as e:
                    logger.warning(f"Could not parse JSON from LLM response: {e}")
                    return self._create_fallback_recommendation(llm_response)
            else:
                logger.error(f"Ollama request failed with status {response.status_code}")
                return self._create_fallback_recommendation("LLM service unavailable")
                
        except Exception as e:
            logger.error(f"Error calling Ollama: {e}")
            return self._create_fallback_coordinated_recommendation(all_backup_data, parameters)
    
    def _create_fallback_recommendation(self, llm_response: str) -> Dict[str, Any]:
        """Create a fallback recommendation when LLM parsing fails."""
        return {
            "recommended_backup": "latest",
            "reasoning": f"Fallback recommendation due to parsing issues. LLM response: {llm_response[:200]}...",
            "confidence": "low",
            "alternative_options": [],
            "restore_command": "pgbackrest --stanza=demo --delta --db-path=/var/lib/postgresql/15/main restore",
            "warnings": ["This is a fallback recommendation. Please verify backup selection manually."]
        }
    
    def _create_fallback_coordinated_recommendation(self, all_backup_data: Dict[str, List[Dict]], parameters: Dict[str, Any]) -> Dict[str, Any]:
        """Create a fallback coordinated recommendation when LLM fails."""
        databases = list(all_backup_data.keys())
        if len(databases) != 2:
            return {
                "status": "error",
                "message": "Coordinated backup restore currently supports exactly 2 databases",
                "recommendation": None
            }
        
        db1, db2 = databases
        recommended_backups = {}
        
        # Use the most recent backup for each database
        for db_name in databases:
            backups = all_backup_data[db_name]
            if backups:
                # Sort by timestamp and take the most recent
                sorted_backups = sorted(backups, key=lambda x: x.get("timestamp", ""), reverse=True)
                recommended_backups[db_name] = sorted_backups[0]["backup_id"]
        
        return {
            "status": "success",
            "recommendation": {
                "recommended_strategy": "most_recent",
                "recommended_backups": recommended_backups,
                "reasoning": "Using most recent backups for each database as fallback recommendation",
                "confidence": "medium",
                "restore_commands": [
                    f"pgbackrest --stanza=demo --delta --db-path=/var/lib/postgresql/15/main restore --set={backup_id}"
                    for backup_id in recommended_backups.values()
                ],
                "warnings": ["This is a fallback recommendation. Please verify backup selection manually."]
            },
            "available_backups": all_backup_data
        }
