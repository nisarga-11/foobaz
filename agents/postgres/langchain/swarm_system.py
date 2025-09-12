#!/usr/bin/env python3
"""
Database Swarm System for coordinating multiple database agents.
"""

from typing import Dict, List, Any, Optional
from agents.db1_agent import CustomerDBAgent
from agents.db2_agent import EmployeeDBAgent
from config.settings import settings

class DatabaseSwarmSystem:
    """Swarm system for coordinating database operations across multiple agents."""
    
    def __init__(self):
        """Initialize the swarm system with database agents."""
        self.customer_agent = CustomerDBAgent()
        self.employee_agent = EmployeeDBAgent()
        self.conversation_history: List[str] = []
    
    def get_agent_status(self) -> Dict[str, Any]:
        """Get the current status of all agents."""
        return {
            "customer_agent": {
                "name": self.customer_agent.agent_name,
                "database": self.customer_agent.database_name,
                "current_task": self.customer_agent.current_task
            },
            "employee_agent": {
                "name": self.employee_agent.agent_name,
                "database": self.employee_agent.database_name,
                "current_task": self.employee_agent.current_task
            }
        }
    
    def process_user_input(self, user_input: str) -> Dict[str, Any]:
        """Process user input through the swarm system."""
        self.conversation_history = []
        
        # Determine which agent should handle this request
        user_input_lower = user_input.lower()
        
        # Check if it's a "both databases" request
        both_databases = any(word in user_input_lower for word in ["both", "all", "each", "every"])
        
        if both_databases:
            # Handle both databases request
            return self._handle_both_databases_request(user_input)
        else:
            # Determine primary agent based on database mentioned
            if "customer" in user_input_lower or "customer_db" in user_input_lower:
                primary_agent = self.customer_agent
                secondary_agent = self.employee_agent
            elif "employee" in user_input_lower or "employee_db" in user_input_lower:
                primary_agent = self.employee_agent
                secondary_agent = self.customer_agent
            else:
                # Default to customer agent
                primary_agent = self.customer_agent
                secondary_agent = self.employee_agent
            
            return self._handle_single_agent_request(user_input, primary_agent, secondary_agent)
    
    def _handle_both_databases_request(self, user_input: str) -> Dict[str, Any]:
        """Handle requests that involve both databases."""
        # Start with customer agent
        customer_result = self.customer_agent.process_user_input(user_input)
        
        # Add customer agent response to conversation
        if customer_result.get("response"):
            self.conversation_history.append(customer_result["response"])
        
        # Check if there's a handoff to employee agent
        if customer_result.get("handoff") == "EmployeeDB_Agent":
            # Process handoff with employee agent
            handoff_message = customer_result.get("handoff_message", "")
            employee_result = self.employee_agent.receive_handoff(handoff_message, "CustomerDB_Agent")
            
            if employee_result.get("response"):
                self.conversation_history.append(employee_result["response"])
        
        return {
            "response": self.conversation_history[-1] if self.conversation_history else "",
            "conversation_history": self.conversation_history
        }
    
    def _handle_single_agent_request(self, user_input: str, primary_agent, secondary_agent) -> Dict[str, Any]:
        """Handle requests that primarily involve one agent."""
        # Process with primary agent
        primary_result = primary_agent.process_user_input(user_input)
        
        # Add primary agent response to conversation
        if primary_result.get("response"):
            self.conversation_history.append(primary_result["response"])
        
        # Check if there's a handoff to secondary agent
        if primary_result.get("handoff"):
            handoff_message = primary_result.get("handoff_message", "")
            secondary_result = secondary_agent.receive_handoff(handoff_message, primary_agent.agent_name)
            
            if secondary_result.get("response"):
                self.conversation_history.append(secondary_result["response"])
        
        return {
            "response": self.conversation_history[-1] if self.conversation_history else "",
            "conversation_history": self.conversation_history
        }
