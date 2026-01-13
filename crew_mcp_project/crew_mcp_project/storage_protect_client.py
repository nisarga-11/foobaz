#!/usr/bin/env python3
"""
IBM Storage Protect (DataMover) Client - FOOBAZ Configuration
Handles sign-on and backup operations to IBM Storage Protect.
"""

import asyncio
import logging
import os
from typing import Any, Dict, Optional, List
import httpx

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class StorageProtectClient:
    """IBM Storage Protect API client for backup operations."""
    
    def __init__(self,
                 base_url: str = "https://9.11.52.248:9081/DpVe/api",
                 node_id: str = "FOOBAZ",
                 node_admin_id: str = "spectrumprotect",
                 password: str = "admin_12345_admin",
                 http_port: str = "1581",
                 sp_backup_directory: str = "/sp_backups",
                 auto_cleanup_tasks: bool = False):
        """
        Initialize Storage Protect Client for FOOBAZ node.
        
        Args:
            base_url: Storage Protect server API URL
            node_id: Node name (FOOBAZ)
            node_admin_id: Node administrator ID
            password: Node password
            http_port: HTTP port for CAD service (1581)
            sp_backup_directory: Base directory path in Storage Protect
            auto_cleanup_tasks: Whether to automatically cleanup completed tasks
        """
        self.base_url = base_url
        self.node_id = node_id
        self.node_admin_id = node_admin_id
        self.password = password
        self.http_port = http_port
        self.sp_backup_directory = sp_backup_directory
        self.auto_cleanup_tasks = auto_cleanup_tasks
        self.context_id = None
        self.server_name = None
        self.node_name = None
        self.tcp_server_address = None
        self.tcp_port = None
        self.session = httpx.AsyncClient(verify=False, timeout=120.0)  # Increased timeout
        self.max_retries = 3
        self.retry_delay = 5

    def _common_headers(self) -> Dict[str, str]:
        """Return common browser-style headers for all API requests."""
        return {
            "Accept": "application/json, text/plain, */*",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8",
            "Connection": "keep-alive",
            "Content-Type": "application/json",
            "Host": "9.11.52.248:9081",
            "Origin": "https://9.11.52.248:9081",
            "Referer": "https://9.11.52.248:9081/bagui/",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36",
            "sec-ch-ua": '"Google Chrome";v="141", "Not?A_Brand";v="8", "Chromium";v="141"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Linux"'
        }

    async def sign_on(self) -> Dict[str, Any]:
        """
        Sign on to Storage Protect and get context ID.
        
        Returns:
            Dictionary with sign-on response including contextId
            
        Raises:
            Exception: If sign-on fails after max retries
        """
        for attempt in range(self.max_retries):
            try:
                url = f"{self.base_url}/ba/signon?api-version=1.1.0"
                headers = self._common_headers()

                # Authentication payload matching FOOBAZ configuration
                auth_payload = {
                    "nodeId": self.node_id,
                    "nodeAdminId": self.node_admin_id,
                    "password": self.password,
                    "httpPort": self.http_port
                }

                logger.info(f"Attempting sign-on to {self.base_url} (attempt {attempt + 1}/{self.max_retries})")
                response = await self.session.post(url, headers=headers, json=auth_payload)
                response.raise_for_status()

                result = response.json()

                # Check for API errors in response
                if result.get("statusRC") != 0:
                    error_msg = result.get("statusMessage", "Unknown error")
                    error_code = result.get("statusRC")
                    
                    # Log detailed error information
                    logger.error(f"Storage Protect API Error:")
                    logger.error(f"  Status Code: {error_code}")
                    logger.error(f"  Message: {error_msg}")
                    
                    # Check for specific CAD service error
                    if "ANS374E" in error_msg or "CAD service" in error_msg:
                        logger.error("CAD Service Issue Detected:")
                        logger.error("  1. Verify CAD is running: ps -ef | grep dsmcad")
                        logger.error("  2. Check dsm.sys has: MANAGEDServices SCHEDULE WEBCLIENT")
                        logger.error("  3. Check port 1581 is listening: ss -tulpn | grep 1581")
                        logger.error("  4. Restart CAD: killall dsmcad && /usr/bin/dsmcad")
                    
                    raise Exception(f"Storage Protect API error (RC={error_code}): {error_msg}")

                # Extract session information
                self.context_id = result.get("contextId")
                self.server_name = result.get("serverName")
                self.node_name = result.get("nodeName")
                self.tcp_server_address = result.get("tcpServerAddress")
                self.tcp_port = result.get("tcpPort")

                logger.info(f"✓ Storage Protect sign-on successful!")
                logger.info(f"  Server: {self.server_name}")
                logger.info(f"  Node: {self.node_name}")
                logger.info(f"  Context ID: {self.context_id}")
                logger.info(f"  TCP Server: {self.tcp_server_address}:{self.tcp_port}")
                
                return result

            except httpx.HTTPStatusError as e:
                logger.warning(f"HTTP error on attempt {attempt + 1}: {e.response.status_code} - {e.response.text}")
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(self.retry_delay)
                else:
                    logger.error(f"Storage Protect sign-on failed after {self.max_retries} attempts")
                    raise
                    
            except Exception as e:
                logger.warning(f"Sign-on attempt {attempt + 1} failed: {e}")
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(self.retry_delay)
                else:
                    logger.error(f"Storage Protect sign-on failed after {self.max_retries} attempts")
                    raise

    async def start_backup(self, 
                          backup_path: str, 
                          backup_name: str,
                          backup_type: str = "selective", 
                          file_list: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Start a selective backup job.
        
        Args:
            backup_path: Path to backup (directory or file)
            backup_name: Name for the backup
            backup_type: Type of backup (default: "selective")
            file_list: Optional list of file paths to explicitly include
            
        Returns:
            Dictionary with backup job response including taskId
            
        Raises:
            Exception: If not signed in or backup start fails
        """
        if not self.context_id:
            raise Exception("Not signed in to Storage Protect. Call sign_on() first.")

        for attempt in range(self.max_retries):
            try:
                url = f"{self.base_url}/ba/backup?api-version=1.0.0"
                headers = self._common_headers()
                headers["X-Flr-Contextid"] = self.context_id

                sp_backup_path = f"{self.sp_backup_directory}/{backup_name}"

                # Build object list for backup
                if file_list:
                    # Explicit file list provided - backup individual files
                    object_list = []
                    for file_path in file_list:
                        filename = os.path.basename(file_path)
                        sp_name = f"/{backup_name}/{filename}"
                        object_list.append({
                            "filesystem": "/",
                            "isDir": False,
                            "name": sp_name,
                            "path": file_path,
                            "isLink": False
                        })
                    logger.info(f"Including {len(file_list)} file(s) explicitly in backup")
                else:
                    # No file list - backup entire directory
                    object_list = [{
                        "filesystem": "/",
                        "isDir": True,
                        "name": f"/{backup_name}",
                        "path": backup_path,
                        "isLink": False
                    }]
                    logger.info(f"Backing up entire directory: {backup_path}")

                payload = {
                    "backupType": "selective",
                    "objectList": object_list
                }

                logger.info(f"Starting backup job (attempt {attempt + 1}/{self.max_retries})")
                response = await self.session.post(url, headers=headers, json=payload)
                response.raise_for_status()

                result = response.json()

                # Check for API errors
                if result.get("statusRC") != 0:
                    error_msg = result.get("statusMessage", "Unknown error")
                    error_code = result.get("statusRC")
                    logger.error(f"Backup start error (RC={error_code}): {error_msg}")
                    raise Exception(f"Storage Protect API error (RC={error_code}): {error_msg}")

                task_id = result.get("taskId")
                logger.info(f"✓ Storage Protect backup started successfully!")
                logger.info(f"  Task ID: {task_id}")
                logger.info(f"  Backup Type: {backup_type}")
                logger.info(f"  Source Path: {backup_path}")
                logger.info(f"  SP Target: {sp_backup_path}")
                logger.info(f"  Node: {self.node_id}")

                return result

            except httpx.HTTPStatusError as e:
                logger.warning(f"HTTP error on attempt {attempt + 1}: {e.response.status_code}")
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(self.retry_delay)
                else:
                    logger.error(f"Backup start failed after {self.max_retries} attempts")
                    raise
                    
            except Exception as e:
                logger.warning(f"Backup start attempt {attempt + 1} failed: {e}")
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(self.retry_delay)
                else:
                    logger.error(f"Backup start failed after {self.max_retries} attempts")
                    raise

    async def poll_task_status(self, task_id: str) -> Dict[str, Any]:
        """
        Poll task status.
        
        Args:
            task_id: Task ID to poll
            
        Returns:
            Dictionary with task status
        """
        if not self.context_id:
            raise Exception("Not signed in to Storage Protect")
            
        url = f"{self.base_url}/Tasks/{task_id}"
        headers = self._common_headers()
        headers["X-Flr-Contextid"] = self.context_id

        response = await self.session.get(url, headers=headers)
        response.raise_for_status()
        result = response.json()
        
        # Check for API errors
        if result.get("statusRC") != 0:
            error_msg = result.get("statusMessage", "Unknown error")
            raise Exception(f"Task status error: {error_msg}")
            
        return result

    async def get_task_data(self, task_id: str) -> Dict[str, Any]:
        """
        Get detailed task data and results.
        
        Args:
            task_id: Task ID to query
            
        Returns:
            Dictionary with detailed task data
        """
        if not self.context_id:
            raise Exception("Not signed in to Storage Protect")
            
        url = f"{self.base_url}/Tasks/{task_id}/Data?getErrorList=false"
        headers = self._common_headers()
        headers["X-Flr-Contextid"] = self.context_id

        response = await self.session.get(url, headers=headers)
        response.raise_for_status()
        result = response.json()
        
        # Check for API errors
        if result.get("statusRC") != 0:
            error_msg = result.get("statusMessage", "Unknown error")
            raise Exception(f"Task data error: {error_msg}")
            
        return result

    async def delete_task(self, task_id: str) -> Dict[str, Any]:
        """
        Delete/cleanup a task.
        
        Args:
            task_id: Task ID to delete
            
        Returns:
            Dictionary with deletion result
        """
        if not self.context_id:
            raise Exception("Not signed in to Storage Protect")
            
        url = f"{self.base_url}/Tasks/{task_id}"
        headers = self._common_headers()
        headers["X-Flr-Contextid"] = self.context_id

        response = await self.session.delete(url, headers=headers)
        response.raise_for_status()
        result = response.json()
        
        # Check for API errors
        if result.get("statusRC") != 0:
            error_msg = result.get("statusMessage", "Unknown error")
            logger.warning(f"Task deletion warning: {error_msg}")
        else:
            logger.info(f"✓ Task {task_id} deleted successfully")
            
        return result

    async def wait_for_task_completion(self, task_id: str, max_wait_minutes: int = 10) -> Dict[str, Any]:
        """
        Wait for a task to complete with polling.
        
        Args:
            task_id: Task ID to monitor
            max_wait_minutes: Maximum time to wait in minutes
            
        Returns:
            Dictionary with final task status
            
        Raises:
            Exception: If task doesn't complete within timeout
        """
        max_polls = max_wait_minutes * 12  # Poll every 5 seconds
        poll_count = 0
        
        logger.info(f"Monitoring task {task_id} (max wait: {max_wait_minutes} minutes)")

        while poll_count < max_polls:
            try:
                status_result = await self.poll_task_status(task_id)
                task_state = status_result.get("taskState")
                
                # Log progress
                if poll_count % 6 == 0:  # Log every 30 seconds
                    logger.info(f"  Task {task_id}: {task_state} [{poll_count * 5}s elapsed]")

                # Check if completed
                if task_state in ["Success", "Failed", "Completed"]:
                    logger.info(f"✓ Task {task_id} completed with state: {task_state}")
                    return status_result

                await asyncio.sleep(5)
                poll_count += 1
                
            except Exception as e:
                logger.warning(f"Status poll error (poll {poll_count + 1}): {e}")
                if poll_count < max_polls - 1:
                    await asyncio.sleep(5)
                    poll_count += 1
                else:
                    raise

        raise Exception(f"Task {task_id} did not complete within {max_wait_minutes} minutes")

    async def close(self):
        """Close the HTTP session."""
        if self.session:
            await self.session.aclose()
            logger.info("Storage Protect client session closed")


# Example usage
async def main():
    """Example usage of StorageProtectClient for FOOBAZ node."""
    client = StorageProtectClient(
        base_url="https://9.11.52.248:9081/DpVe/api",
        node_id="FOOBAZ",
        node_admin_id="spectrumprotect",
        password="admin_12345_admin",
        http_port="1581",
        sp_backup_directory="/sp_backups/test",
        auto_cleanup_tasks=True
    )
    
    try:
        # Sign on
        await client.sign_on()
        
        # Start a test backup (example)
        # backup_result = await client.start_backup(
        #     backup_path="/path/to/backup",
        #     backup_name="test_backup_20241106"
        # )
        # task_id = backup_result.get("taskId")
        
        # Wait for completion
        # final_status = await client.wait_for_task_completion(task_id)
        
    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())