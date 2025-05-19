
import os
import json
import logging
import requests
import time
import msal
import threading
import concurrent.futures
import glob
from typing import List, Dict, Optional, Tuple, Any

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SharePointCleaner:
    """SharePoint content cleaner utility class"""

    def __init__(self, tenant_domain: str, target_library_name: str, site_name: str):
        """
        Initialize SharePointCleaner class

        Args:
            tenant_domain (str): SharePoint tenant domain
            target_library_name (str): Target document library name
            site_name (str): SharePoint site name
        """
        self.tenant_domain = tenant_domain
        self.target_library_name = target_library_name
        self.site_name = site_name
        self.access_token = None
        self.site_id = None
        self.list_id = None
        self.drive_id = None

    def get_access_token(self) -> str:
        """Get SharePoint access token"""
        try:
            with open("output_parameters.json") as f:
                config = json.load(f)

            authority = (
                f"https://login.microsoftonline.com/{config['tenant_id']}"
                if not config.get('authority')
                else config['authority']
            )

            app = msal.ConfidentialClientApplication(
                config["client_id"],
                authority=authority,
                client_credential=config["secret"],
            )

            result = app.acquire_token_for_client(scopes=config["scope"])

            if "access_token" in result:
                self.access_token = result["access_token"]
                return self.access_token
            else:
                logger.error(f"❌ Unable to get access token: {result.get('error_description', 'Unknown error')}")
                return ""
        except Exception as e:
            logger.error(f"❌ Error occurred while getting token: {str(e)}")
            return ""

    def get_sharepoint_info(self) -> Dict[str, str]:
        """Get SharePoint site and list information"""
        if not self.access_token:
            self.get_access_token()

        headers = {'Authorization': f'Bearer {self.access_token}'}
        tenant = self.tenant_domain.split('.')[0]

        try:
            # Get site ID
            site_url = f"https://graph.microsoft.com/v1.0/sites/{tenant}.sharepoint.com:/sites/{self.site_name}"
            response = requests.get(site_url, headers=headers)
            response.raise_for_status()
            self.site_id = response.json()['id']

            # Get document library ID
            lists_url = f"https://graph.microsoft.com/v1.0/sites/{self.site_id}/lists"
            response = requests.get(lists_url, headers=headers)
            response.raise_for_status()

            target_list = next(
                (lst for lst in response.json()['value'] if lst['name'] == self.target_library_name),
                None
            )

            if not target_list:
                raise Exception(f"Target library not found: {self.target_library_name}")

            self.list_id = target_list['id']

            # Get drive ID
            drives_url = f"https://graph.microsoft.com/v1.0/sites/{self.site_id}/drives"
            response = requests.get(drives_url, headers=headers)
            response.raise_for_status()

            if not response.json()['value']:
                raise Exception("Drive not found")

            self.drive_id = response.json()['value'][0]['id']

            return {
                'site_id': self.site_id,
                'list_id': self.list_id,
                'drive_id': self.drive_id
            }

        except requests.exceptions.RequestException as e:
            raise Exception(f"Failed to get SharePoint info: {str(e)}")

    def get_items_recursive(self, folder_path: str = "") -> List[Dict]:
        """
        Recursively get all items (files and folders) under specified folder

        Args:
            folder_path: Folder path, empty means root directory

        Returns:
            List[Dict]: List of items
        """
        if not self.access_token:
            self.get_access_token()

        if not self.drive_id:
            self.get_sharepoint_info()

        headers = {'Authorization': f'Bearer {self.access_token}'}
        items = []

        try:
            # Build API URL
            if folder_path:
                # URL encode the path
                encoded_path = folder_path.replace(" ", "%20")
                url = f"https://graph.microsoft.com/v1.0/drives/{self.drive_id}/root:/{encoded_path}:/children"
            else:
                url = f"https://graph.microsoft.com/v1.0/drives/{self.drive_id}/root/children"

            # Send request
            response = requests.get(url, headers=headers)
            response.raise_for_status()

            # Process each item
            for item in response.json().get('value', []):
                item_info = {
                    'id': item.get('id'),
                    'name': item.get('name'),
                    'path': folder_path + '/' + item.get('name') if folder_path else item.get('name'),
                    'type': 'folder' if 'folder' in item else 'file',
                    'size': item.get('size', 0),
                    'created_time': item.get('createdDateTime'),
                    'modified_time': item.get('lastModifiedDateTime')
                }

                items.append(item_info)

                # If it's a folder, recursively get child items
                if 'folder' in item:
                    child_path = item_info['path']
                    child_items = self.get_items_recursive(child_path)
                    items.extend(child_items)

            return items

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to get item list: {str(e)}")
            return []

    def delete_item(self, item_id: str) -> bool:
        """
        Delete specified item (file or folder)

        Args:
            item_id: Item ID

        Returns:
            bool: Whether deletion was successful
        """
        if not self.access_token:
            self.get_access_token()

        if not self.drive_id:
            self.get_sharepoint_info()

        headers = {'Authorization': f'Bearer {self.access_token}'}
        url = f"https://graph.microsoft.com/v1.0/drives/{self.drive_id}/items/{item_id}"

        try:
            response = requests.delete(url, headers=headers)
            return response.status_code == 204  # Successful deletion returns 204 No Content
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to delete item: {str(e)}")
            return False

    def get_all_fields(self) -> List[Dict]:
        """Get all fields"""
        if not self.access_token:
            self.get_access_token()

        if not self.site_id or not self.list_id:
            self.get_sharepoint_info()

        headers = {'Authorization': f'Bearer {self.access_token}'}
        column_url = f"https://graph.microsoft.com/v1.0/sites/{self.site_id}/lists/{self.list_id}/columns"

        try:
            response = requests.get(column_url, headers=headers)
            response.raise_for_status()
            return response.json().get("value", [])
        except requests.exceptions.RequestException as e:
            raise Exception(f"Failed to get field list: {str(e)}")

    def delete_field(self, field_name: str) -> bool:
        if not self.access_token:
            self.get_access_token()

        if not self.site_id or not self.list_id:
            self.get_sharepoint_info()

        headers = {'Authorization': f'Bearer {self.access_token}'}
        delete_url = f"https://graph.microsoft.com/v1.0/sites/{self.site_id}/lists/{self.list_id}/columns/{field_name}"

        try:
            response = requests.delete(delete_url, headers=headers)
            return response.status_code == 204
        except requests.exceptions.RequestException:
            return False

    def delete_all_items(self, folder_path: str = "", max_workers: int = 10, retry_count: int = 3) -> Tuple[int, int]:
        """
        Delete all items (files and folders) in the specified folder using multithreading

        Args:
            folder_path: Folder path, empty means root directory
            max_workers: Maximum number of worker threads, default is 10
            retry_count: Number of retry attempts for failed deletions, default is 3

        Returns:
            Tuple[int, int]: (successful deletions, failed deletions)
        """
        # Get all items
        items = self.get_items_recursive(folder_path)

        # Sort by path length to ensure child items are deleted first
        items.sort(key=lambda x: len(x['path']), reverse=True)

        # Use thread-safe counters
        success_count = 0
        failed_count = 0
        lock = threading.Lock()
        total_items = len(items)

        # Define function to process single item
        def process_item(item):
            nonlocal success_count, failed_count

            item_id = item['id']
            item_path = item['path']
            item_type = item['type']

            logger.info(f"Deleting {item_type}: {item_path}")

            # Add retry mechanism
            for attempt in range(retry_count):
                try:
                    if self.delete_item(item_id):
                        with lock:
                            success_count += 1
                        logger.info(f"✅ Successfully deleted {item_type}: {item_path}")
                        return True
                    else:
                        # If not the last attempt, wait and retry
                        if attempt < retry_count - 1:
                            logger.warning(f"⚠️ Failed to delete {item_type}: {item_path}, retrying ({attempt + 1}/{retry_count})")
                            time.sleep(0.1 * (attempt + 1))  # Gradually increase delay
                        else:
                            with lock:
                                failed_count += 1
                            logger.error(f"❌ Failed to delete {item_type}: {item_path} after {retry_count} attempts")
                            return False
                except Exception as e:
                    # If not the last attempt, wait and retry
                    if attempt < retry_count - 1:
                        logger.warning(f"⚠️ Error while deleting {item_type}: {str(e)}, retrying ({attempt + 1}/{retry_count})")
                        time.sleep(0.1 * (attempt + 1))  # Gradually increase delay
                    else:
                        with lock:
                            failed_count += 1
                        logger.error(f"❌ Error while deleting {item_type}: {str(e)} after {retry_count} attempts")
                        return False

        # Use thread pool for parallel processing
        logger.info(f"🚀 Starting multithreaded deletion of {total_items} items with {max_workers} max workers...")

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            futures = [executor.submit(process_item, item) for item in items]

            # Process completed tasks and show progress
            for i, future in enumerate(concurrent.futures.as_completed(futures)):
                try:
                    future.result()  # Get result (will raise exception if any)
                except Exception as e:
                    logger.error(f"❌ Task execution failed: {str(e)}")

                # Show progress
                progress = ((i + 1) / total_items) * 100
                if (i + 1) % 10 == 0 or (i + 1) == total_items:
                    logger.info(f"📊 Progress: {progress:.1f}% ({i + 1}/{total_items})")

        logger.info(f"✅ Deletion completed, successful: {success_count}, failed: {failed_count}, total: {total_items}")
        return success_count, failed_count

    def delete_log_files(self) -> Tuple[int, int]:
        """
        Delete breakpoint resume log files related to current site

        Returns:
            Tuple[int, int]: (successful deletions, failed deletions)
        """
        if not self.site_name:
            logger.warning("⚠️ Cannot delete log files: site name is empty")
            return 0, 0

        success_count = 0
        failed_count = 0

        try:
            # Ensure log directory exists
            if not os.path.exists('logs'):
                logger.info("✅ Log directory does not exist, no need to delete")
                return 0, 0

            # Find log files related to the site
            log_pattern = f"logs/upload_log_{self.site_name}_*.json"
            log_files = glob.glob(log_pattern)

            if not log_files:
                logger.info(f"✅ No log files found for site '{self.site_name}'")
                return 0, 0

            logger.info(f"🗑️ Starting deletion of {len(log_files)} log files related to site '{self.site_name}'...")

            # Delete each log file
            for log_file in log_files:
                try:
                    os.remove(log_file)
                    success_count += 1
                    logger.info(f"✅ Successfully deleted log file: {log_file}")
                except Exception as e:
                    failed_count += 1
                    logger.error(f"❌ Failed to delete log file {log_file}: {str(e)}")

            logger.info(f"📊 Log file deletion completed, successful: {success_count}, failed: {failed_count}, total: {len(log_files)}")
            return success_count, failed_count

        except Exception as e:
            logger.error(f"❌ Error occurred while deleting log files: {str(e)}")
            return success_count, failed_count

    def delete_sharepoint_content(self, delete_fields: bool = True, delete_files: bool = True, max_workers: int = 10) -> Dict[str, Any]:
        """
        Main function to delete SharePoint content

        Args:
            delete_fields: Whether to delete tag fields
            delete_files: Whether to delete files and folders
            max_workers: Maximum number of worker threads, default is 10

        Returns:
            Dict[str, Any]: Operation result statistics
        """
        result = {
            'success': False,
            'fields': {'success': 0, 'failed': 0, 'total': 0},
            'items': {'success': 0, 'failed': 0, 'total': 0},
            'logs': {'success': 0, 'failed': 0, 'total': 0},
            'error': None
        }

        try:
            # Step 1: Get access token
            logger.info("🔑 Getting access token...")
            self.access_token = self.get_access_token()
            if not self.access_token:
                result['error'] = "Unable to get access token"
                return result

            # Step 2: Get SharePoint information
            logger.info("📋 Getting SharePoint information...")
            sp_info = self.get_sharepoint_info()
            self.site_id = sp_info['site_id']
            self.list_id = sp_info['list_id']
            self.drive_id = sp_info['drive_id']

            # Step 3: Delete files and folders (if needed)
            if delete_files:
                logger.info("🗑️ Starting deletion of files and folders...")
                success_count, failed_count = self.delete_all_items(max_workers=max_workers)

                result['items']['success'] = success_count
                result['items']['failed'] = failed_count
                result['items']['total'] = success_count + failed_count

                logger.info(f"✅ Successfully deleted: {success_count} items")
                logger.info(f"❌ Failed to delete: {failed_count} items")
                logger.info(f"📊 Total processed: {success_count + failed_count} items")

            # Step 4: Delete tag fields (if needed)
            if delete_fields:
                logger.info("📋 Getting field list...")
                all_fields = self.get_all_fields()

                # Filter tag fields
                tag_fields = [field for field in all_fields if field["name"]]

                if not tag_fields:
                    logger.info("✅ No tag fields found to delete")
                else:
                    logger.info(f"🗑️ Starting deletion of {len(tag_fields)} tag fields...")

                    # Use multithreading to delete fields
                    success_count = 0
                    failed_count = 0
                    lock = threading.Lock()
                    total_fields = len(tag_fields)

                    def process_field(field):
                        nonlocal success_count, failed_count
                        field_name = field["name"]
                        logger.info(f"Deleting field: {field_name}")

                        # Add retry mechanism
                        for attempt in range(3):
                            try:
                                if self.delete_field(field_name):
                                    with lock:
                                        success_count += 1
                                    logger.info(f"✅ Successfully deleted field: {field_name}")
                                    return True
                                else:
                                    # If not the last attempt, wait and retry
                                    if attempt < 2:
                                        logger.warning(f"⚠️ Failed to delete field: {field_name}, retrying ({attempt + 1}/3)")
                                        time.sleep(0.2 * (attempt + 1))  # Gradually increase delay
                                    else:
                                        with lock:
                                            failed_count += 1
                                        logger.error(f"❌ Failed to delete field: {field_name} after 3 attempts")
                                        return False
                            except Exception as e:
                                # If not the last attempt, wait and retry
                                if attempt < 2:
                                    logger.warning(f"⚠️ Error while deleting field: {str(e)}, retrying ({attempt + 1}/3)")
                                    time.sleep(0.2 * (attempt + 1))  # Gradually increase delay
                                else:
                                    with lock:
                                        failed_count += 1
                                    logger.error(f"❌ Error while deleting field: {str(e)} after 3 attempts")
                                    return False

                    # Use thread pool for parallel processing
                    logger.info(f"🚀 Starting multithreaded deletion of {total_fields} fields with {max_workers} max workers...")

                    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                        # Submit all tasks
                        futures = [executor.submit(process_field, field) for field in tag_fields]

                        # Process completed tasks and show progress
                        for i, future in enumerate(concurrent.futures.as_completed(futures)):
                            try:
                                future.result()  # Get result (will raise exception if any)
                            except Exception as e:
                                logger.error(f"❌ Task execution failed: {str(e)}")

                            # Show progress
                            progress = ((i + 1) / total_fields) * 100
                            if (i + 1) % 5 == 0 or (i + 1) == total_fields:
                                logger.info(f"📊 Progress: {progress:.1f}% ({i + 1}/{total_fields})")

                    result['fields']['success'] = success_count
                    result['fields']['failed'] = failed_count
                    result['fields']['total'] = total_fields

                    logger.info(f"✅ Successfully deleted: {success_count} fields")
                    logger.info(f"❌ Failed to delete: {failed_count} fields")
                    logger.info(f"📊 Total processed: {total_fields} fields")

            # Step 5: Delete breakpoint resume log files related to the site
            logger.info("🗑️ Starting deletion of breakpoint resume log files related to the site...")
            success_count, failed_count = self.delete_log_files()

            result['logs']['success'] = success_count
            result['logs']['failed'] = failed_count
            result['logs']['total'] = success_count + failed_count

            if success_count + failed_count > 0:
                logger.info(f"✅ Successfully deleted: {success_count} log files")
                logger.info(f"❌ Failed to delete: {failed_count} log files")
                logger.info(f"📊 Total processed: {success_count + failed_count} log files")

            # Step 6: Show result statistics
            logger.info("\n=== Deletion Operation Completed ===")
            result['success'] = True
            return result

        except Exception as e:
            error_msg = str(e)
            logger.error(f"❌ Program execution error: {error_msg}")
            result['error'] = error_msg
            return result

    def delete_all_tag_fields(self) -> None:
        """Main function to delete all tag fields (kept for backward compatibility)"""
        result = self.delete_sharepoint_content(delete_fields=True, delete_files=False)
        if not result['success']:
            logger.error(f"❌ Failed to delete tag fields: {result['error']}")


# Keep original function interfaces for backward compatibility
def delete_sharepoint_content(site_name: str, delete_fields: bool = True, delete_files: bool = True) -> Dict[str, Any]:
    """
    Main function to delete SharePoint content (backward compatibility)

    Args:
        site_name: Site name
        delete_fields: Whether to delete tag fields
        delete_files: Whether to delete files and folders

    Returns:
        Dict[str, Any]: Operation result statistics
    """
    cleaner = SharePointCleaner("jcardcorp.sharepoint.com", "Shared Documents", site_name)
    return cleaner.delete_sharepoint_content(delete_fields, delete_files)


def delete_all_tag_fields(site_name: str) -> None:
    """Main function to delete all tag fields (backward compatibility)"""
    cleaner = SharePointCleaner("jcardcorp.sharepoint.com", "Shared Documents", site_name)
    cleaner.delete_all_tag_fields()


if __name__ == "__main__":
    import sys
    import argparse

    # Create command line argument parser
    parser = argparse.ArgumentParser(description="Delete SharePoint content (files, folders, and tag fields)")
    parser.add_argument("site_name", help="SharePoint site name")
    parser.add_argument("--tenant-domain", default="jcardcorp.sharepoint.com", help="SharePoint tenant domain")
    parser.add_argument("--library-name", default="Shared Documents", help="Target document library name")
    parser.add_argument("--fields-only", action="store_true", help="Only delete tag fields")
    parser.add_argument("--files-only", action="store_true", help="Only delete files and folders")
    parser.add_argument("--max-workers", type=int, default=10, help="Maximum number of worker threads, default is 10")

    # Parse command line arguments
    args = parser.parse_args()

    # Set deletion options
    delete_fields = not args.files_only
    delete_files = not args.fields_only

    logger.info(f"🚀 Starting to process SharePoint site: {args.site_name}")
    logger.info(f"Tenant domain: {args.tenant_domain}")
    logger.info(f"Library name: {args.library_name}")
    logger.info(f"Delete tag fields: {delete_fields}")
    logger.info(f"Delete files and folders: {delete_files}")
    logger.info(f"Maximum worker threads: {args.max_workers}")

    # Create SharePointCleaner instance
    cleaner = SharePointCleaner(args.tenant_domain, args.library_name, args.site_name)

    # Execute deletion operation
    result = cleaner.delete_sharepoint_content(delete_fields, delete_files, args.max_workers)

    # Show results
    if result['success']:
        logger.info("✅ Operation completed successfully!")

        # Show detailed statistics
        if delete_files:
            logger.info(f"Files and folders: Successfully deleted {result['items']['success']}, failed {result['items']['failed']}, total {result['items']['total']}")

        if delete_fields:
            logger.info(f"Tag fields: Successfully deleted {result['fields']['success']}, failed {result['fields']['failed']}, total {result['fields']['total']}")

        # Show log file deletion statistics
        if result['logs']['total'] > 0:
            logger.info(f"Breakpoint resume logs: Successfully deleted {result['logs']['success']}, failed {result['logs']['failed']}, total {result['logs']['total']}")

        sys.exit(0)
    else:
        logger.error(f"❌ Operation failed: {result['error']}")
        sys.exit(1)




