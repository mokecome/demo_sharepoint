import streamlit as st
import requests
import time
import json
import logging
import os
import pandas as pd
import shutil
import threading
import queue
import concurrent.futures
import datetime
import random
from typing import Dict, List, Any, Optional, Tuple
from document_classifier import AutoDocumentClassifier
from sharepoint_utils import SharePointClient, normalize_path, extract_text_from_docx, extract_text_from_xlsx, extract_text_from_pptx, extract_text_from_dxf, extract_text_from_zip
from breakpoint_resume_log import SharePointUploadLogger
from urllib.parse import quote
from pathlib import Path

sp_client = None
# List of fields to filter when using SharePointClient methods
fiter_list= ['id', 'name', 'path', 'web_url', 'created_time', 'modified_time', 'size', '@odata.context', '@odata.etag', 'MediaServiceImageTags', 'ContentType', 'Created', 'AuthorLookupId', 'Modified', 'EditorLookupId', 'LinkFilenameNoMenu', 'LinkFilename', 'ItemChildCount', 'FolderChildCount', 'Edit', 'ParentVersionStringLookupId', 'ParentLeafNameLookupId', 'type', 'child_count', 'children','AppAuthorLookupId', 'AppEditorLookupId', 'file_type', 'hash', 'download_url','DocIcon','FileSizeDisplay']


def normalize_path(path: str) -> str:
    """
    Normalize file path for consistent handling across platforms
    Args:
        path: Input file path
    Returns:
        str: Normalized path with forward slashes
    """
    if not path:
        return ""
    return str(Path(path).resolve()).replace('\\', '/').strip('/')

def encode_url_path(path: str) -> str:
    """
    Properly encode path for URL usage
    Args:
        path: Input path
    Returns:
        str: URL encoded path
    """
    if not path:
        return ""
    # Split path into parts and encode each part separately
    parts = path.split('/')
    encoded_parts = [quote(part) for part in parts]
    return '/'.join(encoded_parts)

def get_relative_path(path: str, base_dir: str = 'target') -> str:
    """
    Get relative path from base directory
    Args:
        path: Input path
        base_dir: Base directory to calculate relative path from
    Returns:
        str: Relative path
    """
    try:
        return os.path.relpath(path, base_dir).replace('\\', '/')
    except ValueError:
        return path.replace('\\', '/')

def get_field_info_for_file(file_path: str) -> Tuple[str, str]:
    """
    Get corresponding tag information based on file path
    Args:
        file_path: File path, can be relative or absolute path
    Returns:
        Tuple[str str]: (FIELD_NAME, FIELD_VALUE)
    """
    try:
        # Get filename and normalize path
        file_name = os.path.basename(file_path)
        normalized_path = normalize_path(file_path)
        logger.info(f"Processing file: {file_path}")

        # Read merged_sharepoint_data.json
        merged_data_path = "tag_result/merged_sharepoint_data.json"
        with open(merged_data_path, "r", encoding="utf-8") as f:
            merged_data = json.load(f)

        # Find matching tag info - first try full path match
        for item in merged_data:
            item_path = normalize_path(item.get("path", ""))
            if item_path == normalized_path or item.get("name", "") == normalized_path:
                # Use content_tag as tag value
                tag = item.get("content_tag", item.get("tag", "unknown_tag"))
                return "content_tag", tag

        # If full path match fails, try filename only match
        for item in merged_data:
            if item.get("name", "") == file_name or os.path.basename(item.get("path", "")) == file_name:
                # Use content_tag as tag value
                tag = item.get("content_tag", item.get("tag", "unknown_tag"))
                return "content_tag", tag

    except Exception as e:
        logger.error(f"❌ Error occurred while reading tag information: {str(e)}")
        return "", ""

def download_file(access_token, drive_id, file_path, local_path):
    """Download file using SharePointClient"""
    global sp_client
    if not sp_client:
        sp_client = SharePointClient()
        sp_client.access_token = access_token
        sp_client.drive_id = drive_id

    try:
        content = sp_client.download_file(file_path)
        if content:
            os.makedirs(os.path.dirname(local_path), exist_ok=True)

            with open(local_path, 'wb') as f:
                f.write(content)
            return True
        return False
    except Exception as e:
        print(f"Error downloading file: {str(e)}")
        return False

def get_list_item_fields(access_token, drive_id):
    """Get item field information using SharePointClient"""
    global sp_client
    if not sp_client:
        sp_client = SharePointClient()
        sp_client.access_token = access_token
        sp_client.drive_id = drive_id

    return sp_client.get_list_item_fields()

def get_items_recursive(access_token, drive_id):
    """Recursively get items using SharePointClient"""
    global sp_client
    if not sp_client:
        sp_client = SharePointClient()
        sp_client.access_token = access_token

        sp_client.drive_id = drive_id

    return sp_client.get_items_recursive()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def read_and_tag_documents_mt(access_token: str, drive_id: str, max_workers: int = 10, read_json_file:str = "sharepoint_fields.json",
                            temp_dir: str = "temp", tag_dir: str = "tag_result") -> Dict:
    """
    Read file contents and tag them using multi-threading, maintaining directory structure including empty folders.
    Optimization: 1) Two-stage download strategy 2) File integrity check 3) Hybrid multi/single-thread 4) Zero omission guarantee
    """
    import math
    import time
    
    # Initialize directories and thread pool size
    os.makedirs(temp_dir, exist_ok=True)
    os.makedirs(tag_dir, exist_ok=True)
    os.makedirs('target', exist_ok=True)
    
    # Pre-initialize worker_count to ensure definition in all code paths
    worker_count = max_workers
    
    # Initialize all_downloaded list at the beginning
    all_downloaded = []  # 在这里初始化
    
    # Path normalization function
    def normalize_path_consistently(path):
        """Ensure consistent path separators across OS"""
        if not path:
            return ""
        normalized = path.strip("/\\").replace("\\", "/")
        return normalized
    
    # Statistics
    stats = {
        'total_items': 0,
        'files': 0,
        'folders': 0,
        'success': 0,
        'failed': 0,
        'downloaded': 0,
        'downloaded_retry': 0,  # Second stage download count
        'skipped': 0,
        'matched': 0,
        'processed': 0,  # New processing counter
        'total': 0,      # New total counter
        'lock': threading.Lock()
    }
    lock = threading.Lock()
    content_tags = []
    
    # Initialize SharePointClient
    global sp_client
    if not sp_client:
        sp_client = SharePointClient()
        sp_client.access_token = access_token
        sp_client.drive_id = drive_id
    
    try:
        # Step 1: Get/load file list and field info
        print("\n📥 Getting/loading file list and field info...")
        
        json_file_path = f"{tag_dir}/{read_json_file}"
        # Always get new field info
        print("📥 Getting new field info...")
        all_items = st.session_state.source_manager.get_items_with_fields_recursive_mt(drive_id)
        
        if not all_items:
            print("❌ No items retrieved")
            return stats
        
        # Save original field info
        with open(json_file_path, "w", encoding="utf-8") as f:
            json.dump(all_items, f, ensure_ascii=False, indent=2)
        print(f"✅ Field info saved to {json_file_path}")
        
        # Extract all file and folder items
        file_items = []
        folder_items = []
        
        def extract_items(items, file_list, folder_list):
            for item in items:
                if item['type'] == 'file':
                    file_list.append(item)
                else:  # folder
                    folder_list.append(item)
                    if 'children' in item and item['children']:
                        extract_items(item['children'], file_list, folder_list)
        
        extract_items(all_items, file_items, folder_items)
        
        with lock:
            stats['total_items'] = len(file_items) + len(folder_items)
            stats['files'] = len(file_items)
            stats['folders'] = len(folder_items)
        print(f"✅ Identified {len(file_items)} files and {len(folder_items)} folders")
        
        # Step 2: Pre-create directory structure
        print("\n📁 Pre-creating directory structure...")
        for folder in folder_items:
            folder_path = os.path.join(temp_dir, normalize_path_consistently(folder['path']))
            if not os.path.exists(folder_path):
                os.makedirs(folder_path, exist_ok=True)
                print(f"📁 Folder created: {folder_path}")
        
        # Step 3: Scan local files, check against sharepoint_fields.json
        print("\n🔍 Scanning local files and comparing against SharePoint field info...")
        
        # Create all normalized paths for files to download (from sharepoint_fields.json)
        all_sharepoint_files = {}  # Format: {normalized path: (file_item, file size)}
        for file_item in file_items:
            path = file_item.get("path", "")
            norm_path = normalize_path_consistently(path)
            file_size = file_item.get("size", 0)
            all_sharepoint_files[norm_path] = (file_item, file_size)
        
        # Scan temp directory, collect information about downloaded files
        existing_files = {}  # Format: {normalized path: file size}
        for root, dirs, files in os.walk(temp_dir):
            for file in files:
                local_path = os.path.join(root, file)
                rel_path = os.path.relpath(local_path, temp_dir)
                norm_path = normalize_path_consistently(rel_path)
                
                # Record file size for integrity check
                try:
                    file_size = os.path.getsize(local_path)
                    existing_files[norm_path] = file_size
                except Exception as e:
                    print(f"⚠️ Failed to get file size: {local_path} - {str(e)}")
                    # File might be problematic, do not add to existing file list
        
        print(f"✅ Scan complete: {len(all_sharepoint_files)} SharePoint files, {len(existing_files)} local files exist")
        
        # Classify files by comparison: need to download, need to update, already up-to-date
        files_to_download = []  # Files to download
        files_to_update = []    # Files to update (size mismatch)
        skipped_files = []      # Files to skip (already exist and size matches)
        
        for norm_path, (file_item, remote_size) in all_sharepoint_files.items():
            if norm_path not in existing_files:
                # Not present locally, need to download
                files_to_download.append(file_item)
            elif existing_files[norm_path] != remote_size and remote_size > 0:
                # Exists locally but size mismatch, need to update
                files_to_update.append(file_item)
                print(f"⚠️ File size mismatch, will re-download: {file_item.get('path', '')}")
            else:
                # Exists locally and size matches, skip
                skipped_files.append(file_item)
        
        # Merge files to download and update
        all_download_files = files_to_download + files_to_update
        
        # Update statistics
        with lock:
            stats['skipped'] = len(skipped_files)
        
        print(f"📥 Need to download {len(files_to_download)} new files, update {len(files_to_update)} files, skip {len(skipped_files)} complete files")
        
        # ==== Phase 1: Multithreaded fast download ====
        if all_download_files:
            print("\n📦 Phase 1: Multithreaded fast download...")
            
            # Dynamically adjust thread pool size
            dynamic_workers = min(32, max(8, math.ceil(len(all_download_files) / 5)))
            worker_count = max(dynamic_workers, max_workers)
            print(f"🚀 Using {worker_count} worker threads for parallel download")
            
            # Batch download function
            def download_file_batch(file_batch):
                downloaded_files = []
                download_failed = []
                session = requests.Session()
                adapter = requests.adapters.HTTPAdapter(max_retries=2)
                session.mount('https://', adapter)
                
                for file_item in file_batch:
                    try:
                        path = file_item["path"]
                        normalized_path = normalize_path_consistently(path)
                        local_path = os.path.join(temp_dir, normalized_path)
                        os.makedirs(os.path.dirname(local_path), exist_ok=True)
                        
                        success = False
                        
                        # URL
                        if 'download_url' in file_item:
                            try:
                                response = session.get(file_item['download_url'], stream=True, timeout=20)
                                if response.status_code == 200:
                                    with open(local_path, 'wb') as f:
                                        for chunk in response.iter_content(chunk_size=32768):
                                            f.write(chunk)
                                    success = True
                            except:
                                pass 
                        
                        # URL->API
                        if not success:
                            try:
                                content = sp_client.download_file(path)
                                if content:
                                    with open(local_path, 'wb') as f:
                                        f.write(content)
                                    success = True
                            except:
                                pass
                        
                        if success:
                            downloaded_files.append((file_item, local_path))
                            with lock:
                                stats['downloaded'] += 1
                        else:
                            download_failed.append(file_item)
                    except:
                        download_failed.append(file_item)
                
                return downloaded_files, download_failed
            
            # Batch download
            batch_size = 100  # 100 files per batch
            download_batches = [all_download_files[i:i+batch_size] for i in range(0, len(all_download_files), batch_size)]
            all_failed = []
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=worker_count) as executor:
                futures = [executor.submit(download_file_batch, batch) for batch in download_batches]
                
                for i, future in enumerate(concurrent.futures.as_completed(futures)):
                    try:
                        batch_downloaded, batch_failed = future.result()
                        all_downloaded.extend(batch_downloaded)  
                        all_failed.extend(batch_failed)
                        print(f"📥 Downloaded: {stats['downloaded']}/{len(all_download_files)} files")
                    except Exception as e:
                        print(f"❌ Batch download failed: {str(e)}")
            
            print(f"✅ Phase 1 complete: {stats['downloaded']} files downloaded, {len(all_failed)} files failed")
            
            # ==== Phase 2: Single-threaded retry for failed files ====
            if all_failed:
                print(f"🔄 Phase 2: Retrying {len(all_failed)} files")
                
                # Initialize retry session with robust configuration
                retry_session = requests.Session()
                retry_adapter = requests.adapters.HTTPAdapter(
                    max_retries=5,
                    pool_connections=1,  # Single connection for sequential downloads
                    pool_maxsize=1
                )
                retry_session.mount('https://', retry_adapter)
                
                
                # Sequential download with verification
                for file_item in all_failed:
                    path = file_item.get("path", "")
                    file_size = file_item.get("size", 0)
                    local_path = os.path.join(temp_dir, normalize_path_consistently(path))
                    
                    print(f"🔄 Retrying download: {path}")
                    
                    try:
                        # Ensure directory exists
                        os.makedirs(os.path.dirname(local_path), exist_ok=True)
                        
                        # Try direct URL download first
                        success = False
                        if 'download_url' in file_item:
                            try:
                                with retry_session.get(file_item['download_url'], stream=True, timeout=30) as response:
                                    response.raise_for_status()
                                    total_size = int(response.headers.get('content-length', 0))
                                    
                                    with open(local_path, 'wb') as f:
                                        downloaded_size = 0
                                        for chunk in response.iter_content(chunk_size=8192):
                                            if chunk:
                                                f.write(chunk)
                                                downloaded_size += len(chunk)
                                                # Print progress
                                                if total_size > 0:
                                                    progress = (downloaded_size / total_size) * 100
                                                    print(f" Download progress: {progress:.1f}%", end='\r')
                                    
                                    # Verify file size after download
                                    if os.path.getsize(local_path) == file_size:
                                        success = True
                                        print(f"✅ Successfully downloaded via URL: {path}")
                            except Exception as e:
                                print(f"⚠️ URL download failed: {str(e)}")
                                if os.path.exists(local_path):
                                    os.remove(local_path)
                        
                        # If URL download failed, try API download
                        if not success:
                            try:
                                content = sp_client.download_file(path)
                                if content:
                                    with open(local_path, 'wb') as f:
                                        f.write(content)
                                    
                                    # Verify file size after API download
                                    if os.path.getsize(local_path) == file_size:
                                        success = True
                                        print(f"✅ Successfully downloaded via API: {path}")
                            except Exception as e:
                                print(f"⚠️ API download failed: {str(e)}")
                                if os.path.exists(local_path):
                                    os.remove(local_path)
                        
                        # Update statistics based on download result
                        if success:
                            all_downloaded.append((file_item, local_path))
                            with lock:
                                stats['downloaded_retry'] += 1
                        else:
                            print(f"❌ All download methods failed for: {path}")
                            if os.path.exists(local_path):
                                os.remove(local_path)
                            
                    except Exception as e:
                        print(f"❌ Download failed for {path}: {str(e)}")
                        if os.path.exists(local_path):
                            os.remove(local_path)
                        continue
                    
                    # Add small delay between downloads
                    time.sleep(1)
            
            # Add skipped files to downloaded list for further processing
            for file_item in skipped_files:
                try:
                    path = file_item["path"]
                    normalized_path = normalize_path_consistently(path)
                    local_path = os.path.join(temp_dir, normalized_path)
                    all_downloaded.append((file_item, local_path))  # 使用append添加跳过的文件
                except Exception as e:
                    print(f"⚠️ Error processing skipped file: {str(e)}")
                    continue
            
            # Display final download statistics
            print(f"\n✅ File download complete:")
            print(f"   - Phase 1: {stats['downloaded']} files")
            print(f"   - Phase 2: {stats['downloaded_retry']} files")
            print(f"   - Skipped: {stats['skipped']} files")
        
        # Verify final download results, ensure no files are missed
        print("\n🔍 Final verification of download results, checking for any missed files...")
        
        # Calculate expected normalized path set for downloaded files
        expected_paths = set()
        path_to_item = {}  # Mapping from path to file item
        for file_item in file_items:
            path = file_item.get("path", "")
            norm_path = normalize_path_consistently(path)
            expected_paths.add(norm_path)
            path_to_item[norm_path] = file_item
        
        # Calculate actual normalized path set for downloaded files
        downloaded_paths = set()
        for file_item, local_path in all_downloaded:
            path = file_item.get("path", "")
            norm_path = normalize_path_consistently(path)
            downloaded_paths.add(norm_path)
        
        # Calculate missing files
        missing_paths = expected_paths - downloaded_paths
        
        if missing_paths:
            print(f"⚠️ Final discovery of {len(missing_paths)} files not successfully downloaded, marked as processing failure")
            with lock:
                stats['failed'] += len(missing_paths)
            # Record names of missed files for reference
            for i, missing_path in enumerate(list(missing_paths)[:10]):  # Only show first 10
                print(f"  - {missing_path}")
            if len(missing_paths) > 10:
                print(f"  - ... {len(missing_paths) - 10} more not shown")
        else:
            print("✅ Verification complete, all files successfully processed, no missed files")
        
        # Step 9: Content extraction
        print(f"\n🔍 Extracting content from {len(all_downloaded)} files...")
        
        # Batch content extraction function
        def extract_content_batch(file_batch):
            results = []
            for file_item, local_path in file_batch:
                try:
                    path = file_item["path"]
                    normalized_path = normalize_path_consistently(path)
                    file_ext = os.path.splitext(os.path.basename(local_path))[1].lower()
                    
               
                    extractors = {
                        ".docx": extract_text_from_docx,
                        ".xlsx": extract_text_from_xlsx,
                        ".pptx": extract_text_from_pptx,
                        ".dxf": extract_text_from_dxf,
                        ".zip": extract_text_from_zip
                    }
                    
                    content_tag = extractors.get(file_ext, lambda x: file_ext)(local_path) if file_ext in extractors else file_ext
                    
                    results.append({
                        "name": path,
                        "normalized_path": normalized_path,
                        "type": "file",
                        "content_tag": content_tag
                    })
                except Exception as e:
                    results.append({
                        "name": path,
                        "normalized_path": normalized_path,
                        "type": "file",
                        "content_tag": ""
                    })
            return results
        
        # Batch process content extraction
        batch_size = 100
        extract_batches = [all_downloaded[i:i+batch_size] for i in range(0, len(all_downloaded), batch_size)]
        
        # Re-evaluate worker_count before content extraction
        if len(extract_batches) > 0:
            dynamic_workers = min(32, max(8, math.ceil(len(extract_batches) / 5)))
            worker_count = max(dynamic_workers, max_workers)
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=worker_count) as executor:
            futures = [executor.submit(extract_content_batch, batch) for batch in extract_batches]
            
            for i, future in enumerate(concurrent.futures.as_completed(futures)):
                try:
                    batch_result = future.result()
                    content_tags.extend(batch_result)
                    stats['success'] += len(batch_result)
                    progress = ((i + 1) / len(extract_batches)) * 100
                    print(f"🔍 Processed batch {i+1}/{len(extract_batches)} ({progress:.1f}%)")
                except Exception as e:
                    print(f"❌ Failed to extract content: {str(e)}")
                    stats['failed'] += batch_size
        
        # Save content tag results
        with open(f"{tag_dir}/content_tags.json", "w", encoding="utf-8") as f:
            json.dump(content_tags, f, ensure_ascii=False, indent=2)
        print(f"✅ Content tags saved to {tag_dir}/content_tags.json")
        
        # Step 7: Merge results
        print("\n🔄 Merging field and content tags...")
        
        # Build content tag dictionary
        content_tags_dict = {}
        for content_item in content_tags:
            content_tags_dict[content_item["normalized_path"]] = content_item
            if "/" in content_item["normalized_path"]:
                filename = content_item["normalized_path"].split("/")[-1]
                if filename not in content_tags_dict:
                    content_tags_dict[filename] = content_item
        
        # Flatten all items
        all_items_flat = []
        def flatten_items(items):
            for item in items:
                all_items_flat.append(item)
                if 'children' in item and item['children']:
                    flatten_items(item['children'])
        flatten_items(all_items)
        
        # Merge results
        merged_data = []
        for field_item in all_items_flat:
            merged_item = field_item.copy()
            normalized_field_path = normalize_path_consistently(field_item["path"])
            
            if field_item["type"] == "folder":
                merged_data.append(merged_item)
                continue
                
            if normalized_field_path in content_tags_dict:
                merged_item["content_tag"] = content_tags_dict[normalized_field_path]["content_tag"]
                with lock:
                    stats['matched'] += 1
            else:
                if "/" in normalized_field_path:
                    filename = normalized_field_path.split("/")[-1]
                    if filename in content_tags_dict:
                        merged_item["content_tag"] = content_tags_dict[filename]["content_tag"]
                        with lock:
                            stats['matched'] += 1
                    else:
                        found_match = False
                        for content_path, content_item in content_tags_dict.items():
                            if (normalized_field_path in content_path or content_path in normalized_field_path) and len(content_path) > 3:
                                merged_item["content_tag"] = content_item["content_tag"]
                                with lock:
                                    stats['matched'] += 1
                                found_match = True
                                break
                        if not found_match:
                            merged_item["content_tag"] = "📄 " + (field_item["name"].split("/")[-1] if "/" in field_item["name"] else field_item["name"])
                else:
                    merged_item["content_tag"] = "📄 " + field_item["name"]
            merged_data.append(merged_item)
        
        # Save merged results
        with open(f"{tag_dir}/merged_sharepoint_data.json", "w", encoding="utf-8") as f:
            json.dump(merged_data, f, ensure_ascii=False, indent=2)
        print(f"✅ Merged results saved to {tag_dir}/merged_sharepoint_data.json")
        
        # Output final statistics
        print("\n📊 Processing statistics:")
        print(f"Total items: {stats['total_items']}")
        print(f"Files: {stats['files']}")
        print(f"Folders: {stats['folders']}")
        print(f"Successfully processed: {stats['success']}")
        print(f"Processing errors: {stats['failed']}")
        print(f"Files downloaded (Phase 1): {stats['downloaded']}")
        print(f"Files downloaded (Phase 2): {stats['downloaded_retry']}")
        print(f"Skipped: {stats['skipped']}")
        print(f"Tag matches: {stats['matched']}")
        
        print(f"\n✅ Done! Results saved to {tag_dir} directory")
        print(f"✅ Files downloaded to {temp_dir} directory")
        
        return stats
        
    except Exception as e:
        print(f"❌ Error occurred during processing: {str(e)}")
        logger.error(f"Processing error: {str(e)}", exc_info=True)
        return stats




class SharePointManager(SharePointClient):
    """SharePoint Manager, inherits from SharePointClient"""

    def __init__(self, client_id, client_secret, tenant_id, site_name, tenant_name):
        """
        Initialize SharePoint Manager
        Args:
            client_id (str): Azure AD application Client ID
            client_secret (str): Azure AD application Client Secret
            tenant_id (str): Azure AD tenant ID
            site_name (str): SharePoint site name
            tenant_name (str): SharePoint tenant name (e.g. "contoso" from contoso.sharepoint.com)
        """
        super().__init__(client_id=client_id, client_secret=client_secret,
                         tenant_id=tenant_id, site_name=site_name, tenant_name=tenant_name)

    def get_items_with_fields_recursive_mt(self, drive_id, current_path="", item_id=None, max_workers=10):
        """
        Recursively get all folder and file information with optimized performance.
        Args:
            drive_id (str): Drive ID
            current_path (str): Current path
            item_id (str): Item ID (for recursion)
            max_workers (int): Maximum worker threads
        Returns:
            list: List containing all item information
        """
        import time
        try:
            print(f"🔍 Start fetching SharePoint items...")
            
            # Core data structures
            results_dict = {}
            parent_child_map = {}
            item_queue = queue.Queue()
            failed_nodes = []
            
            # Thread-safe locks 
            results_lock = threading.Lock()
            parent_child_lock = threading.Lock()
            
            # Statistics counter with thread-safe lock
            stats = {
                'folders': 0, 
                'files': 0, 
                'errors': 0,
                'retry_success': 0,
                'processed': 0,  # New processing counter
                'total': 0,      # New total counter
                'lock': threading.Lock()
            }
            
            # Queue initial root item
            item_queue.put({
                'id': item_id,
                'path': current_path,
                'is_root': True
            })

            # Process a single item from the queue
            def process_item(queue_item):
                try:
                    headers = {"Authorization": f"Bearer {self.access_token}"}
                    current_id = queue_item['id']
                    current_path = queue_item['path']
                    is_root = queue_item.get('is_root', False)
                    
                    # Build API URL based on current item
                    url = f"{self.base_url}/drives/{drive_id}/root/children" if not current_id else f"{self.base_url}/drives/{drive_id}/items/{current_id}/children"
                    
                    # Get children items
                    response = requests.get(url, headers=headers)
                    if response.status_code != 200:
                        with stats['lock']:
                            stats['errors'] += 1
                        failed_nodes.append(queue_item)
                        return []

                    items = response.json().get('value', [])
                    items_info = []
                    
                    # Update total count when new items are found
                    with stats['lock']:
                        stats['total'] += len(items)
                    
                    # Pre-configure field request headers
                    field_headers = {"Authorization": f"Bearer {self.access_token}"}
                    
                    # Process each item
                    for item in items:
                        item_id = item.get('id')
                        name = item.get('name')
                        new_path = f"{current_path}/{name}" if current_path else name
                        
                        # Create basic item info
                        item_info = {
                            'id': item_id,
                            'name': name,
                            'path': new_path,
                            'web_url': item.get('webUrl'),
                            'created_time': item.get('createdDateTime'),
                            'modified_time': item.get('lastModifiedDateTime'),
                            'size': item.get('size', 0)
                        }
                        
                        # Fetch additional fields in one batch request
                        fields_url = f"{self.base_url}/drives/{drive_id}/items/{item_id}/listItem/fields"
                        fields_response = requests.get(fields_url, headers=field_headers)
                        if fields_response.status_code == 200:
                            fields = fields_response.json()
                            # Add non-system fields to item info
                            item_info.update({
                                k: v for k, v in fields.items() 
                                if k not in ['id', 'FileLeafRef'] and not k.startswith('_')
                            })
                        
                        # Process folder or file type
                        if 'folder' in item:
                            item_info.update({
                                'type': 'folder',
                                'child_count': item.get('folder', {}).get('childCount', 0),
                                'children': []
                            })
                            # Queue folder for processing its children
                            item_queue.put({
                                'id': item_id,
                                'path': new_path,
                                'is_root': False
                            })
                            with stats['lock']:
                                stats['folders'] += 1
                        elif 'file' in item:
                            item_info.update({
                                'type': 'file',
                                'file_type': item.get('file', {}).get('mimeType'),
                                'hash': item.get('file', {}).get('hashes', {}).get('quickXorHash'),
                                'download_url': item.get('@microsoft.graph.downloadUrl')
                            })
                            with stats['lock']:
                                stats['files'] += 1
                        
                        # Store item in results dictionary
                        with results_lock:
                            results_dict[item_id] = item_info
                        
                        # Build parent-child relationship
                        if not is_root:
                            with parent_child_lock:
                                parent_child_map.setdefault(current_id, []).append(item_id)
                        
                        items_info.append(item_info)
                        
                        # Update processed count
                        with stats['lock']:
                            stats['processed'] += 1
                    
                    return items_info
                except Exception as e:
                    with stats['lock']:
                        stats['errors'] += 1
                    failed_nodes.append(queue_item)
                    return []

            # Phase 1: Multi-threaded processing
            print("🚀 Phase 1: Parallel processing...")
            
            # Use ThreadPoolExecutor for concurrent processing
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = set()  # 使用 set 而不是 list
                
                # Start with root item
                futures.add(executor.submit(process_item, {'id': item_id, 'path': current_path, 'is_root': True}))
                
                # Process items until queue is empty and all futures completed
                while futures or not item_queue.empty():
                    # Wait for any future to complete
                    done, pending = concurrent.futures.wait(
                        futures, 
                        timeout=0.1,
                        return_when=concurrent.futures.FIRST_COMPLETED
                    )
                    
                    # Update futures set
                    futures = pending
                    
                    # Track processed items
                    if stats['total'] > 0:  # 確保總數不為零
                        progress = min(1.0, stats['processed'] / stats['total'])
                        if stats['processed'] % 20 == 0:  # 每處理20個項目更新一次進度
                            print(f"✅ Progress: {progress:.1%} ({stats['processed']}/{stats['total']})")
                    
                    # Add new tasks from queue
                    while not item_queue.empty() and len(futures) < max_workers:
                        futures.add(executor.submit(process_item, item_queue.get()))

            # Phase 2: Retry failed items with backoff
            if failed_nodes:
                print(f"🔄 Phase 2: Retrying {len(failed_nodes)} failed items...")
                still_failed = []
                
                for i, node in enumerate(failed_nodes):
                    try:
                        # Exponential backoff
                        backoff = min(1.5 * (i % 3 + 1), 3)
                        time.sleep(backoff)
                        
                        # Retry processing
                        result = process_item(node)
                        if result:
                            with stats['lock']:
                                stats['retry_success'] += 1
                        else:
                            still_failed.append(node)
                    except Exception:
                        still_failed.append(node)
                
                print(f"✅ Retry results: {stats['retry_success']} succeeded, {len(still_failed)} still failed")

            # Build hierarchy
            print("🔄 Building item hierarchy...")
            
            # Find top-level items (not children of any item)
            all_child_ids = set()
            for children in parent_child_map.values():
                all_child_ids.update(children)
            
            top_level_items = [
                item_info for item_id, item_info in results_dict.items() 
                if item_id not in all_child_ids
            ]
            
            # Recursive function to build hierarchy
            def build_hierarchy(items):
                for item in items:
                    if item['type'] == 'folder':
                        item_id = item['id']
                        if item_id in parent_child_map:
                            children = [results_dict[child_id] for child_id in parent_child_map[item_id] 
                                       if child_id in results_dict]
                            item['children'] = children
                            build_hierarchy(children)
                return items
            
            # Generate final hierarchy
            final_result = build_hierarchy(top_level_items)
            
            # Display final statistics
            total_items = len(results_dict)
            print(f"✅ Completed! {total_items} total items: {stats['folders']} folders, {stats['files']} files")
            
            return final_result
        
        except Exception as e:
            print(f"❌ Error fetching items: {str(e)}")
            return []


# Streamlit UI
st.set_page_config(page_title="SharePoint File Transfer", layout="wide")
st.markdown("<h2 style='color: #333;'>SharePoint File Transfer</h2>", unsafe_allow_html=True)

# Initialize session state
if 'source_manager' not in st.session_state:
    st.session_state.source_manager = None
if 'dest_manager' not in st.session_state:
    st.session_state.dest_manager = None

# Two-column input layout
col1, col2 = st.columns([2, 2])

# Load default values
try:
    with open('parameters.json', 'r') as f:
        source_params = json.load(f)
except:
    source_params = {}

try:
    with open('output_parameters.json', 'r') as f:
        dest_params = json.load(f)
except:
    dest_params = {}

with col1:
    st.subheader("Source URL")
    source_url = st.text_input(
        "Source URL",
        value=f"https://{source_params.get('tenant', 'sdabicorp')}.sharepoint.com",
        help="https://tenant.sharepoint.com tenant",
        key="source_url"
    )
    source_client_id = st.text_input(
        "Source Client ID",
        value=source_params.get('client_id', 'fe75973a-5fbf-40bd-a7e5-3a14b46c4744'),
        help="Client ID of the application",
        key="source_client_id"
    )
    source_client_secret = st.text_input(
        "Source Client Secret",
        value=source_params.get('secret', 'Vyj8Q~7ZlcxquDlwJIdRPam~Vemg-_KjANtQ9b.5'),
        type="password",
        help="Client Secret of the application",
        key="source_client_secret"
    )
    source_tenant_id = st.text_input(
        "Source Tenant ID",
        value=source_params.get('tenant_id', 'b7d09c10-00fe-43b5-a830-eb5571816fab'),
        help="Tenant ID of Azure Active Directory",
        key="source_tenant_id"
    )
    source_site_name = st.text_input(
        "Source Site Name",
        value=source_params.get('site_name', 'test'),
        help="SharePoint site name",
        key="source_site_name"
    )

with col2:
    st.subheader("Destination")
    dest_url = st.text_input(
        "Destination URL",
        value=f"https://{dest_params.get('tenant', 'jcardcorp')}.sharepoint.com",
        help="Please enter the destination SharePoint URL",
        key="dest_url"
    )
    dest_client_id = st.text_input(
        "Destination Client ID",
        value=dest_params.get('client_id', '67c1ea63-a3df-40fb-b75d-509ba93f1378'),
        help="Client ID of the application",
        key="dest_client_id"
    )
    dest_client_secret = st.text_input(
        "Destination Client Secret",
        value=dest_params.get('secret', 'g948Q~KPldCRrWsBnvfZrBKFGMpv8.awayfe7bIE'),
        type="password",
        help="Client Secret of the application",
        key="dest_client_secret"
    )
    dest_tenant_id = st.text_input(
        "Destination Tenant ID",
        value=dest_params.get('tenant_id', '16c61e00-bb32-451c-9d79-b3b0dc95bb17'),
        help="Tenant ID of Azure Active Directory",
        key="dest_tenant_id"
    )
    dest_site_name = st.text_input(
        "Destination Site Name",
        value=dest_params.get('site_name', 'test1'),
        help="SharePoint site name",
        key="dest_site_name"
    )

st.markdown("---")
st.subheader("Operations")

# Initialize SharePoint managers when credentials are provided
if all([source_client_id, source_client_secret, source_tenant_id, source_site_name]):
    # Extract tenant name from URL
    source_tenant_name = source_url.split('//')[1].split('.sharepoint.com')[0]
    st.session_state.source_manager = SharePointManager(
        client_id=source_client_id,
        client_secret=source_client_secret,
        tenant_id=source_tenant_id,
        site_name=source_site_name,
        tenant_name=source_tenant_name
    )

if all([dest_client_id, dest_client_secret, dest_tenant_id, dest_site_name]):
    # Extract tenant name from URL
    dest_tenant_name = dest_url.split('//')[1].split('.sharepoint.com')[0]
    st.session_state.dest_manager = SharePointManager(
        client_id=dest_client_id,
        client_secret=dest_client_secret,
        tenant_id=dest_tenant_id,
        site_name=dest_site_name,
        tenant_name=dest_tenant_name
    )

# Button section
col3, col4 = st.columns(2)
with col3:
    if st.button("1. Get file info and tag content", use_container_width=True):
        # Initialize SharePoint manager
        if not st.session_state.source_manager:
            source_tenant_name = source_url.split('//')[1].split('.sharepoint.com')[0]
            st.session_state.source_manager = SharePointManager(
                client_id=source_client_id,
                client_secret=source_client_secret,
                tenant_id=source_tenant_id,
                site_name=source_site_name,
                tenant_name=source_tenant_name
            )

        if not st.session_state.source_manager:
            st.error("Please complete all source authentication information first")
            st.stop()

        # Get access token
        access_token = st.session_state.source_manager.get_access_token()
        if not access_token:
            st.error("Failed to get access token")
            st.stop()
        
        # Get drive_id
        drive_id = st.session_state.source_manager.get_drive_id()
        if not drive_id:
            st.error("Failed to get drive ID")
            st.stop()

        with st.spinner("Retrieving file information..."):
            try:
                # Step 1: Get file information
                st.info("Step 1/2: Getting file information...")
                
                # Create progress indicator
                progress_placeholder = st.empty()
                status_text = st.empty()
                
                # Directly call processing function
                stats = read_and_tag_documents_mt(
                    access_token=access_token, 
                    drive_id=drive_id, 
                    max_workers=10,
                    read_json_file="sharepoint_fields.json"
                )
                
                # Show processing result
                st.success("✅ File processing completed!")
                
                # Show statistics
                st.write("### Processing Statistics")
                st.write(f"Total items: {stats['total_items']}")
                st.write(f"Files: {stats['files']}")
                st.write(f"Folders: {stats['folders']}")
                st.write(f"Successfully processed: {stats['success']}")
                st.write(f"Files downloaded: {stats['downloaded']}")
                st.write(f"Files skipped (already exist): {stats.get('skipped', 0)}")
                st.write(f"Processing errors: {stats['failed']}")
                
                # If processing is successful, show result file link
                if os.path.exists('tag_result/merged_sharepoint_data.json'):
                    st.info("Results saved to tag_result/merged_sharepoint_data.json")
                
            except Exception as e:
                st.error(f"Error occurred during processing: {str(e)}")
                logger.error(f"Processing error: {str(e)}", exc_info=True)

    if st.button("2. Auto classify documents", use_container_width=True):
        if not st.session_state.source_manager:
            st.error("Please complete all source authentication information first")
            st.stop()

        if st.session_state.source_manager.get_access_token():
            with st.spinner("Classifying files..."):
                try:
                    # Check necessary files exist
                    if not os.path.exists('tag_result/merged_sharepoint_data.json'):
                        st.error("Please run 'Read and tag file content' first")
                        st.stop()

                    if not os.path.exists('config/classification_rules.json'):
                        st.error("Classification rules file not found")
                        st.stop()

                    # Initialize classifier
                    classifier = AutoDocumentClassifier(
                        rules_file='config/classification_rules.json',
                        merged_data_file='tag_result/merged_sharepoint_data.json'
                    )

                    # Ensure directory exists
                    os.makedirs('temp', exist_ok=True)
                    os.makedirs('target', exist_ok=True)

                    # Execute classification - change process_files to auto_classify
                    stats = classifier.auto_classify('temp', 'target')

                    # Display classification results
                    st.success("✅ File classification completed!")

                    # Display statistics
                    st.write("Classification statistics:")
                    st.write(f"- Total files: {stats['total']}")
                    st.write(f"- Successfully processed: {stats['success']}")
                    st.write(f"- Failed to process: {stats['failed']}")
                    st.write(f"- Missing files: {stats['missing']}")

                    st.write("\nCategory statistics:")
                    for category, count in stats['categories'].items():
                        st.write(f"- {category or 'Unclassified'}: {count} files")

                    # If there are missing files, display list
                    if stats.get('missing_files'):
                        st.write("\nMissing files list:")
                        for file_path in stats['missing_files']:
                            st.write(f"- {file_path}")

                except Exception as e:
                    st.error(f"Error occurred during file classification: {str(e)}")
                    st.exception(e)
        else:
            st.error("Failed to get access token, please check authentication information")

    def upload_target_to_sharepoint(resume_upload=False, log_file=None):
        """Upload the target folder structure and files to SharePoint, support breakpoint resume with log_file"""
        # Define fields to exclude from metadata payload
        METADATA_EXCLUSION_FIELDS = {
            # Common SharePoint read-only or system-managed fields
            'id', 'name', 'path', 'webUrl', 'web_url', 'createdDateTime', 'created_time', 
            'lastModifiedDateTime', 'modified_time', 'size', 'parentReference', 
            'contentType', 'ContentTag', '@odata.etag', 'eTag', 'createdBy', 'lastModifiedBy', 
            'fileSystemInfo', 'folder', 'file', 'package', 'shared', 'sharing', 
            'specialFolder', 'cTag', 'root', 'remoteItem', 'children', 'activities', 
            'analytics', 'permissions', 'subscriptions', 'versions', 'thumbnails', 
            'listItem', 'driveItem', 'searchResult', 'publication', 'bundle',
            # Fields that might be in source but not directly updatable or relevant for new item
            'AuthorLookupId', 'EditorLookupId', 'DocIcon', 'LinkFilenameNoMenu', 
            'LinkFilename', 'ItemChildCount', 'FolderChildCount', 'AppAuthorLookupId', 
            'AppEditorLookupId', '_ComplianceFlags', '_ComplianceTag', '_ComplianceTagWrittenTime',
            '_ComplianceTagUserId', '_DisplayName', '_IsCurrentVersion', '_Level', '_ModerationComments',
            '_ModerationStatus', '_UIVersion', '_UIVersionString', 'odata.context', 
            'MediaServiceImageTags', 'ParentVersionStringLookupId', 'ParentLeafNameLookupId',
            'BSN', 'FileDirRef', 'FileLeafRef', 'FileRef', 'FSObjType', 'GUID', 'InstanceID',
            'MetaInfo', 'Order', 'ScopeId', 'SortBehavior', 'UniqueId', 'WorkflowInstanceID',
            'WorkflowVersion', 'Modified_x0020_By', 'Created_x0020_By', 'CopySource',
            'CheckoutUserLookupId', 'IsCheckedoutToLocal', 'LinkTitle', 'PrincipalCount',
            'MediaServiceFastMetadata', 'MediaServiceGenerationTime',
            'MediaServiceOCR', 'VirusStatus', '_EditMenuTableEnd', '_EditMenuTableStart',
            # Fields from the provided fiter_list that should be excluded
            'type', 'child_count', 'hash', 'download_url', 'FileSizeDisplay',
            # Fields that are often problematic or managed by SharePoint
            'ContentTypeId', 'GUID', 'Created', 'Modified', 'Author', 'Editor',
            'Attachments', 'CheckedOutTitle', 'CheckoutUserId', 'File_x0020_Type',
            'HTML_x0020_File_x0020_Type', 'TemplateUrl', 'xd_ProgID', 'xd_Signature',
            # Specific to this application's logic, if any, that should not be set as metadata
            'normalized_path', # This is used for lookup, not a SP field
        }
        try:
            if not st.session_state.dest_manager or not st.session_state.dest_manager.get_access_token():
                st.error("Please complete all target authentication information first")
                return False

            # Check target directory exists
            if not os.path.exists('target'):
                st.error("Target directory not found, please run classification first")
                return False

            with st.spinner("📤 Uploading files to SharePoint..."):
                # Get drive ID
                drive_id = st.session_state.dest_manager.get_drive_id()
                if not drive_id:
                    st.error("Failed to get target drive ID")
                    return False

                access_token = st.session_state.dest_manager.access_token

                # Initialize upload logger, including site name
                site_name = st.session_state.dest_manager.site_name
                upload_logger = SharePointUploadLogger(site_name=site_name)

                # Step 1: Ensure all necessary fields exist (maintain original logic)
                try:
                    # Get site ID
                    site_url = f"https://graph.microsoft.com/v1.0/sites/{st.session_state.dest_manager.tenant_name}.sharepoint.com:/sites/{st.session_state.dest_manager.site_name}"
                    site_res = requests.get(site_url, headers={"Authorization": f"Bearer {access_token}"})
                    if site_res.status_code != 200:
                        st.error(f"Failed to get site information: {site_res.text}")
                        return False

                    site_id = site_res.json()["id"]

                    # Get document library ID
                    list_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists"
                    list_res = requests.get(list_url, headers={"Authorization": f"Bearer {access_token}"})
                    if list_res.status_code != 200:
                        st.error(f"Failed to get document library list: {list_res.text}")
                        return False

                    # Find document library
                    doc_lib = next((l for l in list_res.json()["value"] if l["name"] == "Documents" or l["name"] == "Shared Documents"), None)
                    if not doc_lib:
                        st.error("Document library not found")
                        return False

                    list_id = doc_lib["id"]

                    # Extract all possible tags from merged_sharepoint_data.json
                    all_fields = {}
                    if os.path.exists('tag_result/merged_sharepoint_data.json'):
                        try:
                            with open('tag_result/merged_sharepoint_data.json', 'r', encoding='utf-8') as f:
                                merged_data = json.load(f)

                            # Collect all fields from all files
                            for item in merged_data:
                                if item.get('type') == 'file':
                                    try:
                                        # Extract other useful fields - use safe method to get index
                                        keys = list(item.keys())

                                        # Safe get index, if not found use default value
                                        start_index = keys.index('@odata.etag') if '@odata.etag' in keys else 0
                                        end_index = keys.index('ContentType') if 'ContentType' in keys else len(keys)

                                        # Get key-value pairs in specified range
                                        filter_data = {key: item[key] for key in keys[start_index+1:end_index]}
                                        try:
                                             filter_data['content_tag'] = item.get('content_tag', '')
                                        except:
                                            pass
                                        # Ensure filter_list is defined
                                        filter_list = getattr(globals(), 'filter_list', [])
                                        for key, value in filter_data.items():
                                            if key not in filter_list and value:
                                                all_fields[key] = value
                                    except Exception as e:
                                        logger.error(f"❌ Error processing item fields: {str(e)}")
                                        continue
                        except Exception as e:
                            logger.error(f"❌ Error reading/processing merged_sharepoint_data.json: {str(e)}")

                    column_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/columns"
                    # Fields to create
                    fields_to_create = [field for field in all_fields.keys() if field and field.strip()]  # Ensure field names are not empty

                    if fields_to_create:
                        logger.info(f"Need to create {len(fields_to_create)} new fields")

                        # Batch create fields
                        created_count = 0
                        failed_count = 0

                        for field_name in fields_to_create:
                            try:
                                # Ensure field name is not empty
                                if not field_name or not field_name.strip():
                                    logger.warning("⚠️ Skip empty field name")
                                    continue

                                display_name = field_name

                                create_payload = {
                                    "name": field_name,
                                    "displayName": display_name,
                                    "text": {}
                                }

                                # Record full request content for debugging
                                logger.info(f"Creating new field: {field_name}, Request content: {json.dumps(create_payload)}")

                                # Use explicit JSON serialization
                                create_res = requests.post(
                                    column_url,
                                    headers={
                                        "Authorization": f"Bearer {access_token}",
                                        "Content-Type": "application/json"
                                    },
                                    json=create_payload
                                )

                                if create_res.status_code == 201:
                                    logger.info(f"✅ Field {field_name} created successfully!")
                                    created_count += 1
                                else:
                                    logger.warning(f"⚠️ Field {field_name} creation failed: {create_res.status_code} - {create_res.text}")
                                    failed_count += 1
                            except Exception as e:
                                logger.error(f"❌ Error creating field {field_name}: {str(e)}")
                                failed_count += 1

                        logger.info(f"Field creation result: Success {created_count}, Failed {failed_count}")

                        # If new fields were created, wait for SharePoint to process
                        if created_count > 0:
                            wait_time = min(5 + created_count, 10)  # Adjust wait time based on number of fields, up to 20 seconds
                            with st.spinner(f"Waiting for SharePoint to process new fields ({wait_time} seconds)..."):
                                time.sleep(wait_time)
                    else:
                        logger.info("All necessary fields already exist, no need to create")

                except Exception as e:
                    logger.error(f"Error checking and creating fields: {str(e)}")
                    # Continue execution, do not interrupt upload process

                # Step 2: Optimized version of uploading files and writing tags
                # Preload tag data to avoid repeated reading
                # Step 1: Identify Tag Data (Loading merged_sharepoint_data.json and populating file_metadata_mapping)
                file_metadata_mapping: Dict[str, Dict[Any, Any]] = {}
                merged_data_path = "tag_result/merged_sharepoint_data.json"
                logger.info(f"Attempting to load tag data from {merged_data_path}")
                if os.path.exists(merged_data_path):
                    try:
                        with open(merged_data_path, 'r', encoding='utf-8') as f:
                            merged_data = json.load(f)
                        logger.info(f"✅ Successfully loaded {merged_data_path}, containing {len(merged_data)} items.")

                        for item in merged_data:
                            if item.get('type') == 'file':
                                filename = item.get("name")
                                if not filename:
                                    logger.warning(f"⚠️ Item found with no name, skipping: {item}")
                                    continue

                                if filename in file_metadata_mapping:
                                    logger.warning(f"⚠️ Duplicate filename found: '{filename}'. Overwriting with new item. Old: {file_metadata_mapping[filename]}, New: {item}")
                                file_metadata_mapping[filename] = item
                            # else: # Optionally log skipped non-file items
                                # logger.debug(f"Skipping non-file item: {item.get('name')}")
                        
                        logger.info(f"✅ Populated file_metadata_mapping with {len(file_metadata_mapping)} file entries.")

                    except json.JSONDecodeError as e:
                        logger.error(f"❌ Error decoding JSON from {merged_data_path}: {str(e)}")
                    except Exception as e:
                        logger.error(f"❌ An unexpected error occurred while loading or processing {merged_data_path}: {str(e)}")
                else:
                    logger.error(f"❌ Tag data file not found: {merged_data_path}. Cannot populate file_metadata_mapping.")

                # Collect all files to upload
                all_files = []
                created_folders = set()  # Used to track created folders

                # Recursively collect all files
                for root, dirs, files in os.walk('target'):
                    # Calculate relative path
                    rel_path = os.path.relpath(root, 'target') if root != 'target' else ""
                    sp_folder_path = rel_path.replace('\\', '/')

                    # Add folder path (for creation)
                    if rel_path and rel_path != '.':
                        created_folders.add(sp_folder_path)

                    # Add files
                    for file in files:
                        local_path = os.path.join(root, file)
                        sp_file_path = os.path.join(sp_folder_path, file).replace('\\', '/')
                        all_files.append((local_path, sp_file_path))

                # Handle resume upload
                if resume_upload and log_file:
                    try:
                        # Load specified log file
                        if upload_logger.load_log(log_file):
                            # Get successfully uploaded files
                            successfully_uploaded = upload_logger.get_successful_files()

                            # Update statistics
                            files_uploaded = len(successfully_uploaded)

                            # Display resume information
                            st.info(f"Resuming upload task, {files_uploaded} files uploaded, remaining {len(all_files) - files_uploaded} files")

                            # Filter out already uploaded files
                            all_files = upload_logger.filter_files_to_upload(all_files)
                        else:
                            st.warning("Unable to load specified log file, will create new log")
                            upload_logger.create_log(all_files)
                    except Exception as e:
                        st.warning(f"Error processing resume: {str(e)}, will create new log")
                        upload_logger.create_log(all_files)
                else:
                    # Create new log file
                    upload_logger.create_log(all_files)

                # Define thread-safe counter
                from threading import Lock
                counter_lock = Lock()

                # Get log statistics
                log_stats = upload_logger.get_statistics()

                stats = {
                    'processed': 0,
                    'success': 0,
                    'error': 0,
                    'total': len(all_files),
                    'skipped': log_stats['success']
                }

                # Start periodic log saving
                upload_logger.start_periodic_save(30)

                # 在upload_target_to_sharepoint函数内，替换upload_single_file函数定义
                # 添加全局变量用于metadata延迟处理
                metadata_update_queue = []
                metadata_queue_lock = threading.Lock()
                file_upload_locks = {}  # 用于防止同名文件并发上传
                upload_locks_mutex = threading.Lock()  # 保护file_upload_locks字典的锁

                # Define function to upload single file (with enhanced error handling and delayed metadata)
                def upload_single_file(file_tuple, file_metadata_map, expected_fields_keys, sp_manager, max_retries=3):
                    local_path, sp_path = file_tuple
                    file_id = None
                    
                    # 获取文件名用于并发控制
                    filename = os.path.basename(sp_path)
                    
                    # 实现文件级别的上传锁，防止同名文件并发冲突
                    with upload_locks_mutex:
                        if filename not in file_upload_locks:
                            file_upload_locks[filename] = threading.Lock()
                        file_lock = file_upload_locks[filename]
                    
                    # 使用文件级锁确保同名文件不会并发上传
                    with file_lock:
                        try:
                            # 1. 确保文件夹存在
                            folder_path = os.path.dirname(sp_path)
                            if folder_path and folder_path not in created_folders:
                                try:
                                    folder_id = create_folder_safe(folder_path, drive_id, access_token)
                                    if folder_id:
                                        created_folders.add(folder_path)
                                        logger.info(f"✅ Folder created: {folder_path}")
                                    else:
                                        raise Exception(f"Failed to create folder: {folder_path}")
                                except Exception as e:
                                    logger.error(f"❌ Folder creation failed {folder_path}: {str(e)}")
                                    upload_logger.update_file_status(
                                        local_path,
                                        sp_path,
                                        "error",
                                        error=f"Folder creation failed: {str(e)}"
                                    )
                                    with counter_lock:
                                        stats['processed'] += 1
                                        stats['error'] += 1
                                    return False

                            # 2. 获取文件大小
                            try:
                                file_size = os.path.getsize(local_path)
                            except Exception as e:
                                logger.error(f"❌ Cannot get file size for {local_path}: {str(e)}")
                                upload_logger.update_file_status(local_path, sp_path, "error", error=f"File size error: {str(e)}")
                                return False
                            
                            # 3. 根据文件大小选择上传方式
                            if file_size < 4 * 1024 * 1024:
                                file_id = upload_small_file(local_path, sp_path, max_retries)
                            else:
                                file_id = upload_large_file(local_path, sp_path, file_size, max_retries)

                            # 4. 如果上传成功，将metadata任务加入延迟队列而不是立即处理
                            if file_id:
                                source_item_data = file_metadata_map.get(filename)
                                
                                if source_item_data:
                                    # 预处理metadata payload
                                    final_metadata_payload = {}
                                    for key, value in source_item_data.items():
                                        if (key in expected_fields_keys or key == 'content_tag') and \
                                           key not in METADATA_EXCLUSION_FIELDS and value is not None:
                                            processed_key = str(key).replace(" ", "").replace("-", "_").replace(".", "_")
                                            final_metadata_payload[processed_key] = value
                                    
                                    if final_metadata_payload:
                                        # 将metadata更新任务添加到延迟队列
                                        metadata_task = {
                                            'file_id': file_id,
                                            'filename': filename,
                                            'local_path': local_path,
                                            'sp_path': sp_path,
                                            'metadata_payload': final_metadata_payload,
                                            'upload_timestamp': time.time()
                                        }
                                        
                                        with metadata_queue_lock:
                                            metadata_update_queue.append(metadata_task)
                                        
                                        logger.info(f"📝 Metadata task queued for delayed processing: {filename} (ID: {file_id})")
                                    else:
                                        logger.info(f"No applicable metadata to update for {filename} (ID: {file_id})")
                                else:
                                    logger.warning(f"⚠️ No source metadata found for {filename} (ID: {file_id})")
                            else:
                                logger.error(f"❌ Upload failed for {local_path}, cannot attempt metadata update.")
                            
                        except Exception as e:
                            logger.error(f"❌ Unexpected error in upload_single_file: {str(e)}")
                            upload_logger.update_file_status(local_path, sp_path, "error", error=f"Unexpected error: {str(e)}")
                            return False
                    
                    return file_id is not None

                def create_folder_safe(folder_path, drive_id, access_token, max_retries=3):
                    """线程安全的文件夹创建函数"""
                    import urllib.parse
                    
                    for attempt in range(max_retries):
                        try:
                            # URL编码处理
                            encoded_path = urllib.parse.quote(folder_path, safe='/')
                            check_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{encoded_path}"
                            headers = {"Authorization": f"Bearer {access_token}"}
                            
                            # 检查文件夹是否已存在
                            response = requests.get(check_url, headers=headers, timeout=30)
                            if response.status_code == 200:
                                return response.json().get('id')
                            
                            # 创建文件夹
                            folder_name = os.path.basename(folder_path)
                            parent_path = os.path.dirname(folder_path)
                            
                            if parent_path:
                                encoded_parent = urllib.parse.quote(parent_path, safe='/')
                                create_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{encoded_parent}:/children"
                            else:
                                create_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root/children"
                            
                            folder_data = {
                                "name": folder_name,
                                "folder": {},
                                "@microsoft.graph.conflictBehavior": "replace"
                            }
                            
                            create_response = requests.post(
                                create_url,
                                headers={
                                    "Authorization": f"Bearer {access_token}",
                                    "Content-Type": "application/json"
                                },
                                json=folder_data,
                                timeout=30
                            )
                            
                            if create_response.status_code in [201, 200]:
                                return create_response.json().get('id')
                            elif create_response.status_code == 409:
                                # 文件夹已存在，再次尝试获取
                                response = requests.get(check_url, headers=headers, timeout=30)
                                if response.status_code == 200:
                                    return response.json().get('id')
                            
                            if attempt < max_retries - 1:
                                wait_time = (attempt + 1) * 2
                                logger.warning(f"⚠️ Folder creation retry {attempt+1}, waiting {wait_time}s")
                                time.sleep(wait_time)
                            else:
                                logger.error(f"❌ Folder creation failed: {create_response.status_code} - {create_response.text}")
                                return None
                                
                        except Exception as e:
                            if attempt < max_retries - 1:
                                wait_time = (attempt + 1) * 2
                                logger.warning(f"⚠️ Folder creation error, retry {attempt+1}: {str(e)}")
                                time.sleep(wait_time)
                            else:
                                logger.error(f"❌ Folder creation exception: {str(e)}")
                                return None
                    
                    return None

                def upload_small_file(local_path, sp_path, max_retries=3) -> Optional[str]:
                    """上传小文件（<4MB），增强URL编码和冲突处理"""
                    import urllib.parse
                    
                    # 1. 检查文件是否已存在
                    try:
                        encoded_sp_path = urllib.parse.quote(sp_path, safe='/')
                        check_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{encoded_sp_path}"
                        check_headers = {"Authorization": f"Bearer {access_token}"}
                        
                        check_response = requests.get(check_url, headers=check_headers, timeout=30)
                        if check_response.status_code == 200:
                            existing_file = check_response.json()
                            existing_size = existing_file.get('size', 0)
                            existing_id = existing_file.get('id')
                            current_size = os.path.getsize(local_path)
                            
                            # 如果文件已存在且大小匹配，直接返回existing file_id
                            if existing_size == current_size and existing_id:
                                logger.info(f"📄 File already exists with same size, using existing: {os.path.basename(local_path)} (ID: {existing_id})")
                                upload_logger.update_file_status(
                                    local_path,
                                    sp_path,
                                    "success",
                                    file_id=existing_id
                                )
                                return existing_id
                            else:
                                logger.info(f"📄 File exists but size differs (existing: {existing_size}, new: {current_size}), will replace")
                    except Exception as e:
                        logger.info(f"ℹ️ File check failed (will proceed with upload): {str(e)}")
                    
                    # 2. 执行上传，带重试和冲突处理
                    for attempt in range(max_retries):
                        try:
                            url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{encoded_sp_path}:/content"
                            
                            headers = {
                                "Authorization": f"Bearer {access_token}",
                                "Content-Type": "application/octet-stream"
                            }
                            
                            # 动态超时
                            timeout = min(120, 45 * (attempt + 1))
                            
                            with open(local_path, "rb") as f:
                                res = requests.put(url, headers=headers, data=f, timeout=timeout)
                            
                            if res.status_code in [200, 201, 202]:
                                response_data = res.json()
                                file_id = response_data.get('id')
                                if file_id:
                                    upload_logger.update_file_status(
                                        local_path,
                                        sp_path,
                                        "success",
                                        file_id=file_id
                                    )
                                    logger.info(f"✅ File uploaded: {os.path.basename(local_path)} (ID: {file_id})")
                                    return file_id
                                else:
                                    logger.warning(f"⚠️ Upload successful but no file_id in response")
                            
                            # 处理冲突情况
                            elif res.status_code == 409:
                                logger.info(f"ℹ️ File conflict detected, checking if completed by another process: {sp_path}")
                                try:
                                    # 等待一段时间让其他进程完成
                                    time.sleep((attempt + 1) * 3)
                                    
                                    # 检查文件是否已被完成上传
                                    final_check = requests.get(check_url, headers=check_headers, timeout=30)
                                    if final_check.status_code == 200:
                                        final_file = final_check.json()
                                        final_size = final_file.get('size', 0)
                                        final_id = final_file.get('id')
                                        current_size = os.path.getsize(local_path)
                                        
                                        if final_size == current_size and final_id:
                                            logger.info(f"✅ File completed by another process: {os.path.basename(local_path)} (ID: {final_id})")
                                            upload_logger.update_file_status(local_path, sp_path, "success", file_id=final_id)
                                            return final_id
                                except Exception as e:
                                    logger.warning(f"⚠️ Failed to check completed file: {str(e)}")
                            
                            if attempt < max_retries - 1:
                                wait_time = (2 ** attempt) + random.uniform(1, 3)
                                logger.warning(f"⚠️ Upload retry ({attempt+1}/{max_retries}) for {os.path.basename(local_path)}: {res.status_code}")
                                time.sleep(wait_time)
                            else:
                                error_message = f"Upload failed: {res.status_code} - {res.text[:300]}"
                                logger.error(f"❌ {error_message}")
                                upload_logger.update_file_status(local_path, sp_path, "error", error=error_message)
                                
                        except Exception as e:
                            if attempt < max_retries - 1:
                                wait_time = (2 ** attempt) + random.uniform(1, 3)
                                logger.warning(f"⚠️ Upload error, retry ({attempt+1}/{max_retries}): {str(e)}")
                                time.sleep(wait_time)
                            else:
                                error_message = f"Upload failed: {str(e)}"
                                logger.error(f"❌ {error_message}")
                                upload_logger.update_file_status(local_path, sp_path, "error", error=error_message)
                    
                    return None

                def upload_large_file(local_path, sp_path, file_size, max_retries=3):
                    """分块上传大文件（>=4MB），增加冲突检测和处理"""
                    import urllib.parse
                    
                    # 1. 检查文件是否已存在
                    try:
                        encoded_sp_path = urllib.parse.quote(sp_path, safe='/')
                        check_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{encoded_sp_path}"
                        check_headers = {"Authorization": f"Bearer {access_token}"}
                        
                        check_response = requests.get(check_url, headers=check_headers, timeout=30)
                        if check_response.status_code == 200:
                            existing_file = check_response.json()
                            existing_size = existing_file.get('size', 0)
                            existing_id = existing_file.get('id')
                            
                            # 如果文件已存在且大小匹配，直接返回existing file_id
                            if existing_size == file_size and existing_id:
                                logger.info(f"📄 File already exists with same size, using existing: {os.path.basename(local_path)} (ID: {existing_id})")
                                upload_logger.update_file_status(
                                    local_path,
                                    sp_path,
                                    "success",
                                    file_id=existing_id
                                )
                                return existing_id
                            else:
                                logger.info(f"📄 File exists but size differs (existing: {existing_size}, new: {file_size}), will replace")
                    except Exception as e:
                        logger.info(f"ℹ️ File check failed (will proceed with upload): {str(e)}")
                    
                    # 2. 创建上传会话，带重试和冲突处理
                    upload_session = None
                    for attempt in range(max_retries):
                        try:
                            create_session_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{encoded_sp_path}:/createUploadSession"
                            create_session_headers = {
                                "Authorization": f"Bearer {access_token}",
                                "Content-Type": "application/json"
                            }
                            create_session_body = {
                                "@microsoft.graph.conflictBehavior": "replace",
                                "description": f"Large file upload: {os.path.basename(local_path)}",
                                "name": os.path.basename(sp_path)
                            }
                            
                            create_session_res = requests.post(
                                create_session_url,
                                headers=create_session_headers,
                                json=create_session_body,
                                timeout=60
                            )
                            
                            if create_session_res.status_code == 200:
                                upload_session = create_session_res.json()
                                logger.info(f"✅ Upload session created for {os.path.basename(local_path)}")
                                break
                            elif create_session_res.status_code == 409:
                                # 处理"nameAlreadyExists"错误
                                error_info = create_session_res.json().get('error', {})
                                if 'nameAlreadyExists' in error_info.get('code', ''):
                                    logger.warning(f"⚠️ File currently being uploaded by another process, waiting...")
                                    
                                    # 等待其他进程完成上传
                                    wait_time = (attempt + 1) * 10  # 递增等待时间
                                    time.sleep(wait_time)
                                    
                                    # 检查文件是否已被其他进程上传完成
                                    try:
                                        final_check = requests.get(check_url, headers=check_headers, timeout=30)
                                        if final_check.status_code == 200:
                                            final_file = final_check.json()
                                            final_size = final_file.get('size', 0)
                                            final_id = final_file.get('id')
                                            
                                            if final_size == file_size and final_id:
                                                logger.info(f"✅ File completed by another process: {os.path.basename(local_path)} (ID: {final_id})")
                                                upload_logger.update_file_status(
                                                    local_path,
                                                    sp_path,
                                                    "success",
                                                    file_id=final_id
                                                )
                                                return final_id
                                    except Exception:
                                        pass
                                        
                                else:
                                    logger.warning(f"⚠️ Conflict error (attempt {attempt+1}): {create_session_res.text}")
                            elif attempt < max_retries - 1:
                                wait_time = (2 ** attempt) + random.uniform(1, 3)
                                logger.warning(f"⚠️ Create session retry ({attempt+1}/{max_retries}): {create_session_res.status_code}")
                                time.sleep(wait_time)
                            else:
                                error_message = f"Create upload session failed: {create_session_res.status_code} - {create_session_res.text}"
                                logger.error(f"❌ {error_message}")
                                upload_logger.update_file_status(
                                    local_path,
                                    sp_path,
                                    "error",
                                    error=error_message
                                )
                                return None
                        except Exception as e:
                            if attempt < max_retries - 1:
                                wait_time = (2 ** attempt) + random.uniform(1, 3)
                                logger.warning(f"⚠️ Create session error, retry ({attempt+1}/{max_retries}): {str(e)}")
                                time.sleep(wait_time)
                            else:
                                error_message = f"Create upload session failed: {str(e)}"
                                logger.error(f"❌ {error_message}")
                                upload_logger.update_file_status(
                                    local_path,
                                    sp_path,
                                    "error",
                                    error=error_message
                                )
                                return None

                    if not upload_session:
                        return None

                    # 3. 分块上传（保持原有逻辑）
                    upload_url = upload_session['uploadUrl']
                    
                    # 根据文件大小动态调整分块大小
                    if file_size > 100 * 1024 * 1024:  # 大于100MB
                        chunk_size = 10 * 1024 * 1024  # 10MB chunks
                    else:
                        chunk_size = 4 * 1024 * 1024  # 4MB chunks
                    
                    total_chunks = (file_size + chunk_size - 1) // chunk_size
                    uploaded_bytes = 0
                    start_time = time.time()
                    
                    with open(local_path, 'rb') as f:
                        for chunk_index in range(total_chunks):
                            start_byte = chunk_index * chunk_size
                            end_byte = min(start_byte + chunk_size, file_size)
                            chunk_size_actual = end_byte - start_byte
                            
                            # 读取当前块
                            chunk_data = f.read(chunk_size_actual)
                            
                            # 上传当前块
                            for attempt in range(max_retries):
                                try:
                                    headers = {
                                        "Content-Length": str(chunk_size_actual),
                                        "Content-Range": f"bytes {start_byte}-{end_byte-1}/{file_size}"
                                    }
                                    
                                    upload_res = requests.put(
                                        upload_url,
                                        headers=headers,
                                        data=chunk_data,
                                        timeout=120
                                    )
                                    
                                    if upload_res.status_code in [201, 202]:
                                        # 上传完成
                                        if upload_res.status_code == 201:
                                            file_id = upload_res.json().get('id')
                                            logger.info(f"✅ Large file uploaded successfully: {os.path.basename(local_path)} (ID: {file_id})")
                                            upload_logger.update_file_status(
                                                local_path,
                                                sp_path,
                                                "success",
                                                file_id=file_id
                                            )
                                            return file_id
                                        else:
                                            # 继续下一个块
                                            break
                                    elif upload_res.status_code == 404:
                                        # 会话过期，需要重新创建
                                        logger.warning("Upload session expired, retrying...")
                                        return upload_large_file(local_path, sp_path, file_size, max_retries)
                                    elif upload_res.status_code == 416:
                                        # 范围错误，可能是块已上传，继续下一个块
                                        logger.warning(f"Chunk {chunk_index + 1}/{total_chunks} already uploaded, continuing...")
                                        break
                                    elif attempt < max_retries - 1:
                                        wait_time = (2 ** attempt) + random.uniform(1, 3)
                                        logger.warning(f"⚠️ Chunk upload retry ({attempt+1}/{max_retries}): {upload_res.status_code}")
                                        time.sleep(wait_time)
                                    else:
                                        error_message = f"Chunk upload failed: {upload_res.status_code} - {upload_res.text}"
                                        logger.error(f"❌ {error_message}")
                                        upload_logger.update_file_status(
                                            local_path,
                                            sp_path,
                                            "error",
                                            error=error_message
                                        )
                                        return None
                                except Exception as e:
                                    if attempt < max_retries - 1:
                                        wait_time = (2 ** attempt) + random.uniform(1, 3)
                                        logger.warning(f"⚠️ Chunk upload error, retry ({attempt+1}/{max_retries}): {str(e)}")
                                        time.sleep(wait_time)
                                    else:
                                        error_message = f"Chunk upload failed: {str(e)}"
                                        logger.error(f"❌ {error_message}")
                                        upload_logger.update_file_status(
                                            local_path,
                                            sp_path,
                                            "error",
                                            error=error_message
                                        )
                                        return None
                            
                            # 更新上传进度
                            uploaded_bytes += chunk_size_actual
                            elapsed_time = time.time() - start_time
                            speed = uploaded_bytes / elapsed_time if elapsed_time > 0 else 0
                            
                            # 计算进度百分比
                            progress = (uploaded_bytes / file_size) * 100
                            
                            # 格式化速度和大小显示
                            speed_str = f"{speed/1024/1024:.1f} MB/s"
                            uploaded_str = f"{uploaded_bytes/1024/1024:.1f} MB"
                            total_str = f"{file_size/1024/1024:.1f} MB"
                            
                            # 更新日志（降低频率以避免日志过多）
                            if chunk_index % 5 == 0 or chunk_index == total_chunks - 1:
                                logger.info(f"Uploading {os.path.basename(local_path)}: {progress:.1f}% ({uploaded_str}/{total_str}) - {speed_str}")
                    
                    return None

                # Display progress
                progress_bar = st.progress(0)
                status_text = st.empty()

                # Calculate total file count
                total_files = len(all_files)

                # Dynamically adjust thread count based on file count
                max_workers = min(5, max(2, total_files // 10))  # 減少並發數
                max_retries = 5  # 增加重試次數

                # Use thread pool to concurrently upload all files
                with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                    # Submit all tasks, passing necessary parameters to upload_single_file
                    # Ensure all_fields is defined in this scope; it's populated earlier when checking/creating fields.
                    # If all_fields might not be populated (e.g., if no new fields needed creating),
                    # initialize it to an empty dict or handle appropriately.
                    # For now, assuming all_fields is available and has keys() method.
                    # If fields_to_create was the source, ensure it's accessible or its keys are.
                    # Let's assume `all_fields` (from line 1242) is the correct variable holding expected field names.
                    # If `fields_to_create` was more appropriate, that should be used.
                    # Based on previous context, `all_fields` seems to be the dictionary of all potential fields.
                    
                    # Ensure `expected_keys_for_metadata` is correctly defined.
                    # `all_fields` is a dict where keys are field names.
                    expected_keys_for_metadata = set(all_fields.keys())


                    futures = [executor.submit(upload_single_file,
                                               file_tuple,
                                               file_metadata_mapping, # From Step 1
                                               expected_keys_for_metadata, # Derived from all_fields
                                               st.session_state.dest_manager # SharePointManager instance
                                               ) for file_tuple in all_files]

                    # Process completed tasks
                    for future in concurrent.futures.as_completed(futures):
                        upload_attempt_succeeded = False # Default to false for the current file processing in this iteration
                        try:
                            # upload_single_file returns True if file_id was obtained (upload succeeded), False otherwise.
                            upload_attempt_succeeded = future.result() 
                            
                            # Display progress
                            with counter_lock:
                                stats['processed'] += 1 # Increment processed files, regardless of this attempt's outcome
                                
                                # These stats are general counters for upload attempts.
                                # Detailed success/failure including metadata is logged elsewhere or by upload_logger.
                                if upload_attempt_succeeded:
                                    stats['success'] +=1 # Count as a successful upload attempt
                                else:
                                    stats['error'] +=1   # Count as a failed upload attempt

                                # Calculate progress as decimal (0-1)
                                if stats['total'] > 0:
                                    progress = stats['processed'] / stats['total']
                                    # Calculate percentage for display
                                    progress_percentage = progress * 100
                                    # Update progress bar with decimal value
                                    progress_bar.progress(progress)
                                    # Update status text with percentage
                                    status_text.text(f"Progress: {progress_percentage:.1f}% ({stats['processed']}/{stats['total']}) Success: {stats['success']}, Error: {stats['error']}")
                                else:
                                     status_text.text(f"No files to upload. Processed: {stats['processed']}/{stats['total']}")

                        except Exception as e:
                            logger.error(f"❌ Task execution failed in ThreadPool: {str(e)}")
                            with counter_lock: # Ensure stats are updated even if future.result() itself raises an unexpected error
                                stats['processed'] += 1
                                stats['error'] +=1
                                if stats['total'] > 0:
                                    progress = stats['processed'] / stats['total']
                                    progress_percentage = progress * 100
                                    progress_bar.progress(progress)
                                    status_text.text(f"Progress: {progress_percentage:.1f}% ({stats['processed']}/{stats['total']}) Success: {stats['success']}, Error: {stats['error']}")
                                else:
                                    status_text.text(f"Task error. Processed: {stats['processed']}/{stats['total']}")

                # Stop periodic log saving
                upload_logger.stop_periodic_save()

                # Final update and save log file
                upload_logger.save_log()

                # 添加延迟metadata处理
                def process_delayed_metadata_updates():
                    """处理延迟的metadata更新任务"""
                    if not metadata_update_queue:
                        logger.info("📝 No metadata tasks to process")
                        return 0, 0
                    
                    total_tasks = len(metadata_update_queue)
                    success_count = 0
                    
                    logger.info(f"📝 Processing {total_tasks} delayed metadata update tasks...")
                    
                    # 按上传时间排序，确保充分的等待时间
                    metadata_update_queue.sort(key=lambda x: x['upload_timestamp'])
                    
                    for i, task in enumerate(metadata_update_queue):
                        try:
                            file_id = task['file_id']
                            filename = task['filename']
                            metadata_payload = task['metadata_payload']
                            local_path = task['local_path']
                            sp_path = task['sp_path']
                            upload_time = task['upload_timestamp']
                            
                            # 确保每个文件至少等待30秒后再更新metadata
                            elapsed_time = time.time() - upload_time
                            min_wait_time = 30
                            additional_wait = max(0, min_wait_time - elapsed_time)
                            
                            if additional_wait > 0:
                                logger.info(f"⏳ [{i+1}/{total_tasks}] Waiting additional {additional_wait:.1f}s for indexing: {filename}")
                                time.sleep(additional_wait)
                            
                            # 使用增强的重试策略更新metadata
                            success = update_metadata_with_retry(
                                file_id, 
                                metadata_payload, 
                                filename,
                                drive_id,
                                access_token,
                                st.session_state.dest_manager
                            )
                            
                            if success:
                                success_count += 1
                                logger.info(f"✅ [{i+1}/{total_tasks}] Metadata updated: {filename}")
                                # 更新上传日志状态
                                upload_logger.update_file_status(
                                    local_path, 
                                    sp_path, 
                                    "success", 
                                    file_id=file_id
                                )
                            else:
                                logger.error(f"❌ [{i+1}/{total_tasks}] Metadata update failed: {filename}")
                                upload_logger.update_file_status(
                                    local_path, 
                                    sp_path, 
                                    "success_meta_error", 
                                    file_id=file_id, 
                                    error="Delayed metadata update failed after retries"
                                )
                                
                        except Exception as e:
                            logger.error(f"❌ Error processing metadata task {i+1}: {str(e)}")
                    
                    logger.info(f"📝 Metadata processing complete: {success_count}/{total_tasks} successful")
                    return success_count, total_tasks

                def update_metadata_with_retry(file_id, metadata_payload, filename, drive_id, access_token, sp_manager, max_attempts=5):
                    """增强的metadata更新重试策略"""
                    
                    # 首先验证文件的可用性
                    for verify_attempt in range(3):
                        try:
                            # 检查文件是否可通过Drive API访问
                            verify_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{file_id}"
                            verify_response = requests.get(
                                verify_url,
                                headers={"Authorization": f"Bearer {access_token}"},
                                timeout=30
                            )
                            
                            if verify_response.status_code == 200:
                                # 检查ListItem是否可用（这是metadata更新的关键）
                                listitem_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{file_id}/listItem"
                                listitem_response = requests.get(
                                    listitem_url,
                                    headers={"Authorization": f"Bearer {access_token}"},
                                    timeout=30
                                )
                                
                                if listitem_response.status_code == 200:
                                    logger.info(f"✅ File and ListItem verified for {filename}")
                                    break
                                else:
                                    logger.warning(f"⚠️ ListItem not ready for {filename}: {listitem_response.status_code}")
                                    if verify_attempt < 2:
                                        time.sleep(5)
                            else:
                                logger.warning(f"⚠️ File not accessible: {verify_response.status_code}")
                                if verify_attempt < 2:
                                    time.sleep(3)
                                
                        except Exception as e:
                            logger.warning(f"⚠️ Verification attempt {verify_attempt+1} failed: {str(e)}")
                            if verify_attempt < 2:
                                time.sleep(3)
                    
                    # 尝试多种metadata更新方法
                    for attempt in range(max_attempts):
                        # 方法1: 使用SharePointClient
                        try:
                            if sp_manager.update_file_metadata(file_id, metadata_payload):
                                logger.info(f"✅ [Method1] Metadata updated via SharePointClient: {filename}")
                                return True
                        except Exception as e:
                            logger.warning(f"⚠️ [Method1] SharePointClient failed: {str(e)}")
                        
                        # 方法2: 直接使用ListItem PATCH
                        try:
                            listitem_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{file_id}/listItem/fields"
                            response = requests.patch(
                                listitem_url,
                                headers={
                                    "Authorization": f"Bearer {access_token}",
                                    "Content-Type": "application/json"
                                },
                                json=metadata_payload,
                                timeout=60
                            )
                            
                            if response.status_code == 200:
                                logger.info(f"✅ [Method2] Metadata updated via ListItem PATCH: {filename}")
                                return True
                            else:
                                logger.warning(f"⚠️ [Method2] ListItem PATCH failed: {response.status_code}")
                        except Exception as e:
                            logger.warning(f"⚠️ [Method2] ListItem PATCH exception: {str(e)}")
                        
                        # 方法3: 使用替代fields端点
                        try:
                            fields_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{file_id}/fields"
                            response = requests.patch(
                                fields_url,
                                headers={
                                    "Authorization": f"Bearer {access_token}",
                                    "Content-Type": "application/json"
                                },
                                json=metadata_payload,
                                timeout=60
                            )
                            
                            if response.status_code == 200:
                                logger.info(f"✅ [Method3] Metadata updated via fields endpoint: {filename}")
                                return True
                            else:
                                logger.warning(f"⚠️ [Method3] Fields endpoint failed: {response.status_code}")
                        except Exception as e:
                            logger.warning(f"⚠️ [Method3] Fields endpoint exception: {str(e)}")
                        
                        # 如果所有方法都失败，等待后重试
                        if attempt < max_attempts - 1:
                            wait_time = min(15, (attempt + 1) * 5)
                            logger.info(f"⏳ All methods failed, waiting {wait_time}s before retry {attempt+2}/{max_attempts}")
                            time.sleep(wait_time)
                    
                    logger.error(f"❌ All metadata update attempts failed for {filename}")
                    return False

                # 执行延迟metadata处理
                if metadata_update_queue:
                    with st.spinner("📝 Processing delayed metadata updates..."):
                        meta_success, meta_total = process_delayed_metadata_updates()
                        
                        if meta_total > 0:
                            st.info(f"📝 Metadata processing completed: {meta_success}/{meta_total} successful")
                        
                        # 最终保存日志
                        upload_logger.save_log()

                # Clean up progress display
                progress_bar.empty()
                status_text.empty()

                # Display final results
                st.success("✅ File upload completed!")
                st.write(f"Total processed: {stats['total']} files")
                st.write(f"Successfully uploaded: {stats['success']} files")
                st.write(f"Upload errors: {stats['error']} files")
                if stats['skipped'] > 0:
                    st.write(f"Skipped (previously uploaded): {stats['skipped']} files")

                # Display log file information
                st.info(f"Upload log saved to: {upload_logger.log_filename}")

                # If there are failed files, provide retry option
                if stats['error'] > 0:
                    st.warning(f"There are {stats['error']} files that failed to upload. You can retry using the 'Resume Upload' feature later.")

                return True

        except Exception as e:
            st.error(f"Error occurred during upload: {str(e)}")
            logger.error(f"Upload failed: {str(e)}")
            return False

    # Add upload option
    st.subheader("3. Upload files to SharePoint")

    # Add resume upload option (default enabled)
    resume_upload = st.checkbox("Enable resume upload", value=True, help="If the upload is interrupted, you can resume from where it left off")

    selected_log_file = None
    if resume_upload:
        # Get available log files
        log_files = SharePointUploadLogger.get_available_logs()

        if not log_files:
            st.warning("No available upload log files found, a new log will be created")
        else:
            # Display log file selector
            log_options = [log["display_name"] for log in log_files]
            selected_index = st.selectbox("Select the task to resume", range(len(log_options)), format_func=lambda i: log_options[i])
            selected_log_file = log_files[selected_index]["filename"]

            # Display selected log details
            st.info(f"Selected: {log_options[selected_index]}")

            # Display detailed information about selected log file
            with st.expander("View log details"):
                # Create temporary logger to load log
                # Extract site name from log filename (if any)
                site_name = None
                filename = os.path.basename(selected_log_file)
                if filename.startswith("upload_log_") and "_20" in filename:
                    parts = filename.split("_")
                    if len(parts) > 3:  # Has site name
                        site_name = parts[2]

                temp_logger = SharePointUploadLogger(site_name=site_name)
                if temp_logger.load_log(selected_log_file):
                    # Get statistics
                    log_stats = temp_logger.get_statistics()

                    # Display statistics
                    st.write(f"Total files: {log_stats['total']}")
                    st.write(f"Successfully uploaded: {log_stats['success']}")
                    st.write(f"Upload errors: {log_stats['error']}")
                    st.write(f"Pending: {log_stats['pending']}")

                    # Display start time and last update time
                    start_time = log_stats.get("start_time", "Unknown")
                    last_update = log_stats.get("last_update", "Unknown")

                    try:
                        start_dt = datetime.datetime.fromisoformat(start_time)
                        start_time = start_dt.strftime("%Y-%m-%d %H:%M:%S")
                    except:
                        pass

                    try:
                        last_dt = datetime.datetime.fromisoformat(last_update)
                        last_update = last_dt.strftime("%Y-%m-%d %H:%M:%S")
                    except:
                        pass

                    st.write(f"Start time: {start_time}")
                    st.write(f"Last update: {last_update}")

    # Add upload button
    if st.button("Start uploading to SharePoint", use_container_width=True):
        upload_target_to_sharepoint(resume_upload=resume_upload, log_file=selected_log_file)

    # Add delete button
    if st.button("4. Delete SharePoint folders/files/tags", use_container_width=True):
        if not st.session_state.dest_manager:
            st.error("Please complete all target authentication information first")
            st.stop()

        access_token = st.session_state.dest_manager.get_access_token()
        if not access_token:
            st.error("Failed to get access token")
            st.stop()

        # Display confirmation dialog
        shutil.rmtree('temp', ignore_errors=True)
        shutil.rmtree('target', ignore_errors=True)
        with st.spinner("Deleting SharePoint content..."):
            try:
                # Import SharePointCleaner class
                from delete_sharepoint_fields import SharePointCleaner

                # Get parameters
                site_name = st.session_state.dest_manager.site_name
                tenant_domain = getattr(st.session_state.dest_manager, 'tenant_domain', "jcardcorp.sharepoint.com")
                target_library_name = getattr(st.session_state.dest_manager, 'target_library_name', "Shared Documents")

                # Create SharePointCleaner instance
                cleaner = SharePointCleaner(tenant_domain, target_library_name, site_name)

                # Set access token (if already obtained)
                cleaner.access_token = access_token

                # Execute delete operation
                result = cleaner.delete_sharepoint_content(delete_fields=True, delete_files=True)

                # Display result
                if result['success']:
                    st.success("✅ Delete operation completed successfully")

                    # Display detailed statistics
                    st.write("📊 Delete statistics:")
                    st.write(f"- Files and folders: Success {result['items']['success']}, Failed {result['items']['failed']}, Total {result['items']['total']}")
                    st.write(f"- Tags: Success {result['fields']['success']}, Failed {result['fields']['failed']}, Total {result['fields']['total']}")
                else:
                    st.error(f"❌ Delete operation failed: {result['error']}")
            except Exception as e:
                st.error(f"❌ Error occurred during delete: {str(e)}")
                st.exception(e)


st.markdown("---")

