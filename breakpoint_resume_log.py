import os
import json
import datetime
import glob
import logging
import threading
from typing import List, Dict, Tuple, Optional, Any

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SharePointUploadLogger:
    """
    SharePoint upload logger for implementing resumable upload functionality

    This class provides functions to create, load, save and manage upload logs,
    allowing uploads to resume from where they left off after interruption.
    """

    def __init__(self, site_name: Optional[str] = None):
        """
        Initialize upload logger

        Args:
            site_name (Optional[str]): SharePoint site name for log file naming
        """
        self.log_filename = None
        self.log_data = None
        self.lock = threading.Lock()
        self.save_timer = None
        self.site_name = site_name

        # Ensure log directory exists
        os.makedirs('logs', exist_ok=True)

    def create_log(self, files_to_upload: List[Tuple[str, str]]) -> str:
        """
        Create new upload log file

        Args:
            files_to_upload: List of files to upload, each as (local_path, sp_path) tuple

        Returns:
            str: Path of created log file
        """
        with self.lock:
            # Create log filename (using timestamp and site name)
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            if self.site_name:
                self.log_filename = f"logs/upload_log_{self.site_name}_{timestamp}.json"
            else:
                self.log_filename = f"logs/upload_log_{timestamp}.json"

            # Create log data structure
            self.log_data = {
                "metadata": {
                    "start_time": datetime.datetime.now().isoformat(),
                    "total_files": len(files_to_upload),
                    "files_uploaded": 0,
                    "last_update": datetime.datetime.now().isoformat()
                },
                "files": []
            }

            # Add file entries
            for local_path, sp_path in files_to_upload:
                self.log_data["files"].append({
                    "local_path": local_path,
                    "sp_path": sp_path,
                    "status": "pending",
                    "file_id": None,
                    "timestamp": None,
                    "error": None
                })

            # Save log file
            self._save_log_file()

            return self.log_filename

    def load_log(self, log_filename: str) -> bool:
        """
        Load existing upload log file

        Args:
            log_filename: Path of log file to load

        Returns:
            bool: Whether loading was successful
        """
        try:
            with self.lock:
                with open(log_filename, 'r', encoding='utf-8') as f:
                    self.log_data = json.load(f)
                self.log_filename = log_filename
                logger.info(f"✅ Successfully loaded log file: {log_filename}")
                return True
        except Exception as e:
            logger.error(f"❌ Failed to load upload log: {str(e)}")
            return False

    def save_log(self) -> bool:
        with self.lock:  # Ensure thread safety
            return self._save_log_file()

    def _save_log_file(self) -> bool:
        """
        Internal method: Save log file

        Returns:
            bool: Whether saving was successful
        """
        try:
            # Update last update time
            self.log_data["metadata"]["last_update"] = datetime.datetime.now().isoformat()

            # Save log file
            with open(self.log_filename, 'w', encoding='utf-8') as f:
                json.dump(self.log_data, f, ensure_ascii=False, indent=2)
            return True
        except Exception as e:
            logger.error(f"❌ Failed to save upload log: {str(e)}")
            return False

    def start_periodic_save(self, interval: int = 30) -> None:
        """
        Start periodic log saving

        Args:
            interval: Saving interval in seconds
        """
        def save_and_reschedule():
            self.save_log()
            self.save_timer = threading.Timer(interval, save_and_reschedule)
            self.save_timer.daemon = True
            self.save_timer.start()

        # Cancel existing timer (if any)
        if self.save_timer:
            self.save_timer.cancel()

        # Create new timer
        self.save_timer = threading.Timer(interval, save_and_reschedule)
        self.save_timer.daemon = True
        self.save_timer.start()

    def stop_periodic_save(self) -> None:
        """Stop periodic log saving"""
        if self.save_timer:
            self.save_timer.cancel()
            self.save_timer = None

    @staticmethod
    def find_latest_log() -> Optional[str]:
        """
        Find latest upload log file

        Returns:
            Optional[str]: Path of latest log file, None if not found
        """
        try:
            # Ensure log directory exists
            os.makedirs('logs', exist_ok=True)

            # Find all log files
            log_files = glob.glob('logs/upload_log_*.json')

            if not log_files:
                return None

            # Sort by modification time
            latest_log = max(log_files, key=os.path.getmtime)
            return latest_log
        except Exception as e:
            logger.error(f"❌ Failed to find latest upload log: {str(e)}")
            return None

    @staticmethod
    def get_available_logs(site_name: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get all available upload logs, optionally filtered by site name

        Args:
            site_name (Optional[str]): If provided, only returns logs for specified site

        Returns:
            List[Dict[str, Any]]: List of log file information
        """
        try:
            # Ensure log directory exists
            os.makedirs('logs', exist_ok=True)

            # Find all log files
            log_files = glob.glob('logs/upload_log_*.json')

            # Collect log file information
            log_info = []
            for log_file in log_files:
                try:
                    with open(log_file, 'r', encoding='utf-8') as f:
                        log_data = json.load(f)

                    if log_data:
                        # 提取有用的信息
                        start_time = log_data["metadata"].get("start_time", "未知")
                        total_files = log_data["metadata"].get("total_files", 0)
                        files_uploaded = log_data["metadata"].get("files_uploaded", 0)

                        # 格式化開始時間
                        try:
                            start_dt = datetime.datetime.fromisoformat(start_time)
                            formatted_time = start_dt.strftime("%Y-%m-%d %H:%M:%S")
                        except:
                            formatted_time = start_time

                        # 從文件名中提取站點名稱（如果有）
                        filename = os.path.basename(log_file)
                        site_name = None
                        if filename.startswith("upload_log_") and "_20" in filename:
                            parts = filename.split("_")
                            if len(parts) > 3:  # 有站點名稱
                                site_name = parts[2]

                        # 創建顯示名稱
                        if site_name:
                            display_name = f"{filename} [站點: {site_name}] ({formatted_time}, {files_uploaded}/{total_files})"
                        else:
                            display_name = f"{filename} ({formatted_time}, {files_uploaded}/{total_files})"

                        log_info.append({
                            "filename": log_file,
                            "display_name": display_name,
                            "site_name": site_name,
                            "start_time": start_time,
                            "total_files": total_files,
                            "files_uploaded": files_uploaded
                        })
                except Exception as e:
                    logger.error(f"❌ Error occurred while processing log file {log_file}: {str(e)}")

            # 按開始時間排序（最新的在前）
            log_info.sort(key=lambda x: x.get("start_time", ""), reverse=True)

            # 如果提供了站點名稱，過濾出相關的日誌
            if site_name:
                filtered_logs = [log for log in log_info if log.get("site_name") == site_name]
                return filtered_logs

            return log_info
        except Exception as e:
            logger.error(f"❌ Failed to get available upload logs: {str(e)}")
            return []

    def update_file_status(self, local_path: str, sp_path: str, status: str,
                          file_id: Optional[str] = None, error: Optional[str] = None) -> bool:
        """
        Update file upload status

        Args:
            local_path: Local file path
            sp_path: SharePoint file path
            status: Status ('pending', 'success', 'error')
            file_id: File ID (if upload succeeded)
            error: Error message (if upload failed)

        Returns:
            bool: Whether update was successful
        """
        with self.lock:
            if not self.log_data:
                logger.error("❌ Log data not loaded")
                return False

            # 查找文件條目
            file_entry = None
            file_index = -1

            for i, entry in enumerate(self.log_data["files"]):
                if entry["local_path"] == local_path and entry["sp_path"] == sp_path:
                    file_entry = entry
                    file_index = i
                    break

            # 如果找不到條目，創建一個新的
            if file_index == -1:
                file_entry = {
                    "local_path": local_path,
                    "sp_path": sp_path,
                    "status": status,
                    "file_id": file_id,
                    "timestamp": datetime.datetime.now().isoformat(),
                    "error": error
                }
                self.log_data["files"].append(file_entry)
            else:
                # 更新現有條目
                self.log_data["files"][file_index].update({
                    "status": status,
                    "file_id": file_id,
                    "timestamp": datetime.datetime.now().isoformat(),
                    "error": error
                })

            # 更新統計信息
            if status == "success":
                self.log_data["metadata"]["files_uploaded"] = sum(
                    1 for f in self.log_data["files"] if f["status"] == "success"
                )

            return True

    def get_successful_files(self) -> Dict[Tuple[str, str], str]:
        """
        Get successfully uploaded files

        Returns:
            Dict[Tuple[str, str], str]: Dictionary with (local_path, sp_path) as keys and file_id as values
        """
        with self.lock:
            if not self.log_data:
                return {}

            successful_files = {}
            for file_entry in self.log_data["files"]:
                if file_entry["status"] == "success" and file_entry["file_id"]:
                    key = (file_entry["local_path"], file_entry["sp_path"])
                    successful_files[key] = file_entry["file_id"]

            return successful_files

    def get_statistics(self) -> Dict[str, Any]:
        """
        Get log statistics

        Returns:
            Dict[str, Any]: Statistics information
        """
        with self.lock:
            if not self.log_data:
                return {
                    "total": 0,
                    "success": 0,
                    "error": 0,
                    "pending": 0,
                    "start_time": None,
                    "last_update": None
                }

            total = len(self.log_data["files"])
            success = sum(1 for f in self.log_data["files"] if f["status"] == "success")
            error = sum(1 for f in self.log_data["files"] if f["status"] == "error")
            pending = sum(1 for f in self.log_data["files"] if f["status"] == "pending")

            return {
                "total": total,
                "success": success,
                "error": error,
                "pending": pending,
                "start_time": self.log_data["metadata"].get("start_time"),
                "last_update": self.log_data["metadata"].get("last_update")
            }

    def filter_files_to_upload(self, all_files: List[Tuple[str, str]]) -> List[Tuple[str, str]]:
        """
        Filter files that need uploading (excluding successfully uploaded ones)

        Args:
            all_files: List of all files, each as (local_path, sp_path) tuple

        Returns:
            List[Tuple[str, str]]: List of files to upload
        """
        successful_files = self.get_successful_files()

        # 過濾掉已經上傳的文件
        files_to_upload = []
        for file_tuple in all_files:
            if file_tuple not in successful_files:
                files_to_upload.append(file_tuple)
        return files_to_upload
