import os
import json
import shutil
import logging
from typing import Dict, List, Optional
from dataclasses import dataclass
from sharepoint_utils import SharePointClient, normalize_path, extract_text_from_docx, extract_text_from_xlsx, extract_text_from_pptx


logging.basicConfig(level=logging.INFO,
                   format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class ClassificationRule:
    name: str
    conditions: Dict
    target_folder: str

@dataclass
class FileInfo:
    name: str
    type: str
    path: str
    tags: Dict[str, str]
    content_tag: Optional[str] = None

class SharePointDownloader(SharePointClient):
    def __init__(self, config_path="output_parameters.json"):
        """
        Initialize SharePoint downloader
        
        Args:
            config_path (str): Configuration file path
        """
        super().__init__(config_path=config_path)
    
    def _normalize_path(self, path, base_dir=""):
        """
        Normalize path and create complete local path
        
        Args:
            path (str): Original path
            base_dir (str): Base directory
            
        Returns:
            str: Normalized complete local path
        """
        normalized = normalize_path(path).replace('/', os.sep).replace('\\', os.sep)
        return os.path.join(base_dir, normalized) if base_dir else normalized
    
    def _ensure_directory(self, directory_path):
        """
        Ensure directory exists, create if it doesn't
        
        Args:
            directory_path (str): Directory path
            
        Returns:
            bool: Whether creation was successful or directory already exists
        """
        try:
            os.makedirs(directory_path, exist_ok=True)
            return True
        except Exception as e:
            logger.error(f"❌ Failed to create directory {directory_path}: {str(e)}")
            return False
            
    def download_to_temp(self, temp_dir="temp"):
        """
        Download all files to temporary directory, including empty folders
        
        Args:
            temp_dir (str): Temporary directory path
        
        Returns:
            dict: Download statistics
        """
        stats = {
            'total': 0,
            'success': 0,
            'failed': 0,
            'folders': 0  # Folder count
        }
        
        try:
            # Reset temporary directory
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
            self._ensure_directory(temp_dir)
            
            logger.info(f"Starting download to {temp_dir}")
            
            # Ensure access token and drive ID are available
            if not self.access_token and not self.get_access_token():
                logger.error("❌ Unable to obtain access token")
                return stats
                    
            if not self.drive_id and not self.get_drive_id():
                logger.error("❌ Unable to obtain drive ID")
                return stats
            
            # Get all items
            items = self.get_items_recursive()
            
            # Save item information to JSON file
            self._ensure_directory('tag_result')
            with open('tag_result/sharepoint_fields.json', 'w', encoding='utf-8') as f:
                json.dump(items, f, ensure_ascii=False, indent=2)
            logger.info("✅ File information saved to tag_result/sharepoint_fields.json")
            
            # First create all folder structures
            for item in items:
                if item['type'] == 'folder':
                    folder_path = self._normalize_path(item['path'], temp_dir)
                    if self._ensure_directory(folder_path):
                        stats['folders'] += 1
                        logger.info(f"✅ Created folder: {folder_path}")
            
            # Download files
            for item in items:
                if item['type'] == 'file':
                    stats['total'] += 1
                    
                    try:
                        # Get local path
                        local_path = self._normalize_path(item['path'], temp_dir)
                        
                        # Ensure directory exists
                        self._ensure_directory(os.path.dirname(local_path))
                        
                        # Download file
                        content = self.download_file(item['path'])
                        if content:
                            with open(local_path, 'wb') as f:
                                f.write(content)
                            stats['success'] += 1
                            logger.info(f"✅ Downloaded file to: {local_path}")
                        else:
                            stats['failed'] += 1
                            logger.error(f"❌ Download failed: {item['path']}")
                            
                    except Exception as e:
                        stats['failed'] += 1
                        logger.error(f"❌ Error processing file {item['path']}: {str(e)}")
                        
            # Output statistics
            logger.info("\nDownload complete! Statistics:")
            logger.info(f"Total files: {stats['total']}")
            logger.info(f"Successfully downloaded: {stats['success']}")
            logger.info(f"Failed downloads: {stats['failed']}")
            logger.info(f"Created folders: {stats['folders']}")
            
            # Merge SharePoint data
            self.merge_sharepoint_data()
            
            return stats
            
        except Exception as e:
            logger.error(f"❌ Error during download process: {str(e)}")
            raise
            
    def merge_sharepoint_data(self, source_file='tag_result/sharepoint_fields.json', target_file='tag_result/merged_sharepoint_data.json'):
        """
        Merge SharePoint data in preparation for classification
        
        Args:
            source_file (str): Source data file path
            target_file (str): Target merged data file path
            
        Returns:
            bool: Whether data merge was successful
        """
        try:
            # Check if file exists
            if not os.path.exists(source_file):
                logger.error(f"❌ SharePoint fields data file not found: {source_file}")
                return False
                
            # Ensure target directory exists
            self._ensure_directory(os.path.dirname(target_file))
                
            # Read SharePoint fields data
            with open(source_file, 'r', encoding='utf-8') as f:
                fields_data = json.load(f)
                
            # Create merged data
            merged_data = []
            base_fields = ['name', 'type', 'path', 'created_time', 'modified_time', 'size']
            
            for item in fields_data:
                # Basic item information
                merged_item = {
                    'name': item['name'],
                    'type': item['type'],
                    'path': item['path'],
                    'created_time': item.get('created_time'),
                    'modified_time': item.get('modified_time'),
                    'size': item.get('size', 0)
                }
                
                # Add other fields
                for key, value in item.items():
                    if key not in base_fields:
                        merged_item[key] = value
                        
                merged_data.append(merged_item)
                
            # Save merged data
            with open(target_file, 'w', encoding='utf-8') as f:
                json.dump(merged_data, f, ensure_ascii=False, indent=2)
                
            logger.info(f"✅ SharePoint data merged to {target_file}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error merging SharePoint data: {str(e)}")
            return False

class AutoDocumentClassifier:
    def __init__(self, rules_file: str = 'config/classification_rules.json',
                 merged_data_file: str = 'tag_result/merged_sharepoint_data.json'):
        self.rules_file = rules_file
        self.merged_data_file = merged_data_file
        self.rules = self._load_rules(rules_file)
        self.tagged_files = self._load_merged_data(merged_data_file)
        self.default_folder = "Unclassified"
    
    def _normalize_path(self, path, base_dir=""):
        """Normalize path and create complete local path"""
        normalized = normalize_path(path).replace('/', os.sep).replace('\\', os.sep)
        return os.path.join(base_dir, normalized) if base_dir else normalized
    
    def _ensure_directory(self, directory_path):
        """Ensure directory exists, create if it doesn't"""
        try:
            os.makedirs(directory_path, exist_ok=True)
            return True
        except Exception as e:
            logger.error(f"❌ Failed to create directory {directory_path}: {str(e)}")
            return False

    def _load_rules(self, rules_file: str) -> List[ClassificationRule]:
        """Load classification rules"""
        try:
            with open(rules_file, 'r', encoding='utf-8') as f:
                config = json.load(f)
                rules = []
                for rule in config['rules']:
                    rules.append(ClassificationRule(
                        name=rule['name'],
                        conditions=rule['conditions'],
                        target_folder=rule['target_folder']
                    ))
                self.default_folder = config.get('default_folder', 'Unclassified')
                return rules
        except Exception as e:
            logger.error(f"Failed to load rules file: {str(e)}")
            return []

    def _load_merged_data(self, merged_data_file: str) -> List[FileInfo]:
        """Load merged tag data"""
        try:
            with open(merged_data_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                files = []
                for item in data:
                    files.append(FileInfo(
                        name=item['name'],
                        type=item['type'],
                        path=item['path'],
                        tags={k: v for k, v in item.items()
                             if k not in ['name', 'type', 'path', 'content_tag']},
                        content_tag=item.get('content_tag')
                    ))
                return files
        except Exception as e:
            logger.error(f"Failed to load merged data file: {str(e)}")
            return []

    def _check_conditions(self, file_info: FileInfo, conditions: Dict) -> bool:
        """Check if file meets conditions"""
        # Check labels
        if 'labels' in conditions:
            # Check if the specified label fields exist
            if not all(label in file_info.tags for label in conditions['labels']):
                logger.info(f"⚠️ Missing specified label fields: {conditions['labels']}")
                return False

        # Check file type
        if 'file_types' in conditions:
            if '*' not in conditions['file_types']:
                file_ext = os.path.splitext(file_info.name)[1].lower()
                if not any(file_ext.endswith(t.lower())
                          for t in conditions['file_types']):
                    return False

        # Check keywords (only search in specified label field values)
        if 'keywords' in conditions and 'labels' in conditions:
            found = False
            for label in conditions['labels']:
                label_value = str(file_info.tags.get(label, ""))
                for keyword in conditions['keywords']:
                    if keyword.lower() in label_value.lower():
                        logger.info(f"✅ Keyword '{keyword}' found in label field '{label}' with value '{label_value}'")
                        found = True
                        break
                if found:
                    break
            if not found:
                logger.info(f"⚠️ No keywords found in specified label fields: {conditions['keywords']}")
                return False

        # Check content_tag
        if 'content_tags' in conditions:
            if not file_info.content_tag or \
               not any(tag.lower() in file_info.content_tag.lower() 
                      for tag in conditions['content_tags']):
                return False

        return True

    def classify_document(self, file_info: FileInfo) -> str:
        """Classify document according to rules"""
        # For folders, maintain original location
        if file_info.type == 'folder':
            return os.path.dirname(file_info.path) or self.default_folder

        # Check each classification rule
        for rule in self.rules:
            if self._check_conditions(file_info, rule.conditions):
                return rule.target_folder.lstrip('/')

        return self.default_folder

    def auto_classify(self, source_dir: str = 'temp', target_dir: str = 'target') -> Dict:
        """Automatically classify files"""
        stats = {
            'total': 0,
            'success': 0,
            'failed': 0,
            'missing': 0,
            'categories': {},
            'missing_files': []
        }

        try:
            # Reset target directory
            if os.path.exists(target_dir):
                shutil.rmtree(target_dir)
            self._ensure_directory(target_dir)
            
            # 可选：清理相关upload log
            # from breakpoint_resume_log import SharePointUploadLogger
            # SharePointUploadLogger.cleanup_old_logs(keep_recent=0)

            logger.info(f"Starting file classification from {source_dir} to {target_dir}")

            # Process each file
            for file_info in self.tagged_files:
                stats['total'] += 1

                # Skip folders
                if file_info.type == 'folder':
                    continue

                try:
                    # Get source file path
                    source_path = self._normalize_path(file_info.path, source_dir)

                    # Get target category
                    target_category = self.classify_document(file_info)
                    
                    # Update category statistics
                    stats['categories'][target_category] = stats['categories'].get(target_category, 0) + 1

                    # Create target path
                    target_path = os.path.join(target_dir, target_category, os.path.basename(source_path))
                    self._ensure_directory(os.path.dirname(target_path))

                    # Copy file
                    if os.path.exists(source_path) and os.path.isfile(source_path):
                        shutil.copy2(source_path, target_path)
                        stats['success'] += 1
                        logger.info(f"✅ Classified file to: {target_path}")
                    else:
                        stats['missing'] += 1
                        stats['missing_files'].append(file_info.path)
                        logger.warning(f"⚠️ Source file not found: {source_path}")

                except Exception as e:
                    stats['failed'] += 1
                    logger.error(f"❌ Failed to process file {file_info.path}: {str(e)}")

            # Output statistics
            logger.info("\nClassification complete! Statistics:")
            logger.info(f"Total files: {stats['total']}")
            logger.info(f"Successfully processed: {stats['success']}")
            logger.info(f"Processing failed: {stats['failed']}")
            logger.info(f"Missing files: {stats['missing']}")
            logger.info("\nCategory statistics:")
            for category, count in stats['categories'].items():
                logger.info(f"{category}: {count} files")

            return stats

        except Exception as e:
            logger.error(f"Error during classification process: {str(e)}")
            raise

def main():
    try:
        # Initialize downloader and download files
        downloader = SharePointDownloader(config_path="output_parameters.json")
        download_stats = downloader.download_to_temp(temp_dir="temp")
        
        # Initialize classifier
        classifier = AutoDocumentClassifier(
            rules_file='config/classification_rules.json',
            merged_data_file='tag_result/merged_sharepoint_data.json'
        )
        
        # Execute automatic classification
        classify_stats = classifier.auto_classify(source_dir='temp', target_dir='target')
        
        # Output classification results
        logger.info("\nClassification complete! Statistics:")
        logger.info(f"Total files: {classify_stats['total']}")
        logger.info(f"Successfully classified: {classify_stats['success']}")
        logger.info(f"Classification failed: {classify_stats['failed']}")
        logger.info(f"Missing files: {classify_stats['missing']}")
        
        # Output file count by category
        logger.info("\nFile count by category:")
        for category, count in classify_stats['categories'].items():
            logger.info(f"{category or 'Unclassified'}: {count} files")
        
        # If there are missing files, output the list
        if classify_stats['missing_files']:
            logger.info("\nMissing files list:")
            for file_path in classify_stats['missing_files']:
                logger.info(f"- {file_path}")
                
    except Exception as e:
        logger.error(f"Error during program execution: {str(e)}")
        raise

if __name__ == "__main__":
    main()

