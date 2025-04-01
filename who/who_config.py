
FOLDER_PREFIX = "gcs"
DATA_INGESTION_BASE_FOLDER = f"{FOLDER_PREFIX}/pubhealth-ai-training-corpus"
RAW_DATA_BASE_FOLDER = f"{DATA_INGESTION_BASE_FOLDER}/raw"
WHO_DOCUMENTS_URL =  "https://www.who.int/publications/m"
WHO_FACT_SHEETS_URL = "https://www.who.int/news-room/fact-sheets"
WHO_ROBOTS_TXT = 'https://www.who.int/robots.txt'

WHO_DOCUMENTS_FOLDER = 'WHO_documents'
WHO_FACT_SHEETS_FOLDER = 'WHO_fact_sheets'

DOCUMENTS_EXT = 'pdf'
FACT_SHEETS_EXT = 'txt'
METADATA_EXT = "parquet"
METADATA_PREFIX = "logs_"

METADATA_FOLDER = f"{DATA_INGESTION_BASE_FOLDER}/logs"

# metadata format:
# url - string type
# datetime_captured 
# title - string type
# author - list type, list of strings
# publication_date - string type (non-normalized formatting at this stage due to wide variety of inputs & broadly generalized ingestion code)
# license - string type
# file_type - string type lowercase (examples: 'pdf','html','xml','json')
# storage_location - string type (indicates where you save the file to)
# datetime_last_updated - string type, dt.strftime("%Y-%m-%d %H:%M:%S.%f") (indicates when you write it to the log OR if you update the value of that row in the log)
# json_additional_metadata - dict type (examples: {'keywords':['maternal health', 'chronic disease']} )

METADATA_SCHEMA = ["url", "datetime_captured", "title", "author", "publication_date", "license", "file_type", "storage_location", "datetime_last_updated", "json_additional_metadata"]

METADATA_DATE_FMT = "%Y-%m-%d %H:%M:%S.%f"
DEFAULT_WAIT_TIME = 5 #seconds
DOWNLOAD_LIMIT = 100
WHO_LICENSE = 'CC BY-NC-SA 3.0 IGO'
WHO_DOCUMENT_TYPES = ['Manual', 'Position paper']
 