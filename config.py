# WhatsFind Configuration for Large File Processing

# File size thresholds (in MB)
LARGE_FILE_WARNING_MB = 1024     # Show warning for files over 1GB
VERY_LARGE_FILE_MB = 2048        # Consider files over 2GB as "very large"

# Processing settings
BATCH_SIZE = 1000                # Number of messages to process in each batch
PROGRESS_UPDATE_INTERVAL = 100   # Update progress every N batches

# Memory optimization settings
USE_CHUNKED_PROCESSING = True    # Enable chunked processing for large files
CLEANUP_TEMP_DATA = True         # Clean up temporary data after processing

# Upload limits
MAX_UPLOAD_SIZE_MB = 2048        # Maximum file size for web upload (matches Streamlit config)

# Debug settings
ENABLE_MEMORY_MONITORING = False  # Set to True to enable memory usage monitoring
VERBOSE_LOGGING = False          # Set to True for detailed processing logs