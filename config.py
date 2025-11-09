"""
Configuration file for Distributed Image Processing System
Handles network configuration for all nodes in the system
"""

import os

# ========================================
# NETWORK CONFIGURATION
# ========================================

# YOUR 4 UBUNTU VMs:
# Sanjit (Broker): 172.23.54.181 - Kafka + Zookeeper
# Sarang (Master): 172.23.150.205 - Flask + Redis
# Shreyas (Worker): 172.23.168.12 - Image Processing
# Sanjoli (Worker): 172.23.159.0 - Image Processing

# Broker Configuration (Sanjit's Ubuntu VM)
BROKER_IP = os.getenv('BROKER_IP', '172.23.54.181')
KAFKA_BROKER = f"{BROKER_IP}:9092"

# Master Configuration (Sarang's Ubuntu VM)
MASTER_IP = os.getenv('MASTER_IP', '172.23.150.205')

# Redis Configuration (runs on Sarang's Ubuntu VM)
REDIS_HOST = os.getenv('REDIS_HOST', '172.23.150.205')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))

# Flask Configuration
FLASK_HOST = '0.0.0.0'  # Listen on all interfaces
FLASK_PORT = 5000
FLASK_DEBUG = False

# ========================================
# KAFKA CONFIGURATION
# ========================================

# Kafka Topics
TASKS_TOPIC = 'image_tasks'
RESULTS_TOPIC = 'image_results'
HEARTBEAT_TOPIC = 'worker_heartbeats'

# Kafka Consumer Group
CONSUMER_GROUP = 'image_workers'

# Kafka Settings
KAFKA_AUTO_OFFSET_RESET = 'earliest'
KAFKA_SESSION_TIMEOUT_MS = 30000

# ========================================
# IMAGE PROCESSING CONFIGURATION
# ========================================

# Tile Configuration
TILE_SIZE = 512  # 512x512 pixels per tile
MIN_IMAGE_SIZE = 1024  # Minimum image dimension

# Supported Transformations
TRANSFORMATIONS = ['grayscale', 'blur']

# Image Quality
JPEG_QUALITY = 95

# ========================================
# FILE PATHS
# ========================================

import pathlib

BASE_DIR = pathlib.Path(__file__).parent.absolute()
UPLOAD_FOLDER = BASE_DIR / 'uploads'
RESULTS_FOLDER = BASE_DIR / 'results'
TEMP_TILES_FOLDER = BASE_DIR / 'temp_tiles'

# Create folders if they don't exist
UPLOAD_FOLDER.mkdir(exist_ok=True)
RESULTS_FOLDER.mkdir(exist_ok=True)
TEMP_TILES_FOLDER.mkdir(exist_ok=True)

# Maximum file size (50MB)
MAX_FILE_SIZE = 50 * 1024 * 1024

# Allowed extensions
ALLOWED_EXTENSIONS = {'png', 'jpg', 'jpeg', 'bmp', 'tiff'}

# ========================================
# WORKER CONFIGURATION
# ========================================

# Heartbeat interval (seconds)
HEARTBEAT_INTERVAL = 5

# Worker timeout (seconds)
WORKER_TIMEOUT = 15

# ========================================
# REDIS KEY PATTERNS
# ========================================

def get_job_key(job_id):
    """Get Redis key for job metadata"""
    return f"job:{job_id}"

def get_worker_key(worker_id):
    """Get Redis key for worker status"""
    return f"worker:{worker_id}"

# ========================================
# HELPER FUNCTIONS
# ========================================

def allowed_file(filename):
    """Check if file extension is allowed"""
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS
