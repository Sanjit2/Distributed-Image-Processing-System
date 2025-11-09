import os
# Broker Configuration 
BROKER_IP = os.getenv('BROKER_IP', '172.23.54.181')
KAFKA_BROKER = f"{BROKER_IP}:9092"

# Master Configuration 
MASTER_IP = os.getenv('MASTER_IP', '172.23.150.205')

# Redis Configuration 
REDIS_HOST = os.getenv('REDIS_HOST', '172.23.150.205')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))

# Flask Configuration
FLASK_HOST = '0.0.0.0'  
FLASK_PORT = 5000
FLASK_DEBUG = False

# Kafka Topics
TASKS_TOPIC = 'image_tasks'
RESULTS_TOPIC = 'image_results'
HEARTBEAT_TOPIC = 'worker_heartbeats'

# Kafka Consumer Group
CONSUMER_GROUP = 'image_workers'

# Kafka Settings
KAFKA_AUTO_OFFSET_RESET = 'earliest'
KAFKA_SESSION_TIMEOUT_MS = 30000

# Tile Configuration
TILE_SIZE = 512  
MIN_IMAGE_SIZE = 1024  

# Supported Transformations
TRANSFORMATIONS = ['grayscale', 'blur']

# Image Quality
JPEG_QUALITY = 95


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

# Heartbeat interval (seconds)
HEARTBEAT_INTERVAL = 5

# Worker timeout (seconds)
WORKER_TIMEOUT = 15

def get_job_key(job_id):
    """Get Redis key for job metadata"""
    return f"job:{job_id}"

def get_worker_key(worker_id):
    """Get Redis key for worker status"""
    return f"worker:{worker_id}"

def allowed_file(filename):
    """Check if file extension is allowed"""
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS
