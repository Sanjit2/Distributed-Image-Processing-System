
from flask import Flask, render_template, request, jsonify, send_file
from werkzeug.utils import secure_filename
import cv2
import numpy as np
from PIL import Image
import io
import uuid
import json
import time
import threading
from pathlib import Path
from confluent_kafka import Producer, Consumer, KafkaError
import redis
import logging

import config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize Flask app
app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = config.MAX_FILE_SIZE

# Initialize Redis client
try:
    redis_client = redis.Redis(
        host=config.REDIS_HOST,
        port=config.REDIS_PORT,
        decode_responses=True
    )
    redis_client.ping()
    logger.info(f"✅ Connected to Redis at {config.REDIS_HOST}:{config.REDIS_PORT}")
except Exception as e:
    logger.error(f"❌ Failed to connect to Redis: {e}")
    raise

# Initialize Kafka Producer
producer_config = {
    'bootstrap.servers': config.KAFKA_BROKER,
    'client.id': 'master-producer'
}

try:
    producer = Producer(producer_config)
    logger.info(f"✅ Kafka producer initialized for {config.KAFKA_BROKER}")
except Exception as e:
    logger.error(f"❌ Failed to initialize Kafka producer: {e}")
    raise


def delivery_report(err, msg):
    if err is not None:
        logger.error(f'Message delivery failed: {err}')
    else:
        logger.debug(f'Message delivered to {msg.topic()} [{msg.partition()}]')


def split_image_into_tiles(image_path, job_id):
    
    try:
        # Read image
        img = cv2.imread(str(image_path))
        if img is None:
            raise ValueError("Could not read image")
        
        height, width = img.shape[:2]
        logger.info(f"Image dimensions: {width}x{height}")
        
        # Validate minimum size
        if width < config.MIN_IMAGE_SIZE or height < config.MIN_IMAGE_SIZE:
            raise ValueError(f"Image too small. Minimum size: {config.MIN_IMAGE_SIZE}x{config.MIN_IMAGE_SIZE}")
        
        tiles = []
        tile_id = 0
        
        # Split into tiles
        for y in range(0, height, config.TILE_SIZE):
            for x in range(0, width, config.TILE_SIZE):
                # Extract tile
                tile = img[y:y+config.TILE_SIZE, x:x+config.TILE_SIZE]
                
                # Encode tile as JPEG
                _, buffer = cv2.imencode('.jpg', tile, [cv2.IMWRITE_JPEG_QUALITY, config.JPEG_QUALITY])
                tile_bytes = buffer.tobytes()
                
                tiles.append({
                    'tile_id': tile_id,
                    'x': x,
                    'y': y,
                    'width': tile.shape[1],
                    'height': tile.shape[0],
                    'data': tile_bytes
                })
                tile_id += 1
        
        logger.info(f"Split image into {len(tiles)} tiles")
        return tiles, width, height
        
    except Exception as e:
        logger.error(f"Error splitting image: {e}")
        raise


def publish_tasks(job_id, tiles, transformation):
   
    try:
        for tile in tiles:
            # Message key: job_id:tile_id:transformation
            key = f"{job_id}:{tile['tile_id']}:{transformation}"
            
            # Message value: tile image data
            value = tile['data']
            
            # Publish to Kafka
            producer.produce(
                config.TASKS_TOPIC,
                key=key.encode('utf-8'),
                value=value,
                callback=delivery_report
            )
        
        # Wait for all messages to be delivered
        producer.flush()
        logger.info(f"Published {len(tiles)} tasks for job {job_id}")
        
    except Exception as e:
        logger.error(f"Error publishing tasks: {e}")
        raise


def reconstruct_image(job_id):
    
    try:
        # Get job metadata from Redis
        job_key = config.get_job_key(job_id)
        job_data = redis_client.hgetall(job_key)
        
        if not job_data:
            raise ValueError(f"Job {job_id} not found in Redis")
        
        width = int(job_data['img_width'])
        height = int(job_data['img_height'])
        total_tiles = int(job_data['total_tiles'])
        transformation = job_data['transformation']
        
        logger.info(f"Reconstructing image: {width}x{height}, {total_tiles} tiles")
        
        # Determine if grayscale or color
        is_grayscale = (transformation == 'grayscale')
        
        # Create blank canvas
        if is_grayscale:
            canvas = np.zeros((height, width), dtype=np.uint8)
        else:
            canvas = np.zeros((height, width, 3), dtype=np.uint8)
        
        # Load and place tiles
        tile_folder = config.TEMP_TILES_FOLDER / job_id
        placed_tiles = 0
        
        for tile_id in range(total_tiles):
            tile_path = tile_folder / f"tile_{tile_id}.jpg"
            
            if not tile_path.exists():
                logger.warning(f"Missing tile {tile_id} for job {job_id}")
                continue
            
            # Read tile
            tile_img = cv2.imread(str(tile_path))
            if tile_img is None:
                logger.warning(f"Could not read tile {tile_id}")
                continue
            
            # Convert to grayscale if needed
            if is_grayscale and len(tile_img.shape) == 3:
                tile_img = cv2.cvtColor(tile_img, cv2.COLOR_BGR2GRAY)
            
            # Calculate tile position
            tiles_per_row = (width + config.TILE_SIZE - 1) // config.TILE_SIZE
            row = tile_id // tiles_per_row
            col = tile_id % tiles_per_row
            
            y = row * config.TILE_SIZE
            x = col * config.TILE_SIZE
            
            # Place tile on canvas
            tile_h, tile_w = tile_img.shape[:2]
            canvas[y:y+tile_h, x:x+tile_w] = tile_img
            placed_tiles += 1
        
        logger.info(f"Placed {placed_tiles}/{total_tiles} tiles")
        
        # Save result
        result_path = config.RESULTS_FOLDER / f"{job_id}.jpg"
        cv2.imwrite(str(result_path), canvas, [cv2.IMWRITE_JPEG_QUALITY, config.JPEG_QUALITY])
        
        # Update job status
        redis_client.hset(job_key, 'status', 'complete')
        
        logger.info(f"✅ Job {job_id} complete! Result saved to {result_path}")
        
        # Cleanup temp tiles
        import shutil
        shutil.rmtree(tile_folder, ignore_errors=True)
        
        return result_path
        
    except Exception as e:
        logger.error(f"Error reconstructing image: {e}")
        # Update job status to error
        redis_client.hset(config.get_job_key(job_id), 'status', 'error')
        raise


def consume_results():

    consumer_config = {
        'bootstrap.servers': config.KAFKA_BROKER,
        'group.id': 'master-consumer',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True
    }
    
    consumer = Consumer(consumer_config)
    consumer.subscribe([config.RESULTS_TOPIC])
    
    logger.info("🎧 Results consumer started")
    
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            
            if msg is None:
                continue
            
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    logger.error(f"Consumer error: {msg.error()}")
                    continue
            
            try:
                # Parse message key: job_id:tile_id
                key = msg.key().decode('utf-8')
                job_id, tile_id = key.split(':')
                tile_id = int(tile_id)
                
                # Get tile data
                tile_data = msg.value()
                
                # Save tile to temp folder
                tile_folder = config.TEMP_TILES_FOLDER / job_id
                tile_folder.mkdir(exist_ok=True)
                
                tile_path = tile_folder / f"tile_{tile_id}.jpg"
                with open(tile_path, 'wb') as f:
                    f.write(tile_data)
                
                logger.info(f"📥 Received tile {tile_id} for job {job_id}")
                
                # Increment processed count atomically
                job_key = config.get_job_key(job_id)
                processed_count = redis_client.hincrby(job_key, 'processed_count', 1)
                total_tiles = int(redis_client.hget(job_key, 'total_tiles'))
                
                logger.info(f"Job {job_id}: {processed_count}/{total_tiles} tiles processed")
                
                # Check if all tiles received
                if processed_count >= total_tiles:
                    logger.info(f"🎉 All tiles received for job {job_id}. Starting reconstruction...")
                    reconstruct_image(job_id)
                
            except Exception as e:
                logger.error(f"Error processing result message: {e}")
                
    except KeyboardInterrupt:
        logger.info("Shutting down results consumer")
    finally:
        consumer.close()


consumer_thread = threading.Thread(target=consume_results, daemon=True)
consumer_thread.start()


@app.route('/')
def index():
    """Home page with upload form"""
    return render_template('index.html')


@app.route('/upload', methods=['POST'])
def upload():
    """Handle image upload and initiate processing"""
    try:
        # Check if file was uploaded
        if 'file' not in request.files:
            return jsonify({'error': 'No file uploaded'}), 400
        
        file = request.files['file']
        
        if file.filename == '':
            return jsonify({'error': 'No file selected'}), 400
        
        if not config.allowed_file(file.filename):
            return jsonify({'error': 'Invalid file type. Allowed: ' + ', '.join(config.ALLOWED_EXTENSIONS)}), 400
        
        # Get transformation type
        transformation = request.form.get('transformation', 'grayscale')
        if transformation not in config.TRANSFORMATIONS:
            return jsonify({'error': 'Invalid transformation'}), 400
        
        # Generate job ID
        job_id = str(uuid.uuid4())
        
        # Save uploaded file
        filename = secure_filename(file.filename)
        upload_path = config.UPLOAD_FOLDER / f"{job_id}_{filename}"
        file.save(upload_path)
        
        logger.info(f"📤 New job {job_id}: {filename} ({transformation})")
        
        # Split image into tiles
        tiles, img_width, img_height = split_image_into_tiles(upload_path, job_id)
        
        # Store job metadata in Redis
        job_key = config.get_job_key(job_id)
        redis_client.hset(job_key, mapping={
            'status': 'processing',
            'transformation': transformation,
            'total_tiles': len(tiles),
            'processed_count': 0,
            'img_width': img_width,
            'img_height': img_height,
            'filename': filename
        })
        
        # Publish tasks to Kafka
        publish_tasks(job_id, tiles, transformation)
        
        return jsonify({
            'success': True,
            'job_id': job_id,
            'total_tiles': len(tiles)
        })
        
    except Exception as e:
        logger.error(f"Error handling upload: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/status/<job_id>')
def status(job_id):
    """Get job status"""
    try:
        job_key = config.get_job_key(job_id)
        job_data = redis_client.hgetall(job_key)
        
        if not job_data:
            return jsonify({'error': 'Job not found'}), 404
        
        return jsonify({
            'job_id': job_id,
            'status': job_data['status'],
            'total_tiles': int(job_data['total_tiles']),
            'processed_count': int(job_data['processed_count']),
            'transformation': job_data['transformation']
        })
        
    except Exception as e:
        logger.error(f"Error getting status: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/result/<job_id>')
def result(job_id):
    """Download processed image"""
    try:
        result_path = config.RESULTS_FOLDER / f"{job_id}.jpg"
        
        if not result_path.exists():
            return jsonify({'error': 'Result not ready yet'}), 404
        
        return send_file(result_path, mimetype='image/jpeg')
        
    except Exception as e:
        logger.error(f"Error serving result: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/dashboard')
def dashboard():
    """Worker monitoring dashboard"""
    return render_template('dashboard.html')


@app.route('/api/workers')
def api_workers():
    """Get active workers"""
    try:
        # Scan for worker keys
        workers = []
        for key in redis_client.scan_iter("worker:*"):
            worker_id = key.split(':')[1]
            ttl = redis_client.ttl(key)
            
            if ttl > 0:  # Worker is alive
                workers.append({
                    'worker_id': worker_id,
                    'status': 'alive',
                    'ttl': ttl
                })
        
        return jsonify({'workers': workers})
        
    except Exception as e:
        logger.error(f"Error getting workers: {e}")
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    logger.info("=" * 50)
    logger.info("🚀 Starting Master Application")
    logger.info(f"📍 Master IP: {config.MASTER_IP}")
    logger.info(f"📡 Kafka Broker: {config.KAFKA_BROKER}")
    logger.info(f"💾 Redis: {config.REDIS_HOST}:{config.REDIS_PORT}")
    logger.info(f"🌐 Flask: http://{config.FLASK_HOST}:{config.FLASK_PORT}")
    logger.info("=" * 50)
    
    app.run(
        host=config.FLASK_HOST,
        port=config.FLASK_PORT,
        debug=config.FLASK_DEBUG
    )
