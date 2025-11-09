
import sys
import cv2
import numpy as np
import time
import threading
import json
import logging
from confluent_kafka import Consumer, Producer, KafkaError
import redis

import config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Global worker ID
WORKER_ID = None


def apply_transformation(image, transformation):
    """
    Apply image transformation to a tile
    """
    try:
        if transformation == 'grayscale':
            # Convert to grayscale
            if len(image.shape) == 3:
                gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
                # Convert back to 3 channels for consistency
                result = cv2.cvtColor(gray, cv2.COLOR_GRAY2BGR)
            else:
                result = image
            return result
            
        elif transformation == 'blur':
            # Apply Gaussian blur
            result = cv2.GaussianBlur(image, (51, 51), 0)
            return result
            
        else:
            logger.warning(f"Unknown transformation: {transformation}")
            return image
            
    except Exception as e:
        logger.error(f"Error applying transformation: {e}")
        return image


def send_heartbeat(producer, redis_client):
    """
    Background thread to send heartbeat signals
    """
    logger.info(f"💓 Heartbeat thread started for worker {WORKER_ID}")
    
    while True:
        try:
            # Send heartbeat to Kafka
            heartbeat_data = {
                'worker_id': WORKER_ID,
                'status': 'alive',
                'timestamp': time.time()
            }
            
            producer.produce(
                config.HEARTBEAT_TOPIC,
                key=WORKER_ID.encode('utf-8'),
                value=json.dumps(heartbeat_data).encode('utf-8')
            )
            producer.poll(0)
            
            # Update Redis with worker status (with TTL)
            worker_key = config.get_worker_key(WORKER_ID)
            redis_client.setex(worker_key, config.WORKER_TIMEOUT, 'alive')
            
            logger.debug(f"💓 Heartbeat sent by {WORKER_ID}")
            
        except Exception as e:
            logger.error(f"Error sending heartbeat: {e}")
        
        # Sleep for heartbeat interval
        time.sleep(config.HEARTBEAT_INTERVAL)


def process_tasks():
    """
    Main processing loop - consume and process image tiles
    """
    # Configure Kafka consumer
    consumer_config = {
        'bootstrap.servers': config.KAFKA_BROKER,
        'group.id': config.CONSUMER_GROUP,
        'auto.offset.reset': config.KAFKA_AUTO_OFFSET_RESET,
        'enable.auto.commit': True,
        'session.timeout.ms': config.KAFKA_SESSION_TIMEOUT_MS,
        'client.id': WORKER_ID
    }
    
    # Configure Kafka producer for results
    producer_config = {
        'bootstrap.servers': config.KAFKA_BROKER,
        'client.id': f'{WORKER_ID}-producer'
    }
    
    try:
        consumer = Consumer(consumer_config)
        producer = Producer(producer_config)
        
        # Connect to Redis
        redis_client = redis.Redis(
            host=config.REDIS_HOST,
            port=config.REDIS_PORT,
            decode_responses=True
        )
        redis_client.ping()
        
        logger.info(f"✅ Worker {WORKER_ID} connected to Kafka and Redis")
        
        # Subscribe to tasks topic
        consumer.subscribe([config.TASKS_TOPIC])
        logger.info(f"🎧 Worker {WORKER_ID} subscribed to {config.TASKS_TOPIC}")
        
        # Start heartbeat thread
        heartbeat_thread = threading.Thread(
            target=send_heartbeat,
            args=(producer, redis_client),
            daemon=True
        )
        heartbeat_thread.start()
        
        # Main processing loop
        logger.info(f"🚀 Worker {WORKER_ID} ready to process tasks!")
        
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
                # Parse message key: job_id:tile_id:transformation
                key = msg.key().decode('utf-8')
                parts = key.split(':')
                
                if len(parts) != 3:
                    logger.error(f"Invalid message key format: {key}")
                    continue
                
                job_id, tile_id, transformation = parts
                
                logger.info(f"🎨 Worker {WORKER_ID} processing job {job_id}, tile {tile_id}, transformation: {transformation}")
                
                # Decode image tile
                tile_bytes = msg.value()
                nparr = np.frombuffer(tile_bytes, np.uint8)
                tile_img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
                
                if tile_img is None:
                    logger.error(f"Failed to decode tile {tile_id}")
                    continue
                
                # Apply transformation
                processed_tile = apply_transformation(tile_img, transformation)
                
                # Encode processed tile
                _, buffer = cv2.imencode('.jpg', processed_tile, [cv2.IMWRITE_JPEG_QUALITY, config.JPEG_QUALITY])
                processed_bytes = buffer.tobytes()
                
                # Send result back to master
                result_key = f"{job_id}:{tile_id}"
                producer.produce(
                    config.RESULTS_TOPIC,
                    key=result_key.encode('utf-8'),
                    value=processed_bytes
                )
                producer.poll(0)
                
                logger.info(f"✅ Worker {WORKER_ID} completed tile {tile_id} for job {job_id}")
                
            except Exception as e:
                logger.error(f"Error processing task: {e}")
                import traceback
                traceback.print_exc()
    
    except KeyboardInterrupt:
        logger.info(f"⏹️  Worker {WORKER_ID} shutting down...")
    except Exception as e:
        logger.error(f"Fatal error in worker: {e}")
        import traceback
        traceback.print_exc()
    finally:
        consumer.close()
        logger.info(f"👋 Worker {WORKER_ID} stopped")


if __name__ == '__main__':
    # Get worker ID from command line
    if len(sys.argv) < 2:
        print("Usage: python worker.py <worker_id>")
        print("Example: python worker.py shreyas-worker-1")
        sys.exit(1)
    
    WORKER_ID = sys.argv[1]
    
    logger.info("=" * 50)
    logger.info(f"🤖 Starting Worker: {WORKER_ID}")
    logger.info(f"📡 Kafka Broker: {config.KAFKA_BROKER}")
    logger.info(f"💾 Redis: {config.REDIS_HOST}:{config.REDIS_PORT}")
    logger.info(f"🏷️  Consumer Group: {config.CONSUMER_GROUP}")
    logger.info("=" * 50)
    
    # Start processing
    process_tasks()
