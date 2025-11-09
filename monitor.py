"""
Monitor Service for Worker Health Tracking
Can run on any VM - monitors worker heartbeats
"""

import json
import time
import logging
from confluent_kafka import Consumer, KafkaError
import redis

import config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def monitor_heartbeats():
    """
    Monitor worker heartbeats from Kafka and update Redis
    """
    # Configure Kafka consumer
    consumer_config = {
        'bootstrap.servers': config.KAFKA_BROKER,
        'group.id': 'heartbeat-monitor',
        'auto.offset.reset': 'latest',  # Only monitor new heartbeats
        'enable.auto.commit': True
    }
    
    try:
        consumer = Consumer(consumer_config)
        
        # Connect to Redis
        redis_client = redis.Redis(
            host=config.REDIS_HOST,
            port=config.REDIS_PORT,
            decode_responses=True
        )
        redis_client.ping()
        
        logger.info(f"✅ Monitor connected to Kafka and Redis")
        
        # Subscribe to heartbeat topic
        consumer.subscribe([config.HEARTBEAT_TOPIC])
        logger.info(f"🎧 Monitoring heartbeats on {config.HEARTBEAT_TOPIC}")
        
        logger.info("🔍 Monitor service started!")
        
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
                # Parse heartbeat message
                worker_id = msg.key().decode('utf-8')
                heartbeat_data = json.loads(msg.value().decode('utf-8'))
                
                status = heartbeat_data.get('status', 'unknown')
                timestamp = heartbeat_data.get('timestamp', time.time())
                
                # Update Redis with worker status (with TTL)
                worker_key = config.get_worker_key(worker_id)
                redis_client.setex(worker_key, config.WORKER_TIMEOUT, status)
                
                logger.info(f"💓 Heartbeat received from {worker_id} - status: {status}")
                
            except Exception as e:
                logger.error(f"Error processing heartbeat: {e}")
    
    except KeyboardInterrupt:
        logger.info("⏹️  Monitor shutting down...")
    except Exception as e:
        logger.error(f"Fatal error in monitor: {e}")
        import traceback
        traceback.print_exc()
    finally:
        consumer.close()
        logger.info("👋 Monitor stopped")


def check_worker_status():
    """
    Periodically check and display worker status from Redis
    """
    try:
        redis_client = redis.Redis(
            host=config.REDIS_HOST,
            port=config.REDIS_PORT,
            decode_responses=True
        )
        
        while True:
            time.sleep(10)  # Check every 10 seconds
            
            # Scan for worker keys
            workers = []
            for key in redis_client.scan_iter("worker:*"):
                worker_id = key.split(':')[1]
                ttl = redis_client.ttl(key)
                
                if ttl > 0:
                    workers.append({
                        'worker_id': worker_id,
                        'ttl': ttl
                    })
            
            if workers:
                logger.info(f"🟢 Active workers: {len(workers)}")
                for worker in workers:
                    logger.info(f"   - {worker['worker_id']} (TTL: {worker['ttl']}s)")
            else:
                logger.warning("🔴 No active workers found")
                
    except KeyboardInterrupt:
        pass
    except Exception as e:
        logger.error(f"Error checking worker status: {e}")


if __name__ == '__main__':
    logger.info("=" * 50)
    logger.info("🔍 Starting Monitor Service")
    logger.info(f"📡 Kafka Broker: {config.KAFKA_BROKER}")
    logger.info(f"💾 Redis: {config.REDIS_HOST}:{config.REDIS_PORT}")
    logger.info(f"⏱️  Worker Timeout: {config.WORKER_TIMEOUT}s")
    logger.info("=" * 50)
    
    # Start monitoring
    monitor_heartbeats()
