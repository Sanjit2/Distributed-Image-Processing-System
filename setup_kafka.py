from confluent_kafka.admin import AdminClient, NewTopic
import logging

import config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_topics():
    """
    Create required Kafka topics
    """
    admin_client = AdminClient({
        'bootstrap.servers': config.KAFKA_BROKER
    })
    
    topics = [
        NewTopic(
            config.TASKS_TOPIC,
            num_partitions=2,  
            replication_factor=1
        ),
        NewTopic(
            config.RESULTS_TOPIC,
            num_partitions=1,
            replication_factor=1
        ),
        NewTopic(
            config.HEARTBEAT_TOPIC,
            num_partitions=1,
            replication_factor=1
        )
    ]
    
    fs = admin_client.create_topics(topics)
    
    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
            logger.info(f"✅ Topic '{topic}' created successfully")
        except Exception as e:
            if 'TopicExistsError' in str(e):
                logger.info(f"ℹ️  Topic '{topic}' already exists")
            else:
                logger.error(f"❌ Failed to create topic '{topic}': {e}")


def list_topics():
    """
    List all existing topics
    """
    admin_client = AdminClient({
        'bootstrap.servers': config.KAFKA_BROKER
    })
    
    metadata = admin_client.list_topics(timeout=10)
    
    logger.info("\n📋 Existing topics:")
    for topic in metadata.topics:
        logger.info(f"   - {topic}")


if __name__ == '__main__':
    logger.info("=" * 50)
    logger.info("🔧 Kafka Topic Setup")
    logger.info(f"📡 Kafka Broker: {config.KAFKA_BROKER}")
    logger.info("=" * 50)
    
    try:
        logger.info("\n🚀 Creating topics...")
        create_topics()
        
        logger.info("\n📋 Listing all topics...")
        list_topics()
        
        logger.info("\n✅ Setup complete!")
        
    except Exception as e:
        logger.error(f"❌ Setup failed: {e}")
        import traceback
        traceback.print_exc()
