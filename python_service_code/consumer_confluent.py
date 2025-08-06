from confluent_kafka import Consumer, KafkaException
import psycopg2
import json
import uuid

def start_consumer():
    # Kafka config
    kafka_conf = {
        'bootstrap.servers': 'kafka-nodeport:9092',
        'group.id': 'my-consumer-group',
        'auto.offset.reset': 'earliest'
    }

    # PostgreSQL config
    pg_conf = {
        'host': '103.82.23.88',
        'port': 30032,
        'dbname': 'test_database',
        'user': 'myuser',
        'password': 'mypassword'
    }

    # Connect to PostgreSQL
    conn = psycopg2.connect(**pg_conf)
    cursor = conn.cursor()

    # Ensure table exists
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS public.wp_posts (
        id UUID PRIMARY KEY,
        title TEXT NOT NULL,
        description TEXT
    )
    """)
    conn.commit()

    # Connect to Kafka
    consumer = Consumer(kafka_conf)
    consumer.subscribe(['wp-posts'])

    print("🚀 Listening for messages on topic 'wp-posts'...")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())

            # Parse Kafka message
            data = json.loads(msg.value().decode('utf-8'))
            try:
                uuid_id = str(uuid.uuid4())
                cursor.execute(
                    "INSERT INTO public.wp_posts (id, title, description) VALUES (%s, %s, %s) ON CONFLICT (id) DO NOTHING",
                    (uuid_id, data['title']['rendered'], data['content']['rendered'])
                )
                conn.commit()
                print(f"✅ Inserted post: {data['id']}")
            except Exception as e:
                conn.rollback()
                print(f"❌ Failed to insert: {e}")

    except KeyboardInterrupt:
        print("🛑 Stopping consumer...")
    finally:
        consumer.close()
        cursor.close()
        conn.close()
