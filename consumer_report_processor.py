# # consumer_report_processor.py
# import logging
# import json
# import time
# from kafka import KafkaConsumer
# from kafka.errors import KafkaError

# logging.basicConfig(
#     level=logging.INFO,
#     format='%(asctime)s - %(levelname)s - %(consumer)s - %(message)s',
#     datefmt='%Y-%m-%d %H:%M:%S'
# )

# BOOTSTRAP = "kafka:9092"
# TOPIC = "stock-reports"
# GROUP_ID = "report-processor-v1"

# def process_report(data):
#     """This is where you decide what to do with each report"""
#     ts = data.get('timestamp', 'unknown')
#     summary = data.get('summary', '')
#     logging.info(f"New report received at {ts}")
#     logging.info(f"Summary excerpt: {summary[:200]}...")

#     # Possible future actions:
#     # 1. Save full report to another table / file
#     # 2. If alerts → send Telegram / Pushover
#     # 3. Update Grafana annotation
#     # 4. Trigger Airflow DAG via API
#     # 5. etc.


# def main():
#     while True:
#         consumer = None
#         try:
#             consumer = KafkaConsumer(
#                 TOPIC,
#                 bootstrap_servers=BOOTSTRAP,
#                 group_id=GROUP_ID,
#                 auto_offset_reset='earliest',
#                 enable_auto_commit=True,
#                 value_deserializer=lambda x: json.loads(x.decode('utf-8')),
#                 consumer_timeout_ms=30000,
#             )
#             logging.info(f"Consumer connected to {BOOTSTRAP} / group {GROUP_ID}")

#             for msg in consumer:
#                 try:
#                     process_report(msg.value)
#                 except Exception as e:
#                     logging.error(f"Error processing message offset {msg.offset}: {e}")

#         except KafkaError as e:
#             logging.error(f"Kafka connection error: {e}")
#             if consumer:
#                 consumer.close()
#             time.sleep(10)
#         except KeyboardInterrupt:
#             logging.info("Shutting down consumer...")
#             break
#         finally:
#             if consumer:
#                 consumer.close()


# if __name__ == "__main__":
#     main()
# consumer_report_processor.py
import logging
import json
import time
from kafka import KafkaConsumer
from kafka.errors import KafkaError

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

BOOTSTRAP = "kafka:9092"
TOPIC = "stock-reports"
GROUP_ID = "report-processor-v1"

def process_report(data):
    """This is where you decide what to do with each report"""
    ts = data.get('timestamp', 'unknown')
    summary = data.get('summary', '')
    logging.info(f"New report received at {ts}")
    logging.info(f"Summary excerpt: {summary[:200]}...")

    # Possible future actions:
    # 1. Save full report to another table / file
    # 2. If alerts → send Telegram / Pushover
    # 3. Update Grafana annotation
    # 4. Trigger Airflow DAG via API
    # 5. etc.


def main():
    while True:
        consumer = None
        try:
            consumer = KafkaConsumer(
                TOPIC,
                bootstrap_servers=BOOTSTRAP,
                group_id=GROUP_ID,
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                consumer_timeout_ms=30000,
            )
            logging.info(f"Consumer connected to {BOOTSTRAP} / group {GROUP_ID}")

            for msg in consumer:
                try:
                    process_report(msg.value)
                except Exception as e:
                    logging.error(f"Error processing message offset {msg.offset}: {e}")

        except KafkaError as e:
            logging.error(f"Kafka connection error: {e}")
            if consumer:
                consumer.close()
            time.sleep(10)
        except KeyboardInterrupt:
            logging.info("Shutting down consumer...")
            break
        finally:
            if consumer:
                consumer.close()


if __name__ == "__main__":
    main()