from kafka import KafkaConsumer

from block_3.utils import backoff


@backoff(tries=3, sleep=5)
def handle_message(message):
    print(message)


def create_consumer():
    print("Connecting to Kafka brokers")
    consumer = KafkaConsumer(
        "itmo2023_processed",
        group_id="itmo_processed_group",
        bootstrap_servers='localhost:29092',
        auto_offset_reset='earliest',
        enable_auto_commit=True
    )

    for message in consumer:
        handle_message(message)


if __name__ == '__main__':
    create_consumer()
