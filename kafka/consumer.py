from confluent_kafka import Consumer, KafkaError
import psycopg2
import datetime

kafka_params = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "my_group",
    "auto.offset.reset": "earliest",
}

db_params = {
    "host": "localhost",
    "database": "de_db",
    "user": "de_app",
    "password": "de_password",
}

consumer = Consumer(kafka_params)

consumer.subscribe(["products"])

while True:
    msg = consumer.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            print("Reached end of partition")
        else:
            print("Error: {}".format(msg.error()))
    else:
        product_id = msg.value()

        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()

        cursor.execute("UPDATE products SET processed_at = %s WHERE data->>'productId' = %s", (datetime.datetime.now(), product_id))

        if cursor.rowcount == 0:
            consumer.nack(msg)
        else:
            consumer.commit()

        conn.commit()
        conn.close()
