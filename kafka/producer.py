from confluent_kafka import Producer
import psycopg2

kafka_params = {
    "bootstrap.servers": "localhost:9092",
}

db_params = {
    "host": "localhost",
    "database": "de_db",
    "user": "de_app",
    "password": "de_password",
}

conn = psycopg2.connect(**db_params)
cursor = conn.cursor()

producer = Producer(kafka_params)

cursor.execute("SELECT id, data->>'productId' FROM products")
rows = cursor.fetchall()

for row in rows:
    id, product_id = row
    producer.produce("products", key=str(id), value=product_id)

conn.commit()
conn.close()

producer.flush()
