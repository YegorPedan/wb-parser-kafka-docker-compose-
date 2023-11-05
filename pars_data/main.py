import requests
import psycopg2
import json

db_params = {
    "host": "localhost",
    "database": "de_db",
    "user": "de_app",
    "password": "de_password",
}

api_url = "https://search.wb.ru/exactmatch/ru/common/v4/search?&query=куртка&curr=rub&dest=-1257786&regions=80,64,38,4,115,83,33,68,70,69,30,86,75,40,1,66,48,110,31,22,71,114,111&resultset=catalog&sort=popular&spp=0&suppressSpellcheck=false&limit=300&page="

conn = psycopg2.connect(**db_params)
cursor = conn.cursor()

for page in range(1, 11):
    response = requests.get(api_url + str(page))
    data = response.json()

    # Запись данных в базу данных
    for item in data:
        cursor.execute("INSERT INTO products (metadata, state, version, params, data) VALUES (%s, %s, %s, %s, %s)",
                       (json.dumps(item.get("metadata")), json.dumps(item.get("state")), json.dumps(item.get("version")), json.dumps(item.get("params")), json.dumps(item.get("data"))))

conn.commit()
conn.close()
