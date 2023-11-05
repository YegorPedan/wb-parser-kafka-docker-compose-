import requests
import psycopg2
import json

db_params = {
    "host": "localhost",
    "database": "de_db",
    "user": "de_app",
    "password": "de_password",
}

api_url = ("https://search.wb.ru/exactmatch/ru/common/v4/search?&query=куртка&curr=rub&dest=-1257786&regions=80,64,38,"
           "4,115,83,33,68,70,69,30,86,75,40,1,66,48,110,31,22,71,114,"
           "111&resultset=catalog&sort=popular&spp=0&suppressSpellcheck=false&limit=300&page=")

conn = psycopg2.connect(**db_params)
cursor = conn.cursor()

for page in range(1, 11):
    response = requests.get(api_url + str(page))
    try:
        data = response.json()

        metadata = data.get("metadata", {})
        state = data.get("state", {})
        version = data.get("version", {})
        params = data.get("params", {})
        item_data = data.get("data", {})

        cursor.execute("INSERT INTO products (metadata, state, version, params, data) VALUES (%s, %s, %s, %s, %s)",
                       (json.dumps(metadata), json.dumps(state), json.dumps(version), json.dumps(params), json.dumps(item_data)))
        conn.commit()
    except requests.exceptions.JSONDecodeError:
        continue

conn.close()
