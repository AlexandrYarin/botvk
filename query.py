import psycopg2 as db
import json
href = '/home/ya/Документы/Projects/vkproject/support/'

with open(href + 'config.json', 'r') as file: conf = json.load(file)
co_data = conf['connect'] 

#connect with server
conn_string = f'dbname={co_data["base"]} host={co_data["host"]} \
user={co_data["user"]} password={co_data["password"]}'

conn = db.connect(conn_string)
cur = conn.cursor()

def query():
    query = 'select post,links,tag from motorsport where is_watch is null  order by date asc'
    cur.execute(query)

    data = cur.fetchall()
    cur.close()

    with open(href + 'inter.json', 'w') as file:
        json.dump({"data":data}, file)    

if __name__ == "__main__":
    query()
