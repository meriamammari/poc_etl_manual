import pg8000.legacy as pg

conn = pg.connect(user='etl_user', host='localhost', password='etl_pass', database='etl_db')
cursor = conn.cursor()
cursor.execute("SELECT table_schema, table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE'")
for row in cursor.fetchall():
    print(row)
conn.close()