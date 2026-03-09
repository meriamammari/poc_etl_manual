import pg8000.legacy as pg

conn = pg.connect('postgres', host='localhost', password='mimou88!!AA')
conn.autocommit = True
cursor = conn.cursor()

# Grant privileges on existing database
cursor.execute("GRANT ALL PRIVILEGES ON DATABASE etl_db TO etl_user")
print("✓ Privileges granted")

conn.close()
print("\n✓ All done! Now run: python pipeline/main.py")