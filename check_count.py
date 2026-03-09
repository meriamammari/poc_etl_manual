
from dotenv import load_dotenv
import os
load_dotenv('../.env')
import pg8000.legacy as pg
conn = pg.connect(user='etl_user', host='localhost', password='etl_pass', database='etl_db')
cursor = conn.cursor()
cursor.execute('SELECT COUNT(*) FROM manual.crypto_market_snapshot')
print('Count:', cursor.fetchone())
conn.close()
