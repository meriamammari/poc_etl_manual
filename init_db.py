import pg8000.legacy as pg

try:
    conn = pg.connect('postgres', host='localhost', password='mimou88!!AA')
    cursor = conn.cursor()

    try:
        cursor.execute("DROP USER IF EXISTS etl_user")
    except:
        pass

    # Create user with password
    cursor.execute("CREATE USER etl_user WITH PASSWORD 'etl_pass'")
    print("✓ Created user: etl_user")

    # Create database
    try:
        cursor.execute("CREATE DATABASE etl_db OWNER etl_user ENCODING 'UTF8'")
        print("✓ Created database: etl_db")
    except Exception as e:
        print(f"⚠ Database issue: {e}")

    conn.commit()
    conn.close()
    
    # Verify connection works
    test_conn = pg.connect('etl_user', host='localhost', password='etl_pass', database='etl_db')
    print("✓ Verified: etl_user can connect to etl_db")
    test_conn.close()
    
    print("\n✓ Setup Complete!")
except Exception as e:
    print(f"✗ Error: {e}")
