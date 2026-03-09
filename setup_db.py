import pg8000.legacy as pg

try:
    # Connect as postgres superuser
    conn = pg.connect('postgres', host='localhost', password='mimou88!!AA')
    cursor = conn.cursor()
    
    # Create etl_user if it doesn't exist
    try:
        cursor.execute("CREATE USER etl_user WITH PASSWORD 'etl_pass'")
        print("✓ Created user: etl_user")
    except Exception as e:
        print(f"⚠ User etl_user may already exist")
    
    # Create etl_db if it doesn't exist
    try:
        cursor.execute("CREATE DATABASE etl_db OWNER etl_user")
        print("✓ Created database: etl_db")
    except Exception as e:
        print(f"⚠ Database etl_db may already exist")
    
    # Grant privileges
    cursor.execute("GRANT ALL PRIVILEGES ON DATABASE etl_db TO etl_user")
    print("✓ Granted privileges to etl_user on etl_db")
    
    conn.commit()
    conn.close()
    print("\n✓ Database setup complete!")
    
except Exception as e:
    print(f"✗ Error: {e}")
