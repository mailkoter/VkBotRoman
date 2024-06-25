import psycopg2

db_params = {
    'host': '66.151.32.243',
    'port': 5432,
    'dbname': 'social_monitor',
    'user': 'monadmin',
    'password': 'bigdata'
}

try:
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public';
    """)
    tables = cursor.fetchall()
    print("Список таблиц:")
    for table in tables:
        print(table[0])
    
    table_name = 'posts'
    
    cursor.execute(f"SELECT * FROM {table_name} LIMIT 10;")
    rows = cursor.fetchall()
    
    print(f"\nСодержимое таблицы '{table_name}':")
    for row in rows:
        print(row)
    
except Exception as e:
    print(f"Ошибка: {e}")
finally:
    if cursor:
        cursor.close()
    if conn:
        conn.close()