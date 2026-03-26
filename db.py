import mysql.connector
from mysql.connector.pooling import MySQLConnectionPool
from config import DB_CONFIG

def setup_database():
    temp = DB_CONFIG.copy()
    db_name = temp.pop("database")

    conn = mysql.connector.connect(**temp)
    cursor = conn.cursor()
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
    conn.commit()
    cursor.close()
    conn.close()

    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS postal_pages (
            id INT AUTO_INCREMENT PRIMARY KEY,
            country VARCHAR(200),
            region VARCHAR(200),
            sub_region VARCHAR(200),
            sub_sub_region VARCHAR(200),
            url VARCHAR(500) UNIQUE,
            status VARCHAR(50) DEFAULT 'pending'
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS postal_data (
            id INT AUTO_INCREMENT PRIMARY KEY,
            country VARCHAR(200),
            region VARCHAR(200),
            sub_region VARCHAR(200),
            sub_sub_region VARCHAR(200),
            area VARCHAR(200),
            pincode VARCHAR(20)
        )
    """)

    conn.commit()
    cursor.close()
    conn.close()

pool = MySQLConnectionPool(pool_name="mypool", pool_size=30, **DB_CONFIG)

def get_db():
    return pool.get_connection()
