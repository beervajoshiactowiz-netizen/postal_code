import mysql.connector
from mysql.connector import pooling


pool = pooling.MySQLConnectionPool(
    pool_name="mypool",
    pool_size=5,
    host="localhost",
    user="root",
    password="actowiz",
    database="postalFinal_db"
)


def get_conn():
    return pool.get_connection()


def setup_database():
    conn = get_conn()
    cursor = conn.cursor()

    # countries
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS countries (
            id INT AUTO_INCREMENT PRIMARY KEY,
            country VARCHAR(200),
            url VARCHAR(500) UNIQUE,
            status VARCHAR(50)
        )
    """)

    # regions
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS regions (
            id INT AUTO_INCREMENT PRIMARY KEY,
            country VARCHAR(200),
            region VARCHAR(200),
            url VARCHAR(500) UNIQUE,
            status VARCHAR(50)
        )
    """)

    # sub_regions
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sub_regions (
            id INT AUTO_INCREMENT PRIMARY KEY,
            country VARCHAR(200),
            region VARCHAR(200),
            sub_region VARCHAR(200),
            url VARCHAR(500) UNIQUE,
            status VARCHAR(50)
        )
    """)

    # sub_sub_regions
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sub_sub_regions (
            id INT AUTO_INCREMENT PRIMARY KEY,
            country VARCHAR(200),
            region VARCHAR(200),
            sub_region VARCHAR(200),
            sub_sub_region VARCHAR(200),
            url VARCHAR(500) UNIQUE,
            status VARCHAR(50)
        )
    """)

    # postal pages
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS postal_pages (
            id INT AUTO_INCREMENT PRIMARY KEY,
            country VARCHAR(200),
            region VARCHAR(200),
            sub_region VARCHAR(200),
            sub_sub_region VARCHAR(200),
            url VARCHAR(500) UNIQUE,
            status VARCHAR(50)
        )
    """)

    #postal data
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS postal_data (
            id INT AUTO_INCREMENT PRIMARY KEY,
            country VARCHAR(200),
            region VARCHAR(200),
            sub_region VARCHAR(200),
            sub_sub_region VARCHAR(200),
            area VARCHAR(255),
            pincode VARCHAR(50),
            UNIQUE KEY ( area, pincode)
        )
        """)

    conn.commit()
    cursor.close()
    conn.close()

def get_postal_urls(limit=200):
    conn = get_conn()
    cursor = conn.cursor()

    cursor.execute("""
    SELECT country, region, sub_region, sub_sub_region, url
    FROM postal_pages
    WHERE status='pending'
    LIMIT %s
    """, (limit,))

    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return rows

# def update_postal_status(url):
#     conn = get_conn()
#     cursor = conn.cursor()
#
#     cursor.execute("""
#     UPDATE postal_pages
#     SET status='done'
#     WHERE url=%s
#     """, (url,))
#
#     conn.commit()
#     cursor.close()
#     conn.close()
def update_postal_status(urls):

    conn   = get_conn()
    cursor = conn.cursor()
    cursor.executemany(
        "UPDATE postal_pages SET status='done' WHERE url=%s",
        [(u,) for u in urls]
    )
    conn.commit()
    cursor.close()
    conn.close()

def insert_countries(data):
    conn = get_conn()
    cursor = conn.cursor()

    cursor.executemany("""
        INSERT IGNORE INTO countries (country, url, status)
        VALUES (%s, %s, %s)
    """, data)

    conn.commit()
    cursor.close()
    conn.close()


def insert_regions(data):
    conn = get_conn()
    cursor = conn.cursor()

    cursor.executemany("""
        INSERT IGNORE INTO regions (country, region, url, status)
        VALUES (%s, %s, %s, %s)
    """, data)

    conn.commit()
    cursor.close()
    conn.close()


def insert_subregions(data):
    conn = get_conn()
    cursor = conn.cursor()

    cursor.executemany("""
        INSERT IGNORE INTO sub_regions
        (country, region, sub_region, url, status)
        VALUES (%s, %s, %s, %s, %s)
    """, data)

    conn.commit()
    cursor.close()
    conn.close()


def insert_sub_subregions(data):
    conn = get_conn()
    cursor = conn.cursor()

    cursor.executemany("""
        INSERT IGNORE INTO sub_sub_regions
        (country, region, sub_region, sub_sub_region, url, status)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, data)

    conn.commit()
    cursor.close()
    conn.close()


def insert_postal(data):
    conn = get_conn()
    cursor = conn.cursor()

    cursor.executemany("""
        INSERT IGNORE INTO postal_pages
        (country, region, sub_region, sub_sub_region, url, status)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, data)

    conn.commit()
    cursor.close()
    conn.close()

def insert_postal_data(data):
    conn = get_conn()
    cursor = conn.cursor()

    cursor.executemany("""
    INSERT IGNORE INTO postal_data
    (country, region, sub_region, sub_sub_region, area, pincode)
    VALUES (%s,%s,%s,%s,%s,%s)
    """, data)

    conn.commit()
    cursor.close()
    conn.close()
