import sqlite3

DB_PATH = "users.db"


def get_conn():
    return sqlite3.connect(DB_PATH)


def init_users_db():
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            email TEXT UNIQUE NOT NULL,
            password TEXT NOT NULL
        )
    """)
    cursor.execute("""
        INSERT OR IGNORE INTO users (username, email, password)
        VALUES ('admin', 'admin@admin.com', 'password')
    """)
    conn.commit()
    conn.close()


def check_user(login_input, password):
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT * FROM users WHERE (username = ? OR email = ?) AND password = ?",
        (login_input, login_input, password)
    )
    user = cursor.fetchone()
    conn.close()
    return user is not None


def save_user(username, email, password):
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO users (username, email, password) VALUES (?, ?, ?)",
        (username, email, password)
    )
    conn.commit()
    conn.close()
