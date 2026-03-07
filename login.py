import sqlite3

def check_user(login_input, password):

    conn = sqlite3.connect("users.db")

    cursor = conn.cursor()

    cursor.execute(
        "SELECT * FROM users WHERE( username = ? OR password = ?)AND password = ?",
        (login_input, login_input, password)
    )

    user = cursor.fetchone()

    conn.close()

    if user:
        return True
    return False