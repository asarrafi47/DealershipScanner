import sqlite3

DB_PATH = "inventory.db"


def get_conn():
    return sqlite3.connect(DB_PATH)


def init_inventory_db():
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS cars (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            vin TEXT UNIQUE NOT NULL,
            title TEXT,
            year INTEGER,
            make TEXT,
            model TEXT,
            price REAL,
            zip_code TEXT,
            image_url TEXT,
            dealer_name TEXT,
            dealer_url TEXT,
            scraped_at TEXT
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS saved_cars (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            car_id INTEGER NOT NULL,
            saved_at TEXT DEFAULT (datetime('now')),
            UNIQUE(user_id, car_id)
        )
    """)
    conn.commit()
    conn.close()
    seed_cars()


SEED_DATA = [
    ("VIN001", "2022 BMW M3 Competition", 2022, "BMW", "M3 Competition", 72995, "28202",
     "https://images.unsplash.com/photo-1555215695-3004980ad54e?w=800",
     "Hendrick BMW Charlotte", "https://www.hendrickbmwcharlotte.com"),
    ("VIN002", "2023 Mercedes-Benz C300", 2023, "Mercedes-Benz", "C300", 48500, "28203",
     "https://images.unsplash.com/photo-1618843479313-40f8afb4b4d8?w=800",
     "Fletcher Jones Mercedes", "https://www.fletcherjonesmercedes.com"),
    ("VIN003", "2021 Audi Q5 Premium Plus", 2021, "Audi", "Q5", 41200, "28205",
     "https://images.unsplash.com/photo-1606664515524-ed2f786a0bd6?w=800",
     "Hendrick Audi", "https://www.hendrickaudi.com"),
    ("VIN004", "2023 Porsche 911 Carrera", 2023, "Porsche", "911 Carrera", 115000, "28207",
     "https://images.unsplash.com/photo-1503376780353-7e6692767b70?w=800",
     "Porsche Charlotte", "https://www.porschecharlotte.com"),
    ("VIN005", "2022 Toyota Camry XSE", 2022, "Toyota", "Camry XSE", 31450, "28208",
     "https://images.unsplash.com/photo-1621007947382-bb3c3994e3fb?w=800",
     "Toyota of Charlotte", "https://www.toyotaofcharlotte.com"),
    ("VIN006", "2023 Ford F-150 Lariat", 2023, "Ford", "F-150 Lariat", 54900, "28209",
     "https://images.unsplash.com/photo-1558618666-fcd25c85cd64?w=800",
     "Ford of Charlotte", "https://www.fordofcharlotte.com"),
    ("VIN007", "2021 Chevrolet Corvette Stingray", 2021, "Chevrolet", "Corvette Stingray", 67800, "28210",
     "https://images.unsplash.com/photo-1552519507-da3b142c6e3d?w=800",
     "Hendrick Chevrolet", "https://www.hendrickchevrolet.com"),
    ("VIN008", "2022 Honda Accord Sport", 2022, "Honda", "Accord Sport", 29900, "28211",
     "https://images.unsplash.com/photo-1609521263047-f8f205293f24?w=800",
     "Honda of Charlotte", "https://www.hondaofcharlotte.com"),
    ("VIN009", "2023 Tesla Model 3 Long Range", 2023, "Tesla", "Model 3", 47990, "28212",
     "https://images.unsplash.com/photo-1560958089-b8a1929cea89?w=800",
     "Tesla Charlotte", "https://www.tesla.com/findus/location/store/charlotte"),
    ("VIN010", "2022 Lexus RX 350 F Sport", 2022, "Lexus", "RX 350", 55600, "28213",
     "https://images.unsplash.com/photo-1519641471654-76ce0107ad1b?w=800",
     "Lexus of Charlotte", "https://www.lexusofcharlotte.com"),
]


def seed_cars():
    conn = get_conn()
    cursor = conn.cursor()
    cursor.executemany("""
        INSERT OR IGNORE INTO cars
            (vin, title, year, make, model, price, zip_code, image_url, dealer_name, dealer_url)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, SEED_DATA)
    conn.commit()
    conn.close()


def search_cars(make=None, model=None, max_price=None, zip_code=None, radius_miles=None):
    from backend.db.geo import zip_to_coords, haversine

    conn = get_conn()
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    query = "SELECT * FROM cars WHERE 1=1"
    params = []

    if make:
        query += " AND make LIKE ?"
        params.append(f"%{make}%")
    if model:
        query += " AND model LIKE ?"
        params.append(f"%{model}%")
    if max_price:
        query += " AND price <= ?"
        params.append(max_price)

    cursor.execute(query, params)
    results = [dict(row) for row in cursor.fetchall()]
    conn.close()

    # Apply radius filter in Python after SQL fetch
    if zip_code and radius_miles:
        origin = zip_to_coords(zip_code)
        if origin:
            filtered = []
            for car in results:
                dest = zip_to_coords(car.get("zip_code", ""))
                if dest:
                    dist = haversine(origin[0], origin[1], dest[0], dest[1])
                    if dist <= radius_miles:
                        car["distance_miles"] = round(dist, 1)
                        filtered.append(car)
            results = sorted(filtered, key=lambda c: c["distance_miles"])
        # If origin zip couldn't be resolved, fall back to unfiltered results
    else:
        results = sorted(results, key=lambda c: c["price"])

    return results


def get_car_by_id(car_id):
    conn = get_conn()
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM cars WHERE id = ?", (car_id,))
    row = cursor.fetchone()
    conn.close()
    return dict(row) if row else None
