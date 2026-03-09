import sqlite3

DB_PATH = "inventory.db"


def get_conn():
    return sqlite3.connect(DB_PATH)


def init_inventory_db():
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS cars (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            vin              TEXT UNIQUE NOT NULL,
            title            TEXT,
            year             INTEGER,
            make             TEXT,
            model            TEXT,
            trim             TEXT,
            price            REAL,
            mileage          INTEGER,
            zip_code         TEXT,
            fuel_type        TEXT,
            cylinders        INTEGER,
            transmission     TEXT,
            drivetrain       TEXT,
            exterior_color   TEXT,
            interior_color   TEXT,
            image_url        TEXT,
            dealer_name      TEXT,
            dealer_url       TEXT,
            scraped_at       TEXT
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS saved_cars (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id   INTEGER NOT NULL,
            car_id    INTEGER NOT NULL,
            saved_at  TEXT DEFAULT (datetime('now')),
            UNIQUE(user_id, car_id)
        )
    """)
    conn.commit()
    conn.close()
    seed_cars()


# (vin, title, year, make, model, trim, price, mileage, zip_code,
#  fuel_type, cylinders, transmission, drivetrain,
#  exterior_color, interior_color, image_url, dealer_name, dealer_url)
SEED_DATA = [
    ("VIN001", "2022 BMW M3 Competition", 2022, "BMW", "M3", "Competition",
     72995, 8200, "28202", "Gasoline", 6, "Automatic", "RWD",
     "Frozen Portimao Blue", "Silverstone Merino",
     "https://images.unsplash.com/photo-1555215695-3004980ad54e?w=800",
     "Hendrick BMW Charlotte", "https://www.hendrickbmwcharlotte.com"),

    ("VIN002", "2023 BMW M5 Competition", 2023, "BMW", "M5", "Competition",
     115900, 3100, "28203", "Gasoline", 8, "Automatic", "AWD",
     "Isle of Man Green", "Black Merino",
     "https://images.unsplash.com/photo-1607853202273-797f1c22a38e?w=800",
     "Hendrick BMW Charlotte", "https://www.hendrickbmwcharlotte.com"),

    ("VIN003", "2022 BMW 550i xDrive", 2022, "BMW", "5 Series", "M550i",
     78400, 14500, "28202", "Gasoline", 8, "Automatic", "AWD",
     "Carbon Black", "Cognac",
     "https://images.unsplash.com/photo-1556189250-72ba954cfc2b?w=800",
     "Hendrick BMW Charlotte", "https://www.hendrickbmwcharlotte.com"),

    ("VIN004", "2021 BMW 740i xDrive", 2021, "BMW", "7 Series", "740i",
     82000, 22000, "28205", "Gasoline", 6, "Automatic", "AWD",
     "Black Sapphire", "Ivory White",
     "https://images.unsplash.com/photo-1619767886558-efdc259b6e09?w=800",
     "Hendrick BMW Charlotte", "https://www.hendrickbmwcharlotte.com"),

    ("VIN005", "2023 Mercedes-Benz C300", 2023, "Mercedes-Benz", "C-Class", "C300",
     48500, 5600, "28203", "Gasoline", 4, "Automatic", "RWD",
     "Polar White", "Black",
     "https://images.unsplash.com/photo-1618843479313-40f8afb4b4d8?w=800",
     "Fletcher Jones Mercedes", "https://www.fletcherjonesmercedes.com"),

    ("VIN006", "2022 Audi Q5 Premium Plus", 2022, "Audi", "Q5", "Premium Plus",
     41200, 18700, "28205", "Gasoline", 4, "Automatic", "AWD",
     "Mythos Black", "Rock Gray",
     "https://images.unsplash.com/photo-1606664515524-ed2f786a0bd6?w=800",
     "Hendrick Audi", "https://www.hendrickaudi.com"),

    ("VIN007", "2023 Porsche 911 Carrera S", 2023, "Porsche", "911", "Carrera S",
     138000, 1200, "28207", "Gasoline", 6, "Automatic", "RWD",
     "GT Silver", "Black",
     "https://images.unsplash.com/photo-1503376780353-7e6692767b70?w=800",
     "Porsche Charlotte", "https://www.porschecharlotte.com"),

    ("VIN008", "2022 Toyota Camry XSE", 2022, "Toyota", "Camry", "XSE",
     31450, 27000, "28208", "Gasoline", 4, "Automatic", "FWD",
     "Midnight Black", "Black",
     "https://images.unsplash.com/photo-1621007947382-bb3c3994e3fb?w=800",
     "Toyota of Charlotte", "https://www.toyotaofcharlotte.com"),

    ("VIN009", "2023 Tesla Model 3 Long Range", 2023, "Tesla", "Model 3", "Long Range",
     47990, 4300, "28212", "Electric", 0, "Automatic", "AWD",
     "Pearl White", "Black",
     "https://images.unsplash.com/photo-1560958089-b8a1929cea89?w=800",
     "Tesla Charlotte", "https://www.tesla.com/findus/location/store/charlotte"),

    ("VIN010", "2022 Lexus RX 350 F Sport", 2022, "Lexus", "RX 350", "F Sport",
     55600, 16400, "28213", "Gasoline", 6, "Automatic", "AWD",
     "Atomic Silver", "Black",
     "https://images.unsplash.com/photo-1519641471654-76ce0107ad1b?w=800",
     "Lexus of Charlotte", "https://www.lexusofcharlotte.com"),

    ("VIN011", "2021 Ford F-150 Lariat", 2021, "Ford", "F-150", "Lariat",
     54900, 31000, "28209", "Gasoline", 8, "Automatic", "4WD",
     "Oxford White", "Medium Dark Slate",
     "https://images.unsplash.com/photo-1558618666-fcd25c85cd64?w=800",
     "Ford of Charlotte", "https://www.fordofcharlotte.com"),

    ("VIN012", "2021 Chevrolet Corvette Stingray", 2021, "Chevrolet", "Corvette", "Stingray",
     67800, 9800, "28210", "Gasoline", 8, "Manual", "RWD",
     "Rapid Blue", "Jet Black",
     "https://images.unsplash.com/photo-1552519507-da3b142c6e3d?w=800",
     "Hendrick Chevrolet", "https://www.hendrickchevrolet.com"),

    ("VIN013", "2022 Honda Accord Sport", 2022, "Honda", "Accord", "Sport",
     29900, 19200, "28211", "Gasoline", 4, "CVT", "FWD",
     "Sonic Gray Pearl", "Black",
     "https://images.unsplash.com/photo-1609521263047-f8f205293f24?w=800",
     "Honda of Charlotte", "https://www.hondaofcharlotte.com"),

    ("VIN014", "2023 BMW 640i Gran Coupe", 2023, "BMW", "6 Series", "640i Gran Coupe",
     89500, 2900, "28202", "Gasoline", 6, "Automatic", "RWD",
     "Mineral White", "Oyster",
     "https://images.unsplash.com/photo-1556189250-72ba954cfc2b?w=800",
     "Hendrick BMW Charlotte", "https://www.hendrickbmwcharlotte.com"),

    ("VIN015", "2022 BMW M550i xDrive", 2022, "BMW", "5 Series", "M550i",
     91200, 11000, "28202", "Gasoline", 8, "Automatic", "AWD",
     "Bernina Grey", "Tartufo",
     "https://images.unsplash.com/photo-1555215695-3004980ad54e?w=800",
     "Hendrick BMW Charlotte", "https://www.hendrickbmwcharlotte.com"),

    # Diesel example — Ram 1500
    ("VIN016", "2022 Ram 1500 Laramie", 2022, "Ram", "1500", "Laramie",
     58900, 21000, "28209", "Diesel", 6, "Automatic", "4WD",
     "Granite Crystal", "Black",
     "https://images.unsplash.com/photo-1622038085247-bbe4a0e98adb?w=800",
     "Charlotte Ram", "https://www.charlotteram.com"),
]


def seed_cars():
    conn = get_conn()
    cursor = conn.cursor()
    cursor.executemany("""
        INSERT OR IGNORE INTO cars
            (vin, title, year, make, model, trim, price, mileage, zip_code,
             fuel_type, cylinders, transmission, drivetrain,
             exterior_color, interior_color, image_url, dealer_name, dealer_url)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, SEED_DATA)
    conn.commit()
    conn.close()


def _placeholders(lst):
    return ", ".join("?" * len(lst))


def search_cars(makes=None, models=None, trims=None, fuel_types=None,
                cylinders=None, transmissions=None, drivetrains=None,
                exterior_colors=None, interior_colors=None,
                max_price=None, max_mileage=None,
                zip_code=None, radius_miles=None):

    from backend.db.geo import zip_to_coords, haversine

    conn = get_conn()
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    query = "SELECT * FROM cars WHERE 1=1"
    params = []

    def add_multi(col, values):
        nonlocal query
        if values:
            query += f" AND {col} IN ({_placeholders(values)})"
            params.extend(values)

    add_multi("make", makes)
    add_multi("model", models)
    add_multi("trim", trims)
    add_multi("fuel_type", fuel_types)
    add_multi("cylinders", [int(c) for c in cylinders] if cylinders else None)
    add_multi("transmission", transmissions)
    add_multi("drivetrain", drivetrains)
    add_multi("exterior_color", exterior_colors)
    add_multi("interior_color", interior_colors)

    if max_price:
        query += " AND price <= ?"
        params.append(max_price)
    if max_mileage:
        query += " AND mileage <= ?"
        params.append(max_mileage)

    cursor.execute(query, params)
    results = [dict(row) for row in cursor.fetchall()]
    conn.close()

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
            return sorted(filtered, key=lambda c: c["distance_miles"])

    return sorted(results, key=lambda c: c["price"])


def get_car_by_id(car_id):
    conn = get_conn()
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM cars WHERE id = ?", (car_id,))
    row = cursor.fetchone()
    conn.close()
    return dict(row) if row else None


def get_filter_options():
    """
    Returns all filter option data with full relationship maps so the
    frontend can do bidirectional cascading across every dimension.
    """
    conn = get_conn()
    cursor = conn.cursor()

    def distinct(col):
        cursor.execute(
            f"SELECT DISTINCT {col} FROM cars WHERE {col} IS NOT NULL ORDER BY {col}"
        )
        return [r[0] for r in cursor.fetchall()]

    fuel_types      = distinct("fuel_type")
    cylinders       = distinct("cylinders")
    transmissions   = distinct("transmission")
    drivetrains     = distinct("drivetrain")
    exterior_colors = distinct("exterior_color")
    interior_colors = distinct("interior_color")

    # Full relationship rows — every unique combo of all filterable dims.
    # The frontend embeds these as data-* on each checkbox so it can filter
    # any dropdown based on any combination of other active filters.
    cursor.execute("""
        SELECT DISTINCT make, model, trim, fuel_type, cylinders, drivetrain
        FROM cars
        WHERE make IS NOT NULL
        ORDER BY make, model, trim
    """)
    car_rows = cursor.fetchall()  # (make, model, trim, fuel_type, cylinders, drivetrain)

    conn.close()

    # Derive distinct makes/models/trims preserving order
    seen_makes  = []
    seen_models = []  # (make, model)
    seen_trims  = []  # (make, model, trim)
    for row in car_rows:
        make, model, trim = row[0], row[1], row[2]
        if make not in seen_makes:
            seen_makes.append(make)
        if (make, model) not in seen_models:
            seen_models.append((make, model))
        if (make, model, trim) not in seen_trims:
            seen_trims.append((make, model, trim))

    # Full per-car data for client-side live filtering and rendering
    conn2 = get_conn()
    conn2.row_factory = sqlite3.Row
    all_cars_cursor = conn2.cursor()
    all_cars_cursor.execute(
        "SELECT * FROM cars ORDER BY price ASC"
    )
    all_cars = [dict(r) for r in all_cars_cursor.fetchall()]
    conn2.close()

    return {
        "makes":           seen_makes,
        "model_rows":      seen_models,
        "trim_rows":       seen_trims,
        "fuel_types":      fuel_types,
        "cylinders":       cylinders,
        "transmissions":   transmissions,
        "drivetrains":     drivetrains,
        "exterior_colors": exterior_colors,
        "interior_colors": interior_colors,
        # Full relationship table for cascade engine
        "car_rows":        [
            {"make": r[0], "model": r[1], "trim": r[2],
             "fuel": r[3], "cyl": r[4], "drive": r[5]}
            for r in car_rows
        ],
        # Complete car objects for client-side live rendering
        "all_cars":        all_cars,
    }
