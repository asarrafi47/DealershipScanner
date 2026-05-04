#!/usr/bin/env python3
"""
Populate model_specs dictionary with transmission, cylinders, drivetrain data.

This builds a reference table so we can backfill missing specs in car listings.
Run this once to populate, then use reference_model_specs.py to apply the data.

Usage:
  python populate_model_specs.py
"""
import os
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
os.chdir(ROOT)
sys.path.insert(0, str(ROOT))

# Model specs dictionary: make -> model -> {transmission, cylinders, drivetrain}
MODEL_SPECS = {
    # Mazda
    "Mazda": {
        "CX-30": {"transmission": "Automatic", "cylinders": 4, "drivetrain": "Front-Wheel Drive"},
        "CX-5": {"transmission": "Automatic", "cylinders": 4, "drivetrain": "All-Wheel Drive"},
        "CX-9": {"transmission": "Automatic", "cylinders": 6, "drivetrain": "All-Wheel Drive"},
        "Mazda3": {"transmission": "Automatic", "cylinders": 4, "drivetrain": "Front-Wheel Drive"},
    },
    # BMW
    "BMW": {
        "330i": {"transmission": "8-Speed Automatic", "cylinders": 4, "drivetrain": "Rear-Wheel Drive"},
        "330e": {"transmission": "8-Speed Automatic", "cylinders": 4, "drivetrain": "Rear-Wheel Drive"},
        "340i": {"transmission": "8-Speed Automatic", "cylinders": 6, "drivetrain": "Rear-Wheel Drive"},
        "430i": {"transmission": "8-Speed Automatic", "cylinders": 4, "drivetrain": "Rear-Wheel Drive"},
        "430I": {"transmission": "8-Speed Automatic", "cylinders": 4, "drivetrain": "Rear-Wheel Drive"},
        "530i": {"transmission": "8-Speed Automatic", "cylinders": 4, "drivetrain": "Rear-Wheel Drive"},
        "530e": {"transmission": "8-Speed Automatic", "cylinders": 4, "drivetrain": "Rear-Wheel Drive"},
        "540i": {"transmission": "8-Speed Automatic", "cylinders": 6, "drivetrain": "Rear-Wheel Drive"},
        "740i": {"transmission": "8-Speed Automatic", "cylinders": 6, "drivetrain": "Rear-Wheel Drive"},
        "840i": {"transmission": "8-Speed Automatic", "cylinders": 6, "drivetrain": "Rear-Wheel Drive"},
        "X1": {"transmission": "8-Speed Automatic", "cylinders": 4, "drivetrain": "All-Wheel Drive"},
        "X3": {"transmission": "8-Speed Automatic", "cylinders": 4, "drivetrain": "All-Wheel Drive"},
        "X5": {"transmission": "8-Speed Automatic", "cylinders": 6, "drivetrain": "All-Wheel Drive"},
        "X5 PHEV": {"transmission": "8-Speed Automatic", "cylinders": 6, "drivetrain": "All-Wheel Drive"},
        "X7": {"transmission": "8-Speed Automatic", "cylinders": 6, "drivetrain": "All-Wheel Drive"},
        "i4": {"transmission": "Single-speed automatic", "cylinders": 0, "drivetrain": "Rear-Wheel Drive"},
        "i7": {"transmission": "Single-speed automatic", "cylinders": 0, "drivetrain": "Rear-Wheel Drive"},
        "iX": {"transmission": "Single-speed automatic", "cylinders": 0, "drivetrain": "All-Wheel Drive"},
        "M4": {"transmission": "8-Speed Automatic", "cylinders": 6, "drivetrain": "Rear-Wheel Drive"},
        "M235i": {"transmission": "8-Speed Automatic", "cylinders": 4, "drivetrain": "All-Wheel Drive"},
    },
    # Jeep
    "Jeep": {
        "Wrangler": {"transmission": "8-Speed Automatic", "cylinders": 6, "drivetrain": "Four-Wheel Drive"},
        "Grand Cherokee": {"transmission": "8-Speed Automatic", "cylinders": 6, "drivetrain": "All-Wheel Drive"},
        "Grand Cherokee L": {"transmission": "8-Speed Automatic", "cylinders": 6, "drivetrain": "All-Wheel Drive"},
        "Compass": {"transmission": "Continuously Variable Transmission (CVT)", "cylinders": 4, "drivetrain": "Front-Wheel Drive"},
        "Cherokee": {"transmission": "9-Speed Automatic", "cylinders": 4, "drivetrain": "Front-Wheel Drive"},
        "Gladiator": {"transmission": "8-Speed Automatic", "cylinders": 6, "drivetrain": "Four-Wheel Drive"},
    },
    # Ford
    "Ford": {
        "Maverick": {"transmission": "10-Speed Automatic", "cylinders": 4, "drivetrain": "Rear-Wheel Drive"},
        "Bronco Sport": {"transmission": "8-Speed Automatic", "cylinders": 4, "drivetrain": "All-Wheel Drive"},
        "Ranger": {"transmission": "10-Speed Automatic", "cylinders": 4, "drivetrain": "Four-Wheel Drive"},
        "F-150": {"transmission": "10-Speed Automatic", "cylinders": 6, "drivetrain": "Four-Wheel Drive"},
        "Escape": {"transmission": "8-Speed Automatic", "cylinders": 4, "drivetrain": "All-Wheel Drive"},
        "Expedition MAX": {"transmission": "10-Speed Automatic", "cylinders": 6, "drivetrain": "Four-Wheel Drive"},
        "Explorer": {"transmission": "10-Speed Automatic", "cylinders": 6, "drivetrain": "All-Wheel Drive"},
        "Mustang": {"transmission": "10-Speed Automatic", "cylinders": 6, "drivetrain": "Rear-Wheel Drive"},
        "Bronco": {"transmission": "7-Speed Manual/Standard", "cylinders": 6, "drivetrain": "Four-Wheel Drive"},
        "Fusion Energi": {"transmission": "Continuously Variable Transmission (CVT)", "cylinders": 4, "drivetrain": "Front-Wheel Drive"},
    },
    # Dodge
    "Dodge": {
        "Durango": {"transmission": "8-Speed Automatic", "cylinders": 8, "drivetrain": "All-Wheel Drive"},
        "Charger": {"transmission": "8-Speed Automatic", "cylinders": 8, "drivetrain": "Rear-Wheel Drive"},
        "Journey": {"transmission": "6-Speed Automatic", "cylinders": 6, "drivetrain": "Front-Wheel Drive"},
    },
    # Ram
    "Ram": {
        "1500": {"transmission": "10-Speed Automatic", "cylinders": 6, "drivetrain": "Four-Wheel Drive"},
        "2500": {"transmission": "6-Speed Automatic", "cylinders": 6, "drivetrain": "Four-Wheel Drive"},
        "3500": {"transmission": "6-Speed Automatic", "cylinders": 8, "drivetrain": "Four-Wheel Drive"},
    },
    # Lincoln
    "Lincoln": {
        "Navigator": {"transmission": "10-Speed Automatic", "cylinders": 6, "drivetrain": "Four-Wheel Drive"},
        "Nautilus": {"transmission": "8-Speed Automatic", "cylinders": 6, "drivetrain": "All-Wheel Drive"},
    },
    # Honda
    "Honda": {
        "Civic": {"transmission": "Continuously Variable Transmission (CVT)", "cylinders": 4, "drivetrain": "Front-Wheel Drive"},
        "Accord": {"transmission": "10-Speed Automatic", "cylinders": 4, "drivetrain": "Front-Wheel Drive"},
    },
    # Hyundai
    "Hyundai": {
        "Kona": {"transmission": "Continuously Variable Transmission (CVT)", "cylinders": 4, "drivetrain": "Front-Wheel Drive"},
        "Elantra Gt": {"transmission": "Continuously Variable Transmission (CVT)", "cylinders": 4, "drivetrain": "Front-Wheel Drive"},
        "Sonata": {"transmission": "8-Speed Automatic", "cylinders": 4, "drivetrain": "Front-Wheel Drive"},
    },
    # Chevrolet
    "Chevrolet": {
        "Camaro": {"transmission": "10-Speed Automatic", "cylinders": 6, "drivetrain": "Rear-Wheel Drive"},
        "Silverado": {"transmission": "10-Speed Automatic", "cylinders": 6, "drivetrain": "Four-Wheel Drive"},
    },
    # Buick
    "Buick": {
        "Lacrosse": {"transmission": "9-Speed Automatic", "cylinders": 6, "drivetrain": "Front-Wheel Drive"},
    },
    # Chrysler
    "Chrysler": {
        "Pacifica": {"transmission": "9-Speed Automatic", "cylinders": 6, "drivetrain": "Front-Wheel Drive"},
    },
    # GMC
    "GMC": {
        "Acadia": {"transmission": "9-Speed Automatic", "cylinders": 6, "drivetrain": "All-Wheel Drive"},
        "Sierra": {"transmission": "10-Speed Automatic", "cylinders": 6, "drivetrain": "Four-Wheel Drive"},
    },
}


def populate_model_specs():
    """Insert model specs into the database."""
    db_path = os.environ.get("INVENTORY_DB_PATH", "inventory.db")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Ensure table exists
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS model_specs (
            make TEXT NOT NULL,
            model TEXT NOT NULL,
            cylinders INTEGER,
            gears INTEGER,
            transmission TEXT,
            PRIMARY KEY (make, model)
        )
    """)

    # Add drivetrain column if it doesn't exist
    try:
        cursor.execute("ALTER TABLE model_specs ADD COLUMN drivetrain TEXT")
    except sqlite3.OperationalError:
        pass  # Column already exists

    inserted = 0
    updated = 0

    for make, models in MODEL_SPECS.items():
        for model, specs in models.items():
            transmission = specs.get("transmission")
            cylinders = specs.get("cylinders")
            drivetrain = specs.get("drivetrain")

            try:
                cursor.execute(
                    """
                    INSERT OR REPLACE INTO model_specs
                    (make, model, transmission, cylinders, drivetrain)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (make, model, transmission, cylinders, drivetrain),
                )
                inserted += 1
            except sqlite3.Error as e:
                print(f"Error inserting {make} {model}: {e}")

    conn.commit()

    # Show summary
    cursor.execute("SELECT COUNT(*) FROM model_specs")
    total = cursor.fetchone()[0]

    print("\n" + "=" * 60)
    print("Model Specs Dictionary Populated")
    print("=" * 60)
    print(f"Total entries: {total}")
    print(f"Makes covered: {len(MODEL_SPECS)}")
    print("=" * 60)
    print("\nSample entries:")
    cursor.execute(
        "SELECT make, model, transmission, cylinders, drivetrain FROM model_specs LIMIT 10"
    )
    for row in cursor.fetchall():
        print(f"  {row[0]:15} {row[1]:20} {row[2]:35} {row[3]:2} cyl {row[4]}")

    conn.close()


if __name__ == "__main__":
    populate_model_specs()
