"""Saved cars / favorites."""
from backend.db.repositories.base_repo import db_conn


def save_car(user_id: int, car_id: int) -> None:
    with db_conn() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO saved_cars (user_id, car_id) VALUES (?, ?)",
            (int(user_id), int(car_id)),
        )
        conn.commit()


def unsave_car(user_id: int, car_id: int) -> None:
    with db_conn() as conn:
        conn.execute(
            "DELETE FROM saved_cars WHERE user_id = ? AND car_id = ?",
            (int(user_id), int(car_id)),
        )
        conn.commit()


def get_saved_car_ids(user_id: int) -> list[int]:
    with db_conn() as conn:
        rows = conn.execute(
            "SELECT car_id FROM saved_cars WHERE user_id = ? ORDER BY saved_at DESC",
            (int(user_id),),
        ).fetchall()
    return [int(r[0]) for r in rows]


def is_car_saved(user_id: int, car_id: int) -> bool:
    with db_conn() as conn:
        row = conn.execute(
            "SELECT 1 FROM saved_cars WHERE user_id = ? AND car_id = ? LIMIT 1",
            (int(user_id), int(car_id)),
        ).fetchone()
    return row is not None
