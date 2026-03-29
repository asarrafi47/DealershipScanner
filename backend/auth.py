from flask_login import UserMixin

from backend.db.users_db import get_user_by_id
from backend.security import login_manager


class User(UserMixin):
    __slots__ = ("id", "username", "email")

    def __init__(self, user_id: int, username: str, email: str):
        self.id = user_id
        self.username = username
        self.email = email


@login_manager.user_loader
def load_user(user_id):
    if user_id is None:
        return None
    try:
        uid = int(user_id)
    except (TypeError, ValueError):
        return None
    row = get_user_by_id(uid)
    if row is None:
        return None
    return User(row["id"], row["username"], row["email"])
