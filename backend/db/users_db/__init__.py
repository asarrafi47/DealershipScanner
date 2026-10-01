"""Facade for the users database layer.

Historically this was a single ~2000-line module. It has been split by domain
into submodules (``_common``, ``schema``, ``auth``, ``accounts``, ``billing``,
``admin``). This package re-exports every previously-public name so that
``from backend.db.users_db import X`` keeps working unchanged.
"""

# Preserve the original module-level imports so names like ``os`` / ``sqlite3``
# / ``hash_password`` remain importable from this module path.
import os
import secrets
import sqlite3
import threading
import time
from functools import lru_cache

from backend.db.password_hash import hash_password, password_needs_rehash, verify_or_legacy
from backend.db.users_sqlite import DB_PATH, get_users_conn
from backend.utils.roles import ROLE_ADMIN
from backend.utils.runtime_env import is_production_env

from ._common import (
    _schedule_password_rehash,
    _user_dict_from_row,
    _user_row_is_active,
    _users_select_columns,
    get_conn,
)
from .accounts import (
    create_org,
    delete_user_by_email,
    get_org,
    get_user_by_apple_sub,
    get_user_by_google_sub,
    link_user_apple_sub,
    link_user_google_sub,
    save_apple_oauth_user,
    save_oauth_user,
    save_user,
    update_org_stripe_subscription,
    user_exists_by_email,
    user_exists_by_username,
)
from .admin import (
    ADMIN_ASSIGNABLE_ROLES,
    _admin_login_methods,
    _admin_scope_label,
    _org_names_for_users,
    _saved_car_counts_for_users,
    _user_has_env_admin_privilege_by_id,
    admin_create_user,
    admin_reset_user_password,
    admin_role_is_assignable,
    admin_update_user,
    admin_update_user_scope,
    delete_user_by_id,
    get_user_admin_record,
    list_users_for_admin,
    set_user_is_active,
)
from .auth import (
    authenticate_app_user,
    submitted_password_attempts,
    change_user_password,
    check_user,
    clear_user_email_verify_token,
    clear_user_password_reset_token,
    get_user_by_login,
    get_user_email_verification_state,
    get_user_id_by_email_verify_token_hash,
    get_user_id_by_password_reset_token_hash,
    get_user_profile,
    get_user_totp,
    mark_user_email_verified,
    reset_user_password,
    set_user_email_verify_token,
    set_user_mfa_phone,
    set_user_password_reset_token,
    set_user_totp,
    update_user_profile,
)
from .billing import (
    get_user_billing_snapshot,
    get_user_premium_status,
    grant_user_premium,
    revoke_user_premium,
)
from .schema import (
    _apply_env_admin_privileges,
    _env_admin_set_clause,
    init_users_db,
    sync_env_admin_user_row,
)

__all__ = [
    # Re-exported convenience imports preserved from the original module surface.
    "os",
    "secrets",
    "sqlite3",
    "threading",
    "time",
    "lru_cache",
    "hash_password",
    "password_needs_rehash",
    "verify_or_legacy",
    "DB_PATH",
    "get_users_conn",
    "ROLE_ADMIN",
    "is_production_env",
    "get_conn",
    "_users_select_columns",
    "_user_dict_from_row",
    "_user_row_is_active",
    "_schedule_password_rehash",
    "_env_admin_set_clause",
    "_apply_env_admin_privileges",
    "sync_env_admin_user_row",
    "init_users_db",
    "get_user_totp",
    "set_user_totp",
    "get_user_by_login",
    "update_user_profile",
    "change_user_password",
    "reset_user_password",
    "authenticate_app_user",
    "submitted_password_attempts",
    "get_user_profile",
    "get_user_email_verification_state",
    "set_user_email_verify_token",
    "clear_user_email_verify_token",
    "mark_user_email_verified",
    "get_user_id_by_email_verify_token_hash",
    "set_user_password_reset_token",
    "clear_user_password_reset_token",
    "get_user_id_by_password_reset_token_hash",
    "set_user_mfa_phone",
    "check_user",
    "create_org",
    "get_org",
    "update_org_stripe_subscription",
    "user_exists_by_email",
    "get_user_by_google_sub",
    "link_user_google_sub",
    "get_user_by_apple_sub",
    "link_user_apple_sub",
    "user_exists_by_username",
    "save_oauth_user",
    "save_apple_oauth_user",
    "save_user",
    "delete_user_by_email",
    "grant_user_premium",
    "revoke_user_premium",
    "get_user_premium_status",
    "get_user_billing_snapshot",
    "_saved_car_counts_for_users",
    "_org_names_for_users",
    "_admin_login_methods",
    "_admin_scope_label",
    "list_users_for_admin",
    "ADMIN_ASSIGNABLE_ROLES",
    "admin_role_is_assignable",
    "get_user_admin_record",
    "_user_has_env_admin_privilege_by_id",
    "admin_create_user",
    "admin_update_user_scope",
    "admin_update_user",
    "admin_reset_user_password",
    "delete_user_by_id",
    "set_user_is_active",
]
