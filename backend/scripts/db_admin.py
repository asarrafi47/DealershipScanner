#!/usr/bin/env python3
"""
Local-only model_specs dictionary editor (operator tool — not the store ``/admin`` UI).

Requires ``DB_ADMIN_TOKEN`` (min 16 chars). Binds ``127.0.0.1`` only.

Run from repo root::

    export DB_ADMIN_TOKEN="$(python -c 'import secrets; print(secrets.token_urlsafe(32))')"
    python -m backend.scripts.db_admin

Then open: http://127.0.0.1:5001/model-specs-admin
"""
from __future__ import annotations

import hmac
import os
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
os.chdir(ROOT)
sys.path.insert(0, str(ROOT))

try:
    from backend.utils.project_env import load_project_dotenv

    load_project_dotenv()
except ImportError:
    pass

from flask import Flask, abort, jsonify, render_template_string, request

from backend.utils.runtime_env import is_production_env

app = Flask(__name__)

DB_PATH = os.environ.get("INVENTORY_DB_PATH", "inventory.db")
_MODEL_SPEC_COLUMNS = frozenset({"transmission", "drivetrain", "cylinders"})
_MIN_TOKEN_LEN = 16


def _expected_token() -> str:
    return (os.environ.get("DB_ADMIN_TOKEN") or "").strip()


def _token_ok(provided: str) -> bool:
    expected = _expected_token()
    if not expected or len(expected) < _MIN_TOKEN_LEN:
        return False
    got = (provided or "").strip()
    if not got:
        return False
    return hmac.compare_digest(got, expected)


def _auth_header_token() -> str:
    auth = (request.headers.get("Authorization") or "").strip()
    if auth.lower().startswith("bearer "):
        return auth[7:].strip()
    return (request.headers.get("X-DB-Admin-Token") or "").strip()


@app.before_request
def _require_db_admin_token() -> None:
    if request.endpoint == "db_admin_ui":
        return
    if not _token_ok(_auth_header_token()):
        abort(401)


HTML = """
<!DOCTYPE html>
<html>
<head>
    <title>Model Specs Dictionary (local)</title>
    <style>
        * { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; }
        body { padding: 20px; background: #f5f5f5; }
        .container { max-width: 1200px; margin: 0 auto; }
        h1 { color: #333; }
        table { width: 100%; border-collapse: collapse; background: white; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
        th, td { padding: 12px; text-align: left; border-bottom: 1px solid #ddd; }
        th { background: #2c3e50; color: white; font-weight: 600; }
        tr:hover { background: #f9f9f9; }
        input { padding: 6px; border: 1px solid #ddd; border-radius: 4px; }
        button { padding: 8px 16px; background: #3498db; color: white; border: none; border-radius: 4px; cursor: pointer; }
        button:hover { background: #2980b9; }
        button.delete { background: #e74c3c; }
        button.delete:hover { background: #c0392b; }
        .form { background: white; padding: 20px; margin-bottom: 20px; border-radius: 4px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
        .form-row { margin-bottom: 12px; display: grid; grid-template-columns: 1fr 1fr 1fr 1fr; gap: 10px; }
        label { display: block; font-size: 12px; color: #666; margin-bottom: 4px; }
        input[type="number"] { width: 100%; }
        .stats { background: white; padding: 15px; border-radius: 4px; margin-bottom: 20px; }
        .stats p { margin: 5px 0; color: #666; }
        .auth { background: #fff3cd; padding: 12px; border-radius: 4px; margin-bottom: 16px; }
    </style>
</head>
<body>
    <div class="container">
        <h1>Model Specs Dictionary (local)</h1>
        <div class="auth">
            <label for="admin_token">DB_ADMIN_TOKEN</label>
            <input type="password" id="admin_token" style="width:100%;max-width:480px" placeholder="Paste token from your shell env">
            <button type="button" onclick="saveToken()">Save token</button>
        </div>

        <div class="stats">
            <p><strong>Total entries:</strong> <span id="total">0</span></p>
            <p><strong>Database:</strong> model_specs</p>
        </div>

        <div class="form">
            <h2>Add entry</h2>
            <div class="form-row">
                <div><label>Make</label><input type="text" id="new_make"></div>
                <div><label>Model</label><input type="text" id="new_model"></div>
                <div><label>Transmission</label><input type="text" id="new_transmission"></div>
                <div><label>Cylinders</label><input type="number" id="new_cylinders" min="0"></div>
            </div>
            <div class="form-row">
                <div style="grid-column: 1/3;"><label>Drivetrain</label><input type="text" id="new_drivetrain"></div>
                <div style="grid-column: 3/5;"><label>&nbsp;</label><button type="button" onclick="addEntry()">Add</button></div>
            </div>
        </div>

        <h2>All entries</h2>
        <table>
            <thead>
                <tr><th>Make</th><th>Model</th><th>Transmission</th><th>Drivetrain</th><th>Cylinders</th><th></th></tr>
            </thead>
            <tbody id="table_body"></tbody>
        </table>
    </div>

    <script>
        const TOKEN_KEY = 'db_admin_token';

        function esc(s) {
            const d = document.createElement('div');
            d.textContent = s == null ? '' : String(s);
            return d.innerHTML;
        }

        function saveToken() {
            const t = document.getElementById('admin_token').value.trim();
            if (t) sessionStorage.setItem(TOKEN_KEY, t);
            loadData();
        }

        function headers() {
            const t = sessionStorage.getItem(TOKEN_KEY) || '';
            return t ? { 'Authorization': 'Bearer ' + t, 'Content-Type': 'application/json' } : {};
        }

        async function loadData() {
            const resp = await fetch('/model-specs-admin/api/specs', { headers: headers() });
            if (!resp.ok) { alert('Unauthorized — set DB_ADMIN_TOKEN'); return; }
            const data = await resp.json();
            document.getElementById('total').textContent = data.specs.length;
            const tbody = document.getElementById('table_body');
            tbody.innerHTML = '';
            for (const spec of data.specs) {
                const row = document.createElement('tr');
                row.innerHTML =
                    '<td>' + esc(spec.make) + '</td>' +
                    '<td>' + esc(spec.model) + '</td>' +
                    '<td><input type="text" value="' + esc(spec.transmission || '') + '" data-id="' + spec.id + '" data-field="transmission"></td>' +
                    '<td><input type="text" value="' + esc(spec.drivetrain || '') + '" data-id="' + spec.id + '" data-field="drivetrain"></td>' +
                    '<td><input type="number" value="' + esc(spec.cylinders || 0) + '" data-id="' + spec.id + '" data-field="cylinders"></td>' +
                    '<td><button class="delete" type="button" data-del="' + spec.id + '">Delete</button></td>';
                tbody.appendChild(row);
            }
            tbody.querySelectorAll('input[data-field]').forEach((inp) => {
                inp.addEventListener('change', () => updateSpec(inp.dataset.id, inp.dataset.field, inp.value));
            });
            tbody.querySelectorAll('button[data-del]').forEach((btn) => {
                btn.addEventListener('click', () => deleteSpec(btn.dataset.del));
            });
        }

        async function addEntry() {
            const spec = {
                make: document.getElementById('new_make').value.trim(),
                model: document.getElementById('new_model').value.trim(),
                transmission: document.getElementById('new_transmission').value.trim(),
                drivetrain: document.getElementById('new_drivetrain').value.trim(),
                cylinders: parseInt(document.getElementById('new_cylinders').value, 10) || 0,
            };
            if (!spec.make || !spec.model) { alert('Make and Model required'); return; }
            const resp = await fetch('/model-specs-admin/api/specs', {
                method: 'POST', headers: headers(), body: JSON.stringify(spec),
            });
            if (resp.ok) loadData(); else alert('Error adding entry');
        }

        async function updateSpec(id, field, value) {
            const body = { field, value: field === 'cylinders' ? (parseInt(value, 10) || 0) : value };
            const resp = await fetch('/model-specs-admin/api/specs/' + id, {
                method: 'PUT', headers: headers(), body: JSON.stringify(body),
            });
            if (!resp.ok) { alert('Error updating'); loadData(); }
        }

        async function deleteSpec(id) {
            if (!confirm('Delete?')) return;
            const resp = await fetch('/model-specs-admin/api/specs/' + id, {
                method: 'DELETE', headers: headers(),
            });
            if (resp.ok) loadData(); else alert('Error deleting');
        }

        loadData();
    </script>
</body>
</html>
"""


@app.route("/model-specs-admin")
def db_admin_ui():
    return render_template_string(HTML)


@app.route("/model-specs-admin/api/specs", methods=["GET"])
def get_specs():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute(
        "SELECT ROWID AS id, make, model, transmission, drivetrain, cylinders "
        "FROM model_specs ORDER BY make, model"
    )
    specs = [dict(row) for row in cur.fetchall()]
    conn.close()
    return jsonify({"specs": specs})


def _row_by_display_id(conn: sqlite3.Connection, spec_id: int) -> tuple[str, str] | None:
    cur = conn.cursor()
    cur.execute(
        "SELECT make, model FROM model_specs ORDER BY make, model LIMIT 1 OFFSET ?",
        (max(0, spec_id - 1),),
    )
    row = cur.fetchone()
    if not row:
        return None
    return str(row[0]), str(row[1])


@app.route("/model-specs-admin/api/specs", methods=["POST"])
def add_spec():
    data = request.get_json(silent=True) or {}
    make = (data.get("make") or "").strip()
    model = (data.get("model") or "").strip()
    if not make or not model:
        return jsonify({"error": "make and model required"}), 400
    try:
        cylinders = int(data.get("cylinders") or 0)
    except (TypeError, ValueError):
        cylinders = 0
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute(
            """
            INSERT OR REPLACE INTO model_specs (make, model, transmission, drivetrain, cylinders)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                make,
                model,
                (data.get("transmission") or "").strip() or None,
                (data.get("drivetrain") or "").strip() or None,
                cylinders,
            ),
        )
        conn.commit()
        return jsonify({"ok": True})
    except sqlite3.Error as e:
        return jsonify({"error": str(e)}), 400
    finally:
        conn.close()


@app.route("/model-specs-admin/api/specs/<int:spec_id>", methods=["PUT"])
def update_spec(spec_id: int):
    data = request.get_json(silent=True) or {}
    field = (data.get("field") or "").strip()
    if field not in _MODEL_SPEC_COLUMNS:
        return jsonify({"error": "invalid_field"}), 400
    value = data.get("value")
    if field == "cylinders":
        try:
            value = int(value)
        except (TypeError, ValueError):
            value = 0

    conn = sqlite3.connect(DB_PATH)
    try:
        pair = _row_by_display_id(conn, spec_id)
        if not pair:
            return jsonify({"error": "not_found"}), 404
        make, model = pair
        conn.execute(
            f"UPDATE model_specs SET {field} = ? WHERE make = ? AND model = ?",
            (value, make, model),
        )
        conn.commit()
        return jsonify({"ok": True})
    except sqlite3.Error as e:
        return jsonify({"error": str(e)}), 400
    finally:
        conn.close()


@app.route("/model-specs-admin/api/specs/<int:spec_id>", methods=["DELETE"])
def delete_spec(spec_id: int):
    conn = sqlite3.connect(DB_PATH)
    try:
        pair = _row_by_display_id(conn, spec_id)
        if not pair:
            return jsonify({"error": "not_found"}), 404
        make, model = pair
        conn.execute("DELETE FROM model_specs WHERE make = ? AND model = ?", (make, model))
        conn.commit()
        return jsonify({"ok": True})
    except sqlite3.Error as e:
        return jsonify({"error": str(e)}), 400
    finally:
        conn.close()


def _startup_checks() -> None:
    token = _expected_token()
    if len(token) < _MIN_TOKEN_LEN:
        raise RuntimeError(
            f"Set DB_ADMIN_TOKEN (min {_MIN_TOKEN_LEN} chars) before running db_admin (SEC-080)."
        )
    if is_production_env() and os.environ.get("DB_ADMIN_DEBUG", "").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    ):
        raise RuntimeError("DB_ADMIN_DEBUG is not allowed when FLASK_ENV=production.")


if __name__ == "__main__":
    _startup_checks()
    print(f"Database: {DB_PATH}")
    print("Open: http://127.0.0.1:5001/model-specs-admin")
    debug = os.environ.get("DB_ADMIN_DEBUG", "").strip().lower() in ("1", "true", "yes", "on")
    app.run(host="127.0.0.1", port=5001, debug=debug)
