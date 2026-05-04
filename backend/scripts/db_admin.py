#!/usr/bin/env python3
"""
Simple database admin UI to view/edit model_specs dictionary.

Run: python db_admin.py

Then open: http://127.0.0.1:5001/admin
"""
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
os.chdir(ROOT)
sys.path.insert(0, str(ROOT))

try:
    from backend.utils.project_env import load_project_dotenv
    load_project_dotenv()
except ImportError:
    pass

from flask import Flask, render_template_string, request, jsonify
import sqlite3

app = Flask(__name__)

DB_PATH = os.environ.get("INVENTORY_DB_PATH", "inventory.db")

HTML = """
<!DOCTYPE html>
<html>
<head>
    <title>Database Admin - Model Specs</title>
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
    </style>
</head>
<body>
    <div class="container">
        <h1>🗄️ Model Specs Dictionary</h1>

        <div class="stats">
            <p><strong>Total entries:</strong> <span id="total">0</span></p>
            <p><strong>Database:</strong> inventory.db (model_specs table)</p>
        </div>

        <div class="form">
            <h2>➕ Add New Entry</h2>
            <div class="form-row">
                <div>
                    <label>Make</label>
                    <input type="text" id="new_make" placeholder="e.g., BMW">
                </div>
                <div>
                    <label>Model</label>
                    <input type="text" id="new_model" placeholder="e.g., X3">
                </div>
                <div>
                    <label>Transmission</label>
                    <input type="text" id="new_transmission" placeholder="e.g., 8-Speed Automatic">
                </div>
                <div>
                    <label>Cylinders</label>
                    <input type="number" id="new_cylinders" placeholder="0" min="0">
                </div>
            </div>
            <div class="form-row">
                <div style="grid-column: 1/3;">
                    <label>Drivetrain</label>
                    <input type="text" id="new_drivetrain" placeholder="e.g., All-Wheel Drive">
                </div>
                <div style="grid-column: 3/5;">
                    <label>&nbsp;</label>
                    <button onclick="addEntry()">➕ Add Entry</button>
                </div>
            </div>
        </div>

        <h2>📋 All Entries</h2>
        <table>
            <thead>
                <tr>
                    <th>Make</th>
                    <th>Model</th>
                    <th>Transmission</th>
                    <th>Drivetrain</th>
                    <th>Cylinders</th>
                    <th>Actions</th>
                </tr>
            </thead>
            <tbody id="table_body">
            </tbody>
        </table>
    </div>

    <script>
        async function loadData() {
            const resp = await fetch('/admin/api/specs');
            const data = await resp.json();

            document.getElementById('total').textContent = data.specs.length;

            const tbody = document.getElementById('table_body');
            tbody.innerHTML = '';

            for (const spec of data.specs) {
                const row = document.createElement('tr');
                row.innerHTML = `
                    <td>${spec.make}</td>
                    <td>${spec.model}</td>
                    <td><input type="text" value="${spec.transmission || ''}" onchange="updateSpec(${spec.id}, 'transmission', this.value)"></td>
                    <td><input type="text" value="${spec.drivetrain || ''}" onchange="updateSpec(${spec.id}, 'drivetrain', this.value)"></td>
                    <td><input type="number" value="${spec.cylinders || 0}" onchange="updateSpec(${spec.id}, 'cylinders', this.value)"></td>
                    <td><button class="delete" onclick="deleteSpec(${spec.id})">🗑️ Delete</button></td>
                `;
                tbody.appendChild(row);
            }
        }

        async function addEntry() {
            const spec = {
                make: document.getElementById('new_make').value.trim(),
                model: document.getElementById('new_model').value.trim(),
                transmission: document.getElementById('new_transmission').value.trim(),
                drivetrain: document.getElementById('new_drivetrain').value.trim(),
                cylinders: parseInt(document.getElementById('new_cylinders').value) || 0,
            };

            if (!spec.make || !spec.model) {
                alert('Make and Model are required');
                return;
            }

            const resp = await fetch('/admin/api/specs', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify(spec)
            });

            if (resp.ok) {
                document.getElementById('new_make').value = '';
                document.getElementById('new_model').value = '';
                document.getElementById('new_transmission').value = '';
                document.getElementById('new_drivetrain').value = '';
                document.getElementById('new_cylinders').value = '';
                loadData();
            } else {
                alert('Error adding entry');
            }
        }

        async function updateSpec(id, field, value) {
            const resp = await fetch(`/admin/api/specs/${id}`, {
                method: 'PUT',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({
                    field: field,
                    value: field === 'cylinders' ? (parseInt(value) || 0) : value
                })
            });

            if (!resp.ok) {
                alert('Error updating');
                loadData();
            }
        }

        async function deleteSpec(id) {
            if (!confirm('Delete this entry?')) return;

            const resp = await fetch(`/admin/api/specs/${id}`, {method: 'DELETE'});
            if (resp.ok) {
                loadData();
            } else {
                alert('Error deleting');
            }
        }

        loadData();
    </script>
</body>
</html>
"""


@app.route("/admin")
def admin():
    return render_template_string(HTML)


@app.route("/admin/api/specs", methods=["GET"])
def get_specs():
    """List all model specs."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("SELECT ROWID as id, make, model, transmission, drivetrain, cylinders FROM model_specs ORDER BY make, model")
    specs = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return jsonify({"specs": specs})


@app.route("/admin/api/specs", methods=["POST"])
def add_spec():
    """Add a new model spec."""
    data = request.json
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    try:
        cursor.execute(
            """
            INSERT OR REPLACE INTO model_specs (make, model, transmission, drivetrain, cylinders)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                data.get("make"),
                data.get("model"),
                data.get("transmission"),
                data.get("drivetrain"),
                data.get("cylinders", 0)
            )
        )
        conn.commit()
        conn.close()
        return jsonify({"ok": True})
    except Exception as e:
        conn.close()
        return jsonify({"error": str(e)}), 400


@app.route("/admin/api/specs/<int:spec_id>", methods=["PUT"])
def update_spec(spec_id):
    """Update a spec field."""
    data = request.json
    field = data.get("field")
    value = data.get("value")

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Get current row
    cursor.execute("SELECT ROWID, make, model FROM model_specs LIMIT 1 OFFSET ?", (spec_id - 1,))
    row = cursor.fetchone()

    if not row:
        conn.close()
        return jsonify({"error": "Not found"}), 404

    make, model = row[1], row[2]

    try:
        cursor.execute(
            f"UPDATE model_specs SET {field} = ? WHERE make = ? AND model = ?",
            (value, make, model)
        )
        conn.commit()
        conn.close()
        return jsonify({"ok": True})
    except Exception as e:
        conn.close()
        return jsonify({"error": str(e)}), 400


@app.route("/admin/api/specs/<int:spec_id>", methods=["DELETE"])
def delete_spec(spec_id):
    """Delete a spec."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Get current row
    cursor.execute("SELECT ROWID, make, model FROM model_specs LIMIT 1 OFFSET ?", (spec_id - 1,))
    row = cursor.fetchone()

    if not row:
        conn.close()
        return jsonify({"error": "Not found"}), 404

    make, model = row[1], row[2]

    try:
        cursor.execute("DELETE FROM model_specs WHERE make = ? AND model = ?", (make, model))
        conn.commit()
        conn.close()
        return jsonify({"ok": True})
    except Exception as e:
        conn.close()
        return jsonify({"error": str(e)}), 400


if __name__ == "__main__":
    print(f"Database: {DB_PATH}")
    print("Open: http://127.0.0.1:5001/admin")
    app.run(port=5001, debug=True)
