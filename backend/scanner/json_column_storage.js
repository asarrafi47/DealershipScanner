/**
 * Persist JSON array columns as NULL when empty (never literal "[]" in TEXT columns).
 */

function nullableJsonArrayText(value) {
  if (value == null) return null;
  if (Array.isArray(value)) return value.length ? JSON.stringify(value) : null;
  if (typeof value === "string") {
    const s = value.trim();
    if (!s || s === "[]" || s === "null") return null;
    try {
      const parsed = JSON.parse(s);
      if (Array.isArray(parsed)) return parsed.length ? s : null;
    } catch (_e) {
      return s;
    }
    return s;
  }
  return null;
}

module.exports = { nullableJsonArrayText };
