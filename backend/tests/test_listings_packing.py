"""Contract tests for the dictionary-encoded /listings cascade table.

``pack_car_rows`` shrinks ``options["car_rows"]`` (~12,700 rows) from ~1.9 MB of
uncompressed HTML down to a codes-plus-vocabulary blob. The decoder lives in
``frontend/static/listings_boot.js`` (``unpackCarRows``), so the wire format is a contract
between two languages with nothing enforcing it. These tests pin it from the
Python side and assert the JS decoder still speaks the same dialect.
"""

import random
import re
from pathlib import Path

from backend.listings.routes import _CAR_ROW_COLUMNS, pack_car_rows

_MAIN_JS = Path(__file__).resolve().parents[2] / "frontend" / "static" / "listings_boot.js"


def unpack_car_rows(packed):
    """Line-for-line mirror of ``unpackCarRows`` in frontend/static/listings_boot.js.

    Kept here (rather than importing an unused Python decoder from the app) so
    the round-trip test exercises the exact decode the browser performs. If you
    change the JS decoder, change this and ``test_js_decoder_matches_wire_format``
    will tell you if the sentinels drifted.
    """
    if isinstance(packed, list):
        return packed  # legacy/plain form
    if not isinstance(packed, dict):
        return []
    rows = packed.get("r")
    cols = packed.get("c")
    if not isinstance(rows, list) or not isinstance(cols, list):
        return []
    vocabs = packed.get("v") if isinstance(packed.get("v"), list) else []
    out = []
    for row in rows:
        obj = {}
        for i, col in enumerate(cols):
            vocab = vocabs[i] if i < len(vocabs) else None
            if not isinstance(vocab, list):
                obj[col] = row[i]
                continue
            code = row[i]
            obj[col] = vocab[code] if 0 <= code < len(vocab) else None
        out.append(obj)
    return out


_MAKES = ["Ram", "Toyota", "BMW", "Ford", None]
_MODELS = ["1500", "Camry", "X5", "F-150", "Wrangler", None]
_TRIMS = ["Limited", "SE", "xDrive40i", None, "", "Big Horn"]
_FUELS = ["Gasoline", "Hybrid", "Electric", None]
_CYLS = [4, 6, 8, 0, None]
_DRIVES = ["4WD", "AWD", "FWD", None]
_BODIES = ["Truck", "Sedan", "SUV", None]
_INDUCTIONS = ["Turbocharged", "Naturally Aspirated", None]


def _random_rows(n, seed):
    rng = random.Random(seed)
    return [
        {
            "make": rng.choice(_MAKES),
            "model": rng.choice(_MODELS),
            "trim": rng.choice(_TRIMS),
            "fuel": rng.choice(_FUELS),
            "cyl": rng.choice(_CYLS),
            "drive": rng.choice(_DRIVES),
            "body_style": rng.choice(_BODIES),
            "induction": rng.choice(_INDUCTIONS),
        }
        for _ in range(n)
    ]


def test_round_trip_restores_every_row_verbatim():
    """pack -> unpack is the identity over the real column set, nulls included."""
    rows = _random_rows(500, seed=20260729)
    assert unpack_car_rows(pack_car_rows(rows)) == rows


def test_round_trip_over_many_random_shapes():
    """Property-ish sweep: many independently shaped tables all round-trip."""
    for seed in range(40):
        rng = random.Random(seed)
        rows = _random_rows(rng.randint(1, 60), seed=seed)
        assert unpack_car_rows(pack_car_rows(rows)) == rows, f"seed {seed}"


def test_empty_string_trim_is_not_collapsed_to_none():
    """`trim=""` is a real cascade value (the "—" option); it must survive the codec."""
    rows = [{c: None for c in _CAR_ROW_COLUMNS} | {"make": "Ram", "model": "1500", "trim": ""}]
    out = unpack_car_rows(pack_car_rows(rows))
    assert out[0]["trim"] == ""
    assert out[0]["trim"] is not None


def test_missing_keys_decode_as_none():
    """Rows that omit a column entirely come back with that column explicitly None."""
    packed = pack_car_rows([{"make": "Ford", "model": "F-150"}])
    row = unpack_car_rows(packed)[0]
    assert set(row) == set(_CAR_ROW_COLUMNS)
    assert row["make"] == "Ford"
    assert row["model"] == "F-150"
    assert all(row[c] is None for c in _CAR_ROW_COLUMNS if c not in ("make", "model"))


def test_cyl_ships_raw_not_dictionary_encoded():
    """`cyl` is a small int already — encoding it would only add a vocabulary."""
    packed = pack_car_rows([{"make": "Ram", "model": "1500", "cyl": 8}])
    cyl_idx = _CAR_ROW_COLUMNS.index("cyl")
    assert packed["v"][cyl_idx] is None
    assert packed["r"][0][cyl_idx] == 8


def test_cyl_zero_survives_and_is_not_confused_with_a_code():
    """Electric rows carry cyl=0; the -1 null sentinel must not eat it."""
    rows = [{"make": "Tesla", "model": "Model 3", "cyl": 0}, {"make": "Tesla", "model": "Model Y", "cyl": None}]
    out = unpack_car_rows(pack_car_rows(rows))
    assert out[0]["cyl"] == 0
    assert out[1]["cyl"] is None


def test_vocabulary_is_deduplicated():
    """The whole point: N repeats of a string cost one vocabulary entry."""
    rows = [{"make": "Ram", "model": "1500", "trim": "Limited"} for _ in range(200)]
    packed = pack_car_rows(rows)
    make_idx = _CAR_ROW_COLUMNS.index("make")
    assert packed["v"][make_idx] == ["Ram"]
    assert len(packed["r"]) == 200
    assert {tuple(r) for r in packed["r"]} == {tuple(packed["r"][0])}


def test_empty_and_degenerate_input():
    for empty in ([], None):
        packed = pack_car_rows(empty)
        assert packed["c"] == list(_CAR_ROW_COLUMNS)
        assert packed["r"] == []
        assert unpack_car_rows(packed) == []
    # A table of nothing-but-nulls still decodes to the same rows.
    rows = [{c: None for c in _CAR_ROW_COLUMNS} for _ in range(3)]
    assert unpack_car_rows(pack_car_rows(rows)) == rows


def test_legacy_plain_array_passthrough():
    """Older cached HTML shipped a plain list of dicts; the decoder must not eat it."""
    legacy = [{"make": "Ram", "model": "1500", "trim": "Limited", "cyl": 8}]
    assert unpack_car_rows(legacy) is legacy
    assert unpack_car_rows([]) == []


def test_decoder_tolerates_garbage_without_raising():
    """A truncated/garbled blob must degrade to an empty cascade, never throw."""
    for junk in ({}, {"c": ["make"]}, {"r": [[0]]}, "nope", 7, None):
        assert unpack_car_rows(junk) == []


def test_js_decoder_matches_wire_format():
    """Guard against silent drift between pack_car_rows and unpackCarRows."""
    js = _MAIN_JS.read_text(encoding="utf-8")
    start = js.index("function unpackCarRows(")
    body = js[start : start + 1200]
    # Same three top-level keys.
    assert "packed.r" in body and "packed.c" in body and "packed.v" in body
    # Same legacy passthrough branch.
    assert "Array.isArray(packed)) return packed" in body
    # Same null sentinel: a code outside the vocabulary decodes to null, and
    # pack_car_rows emits -1 for null, so the JS must reject negatives.
    assert re.search(r"code\s*>=\s*0\s*&&\s*code\s*<\s*vocab\.length", body)
    packed = pack_car_rows([{"make": None}])
    assert packed["r"][0][_CAR_ROW_COLUMNS.index("make")] == -1
