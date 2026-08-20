"""Sticker-printed colors supersede the feed's — photo-observed ones never do.

Policy (2026-08-18): the window sticker is the single source of truth, so a
color the document PRINTS (``color_source == "monroney"`` in the
``car_image_text`` summary) is displayed as the car's color with provenance,
while a color an agent merely OBSERVED in gallery photos (``"photo"``) never
overrides the feed — dark leather under warm light has misread black interiors
as brown. Covers the ``color_overlay`` payload shaping and the batched
``cars_repo.car_sticker_msrp_values`` read that now carries the color facts.
"""

from backend.utils.car_serialize.color_overlay import color_overlay_public_fields


def _facts(**overrides):
    base = {
        "value": 41_500,
        "sticker_is_original": True,
        "msrp_document": "monroney",
        "exterior_color_seen": "Avalanche Gray",
        "interior_color_seen": "Black Onyx",
        "color_source": "monroney",
    }
    base.update(overrides)
    return base


# --- payload shaping -----------------------------------------------------------


def test_sticker_printed_colors_become_overlay_fields() -> None:
    fields = color_overlay_public_fields(_facts())
    assert fields["sticker_exterior_color"] == "Avalanche Gray"
    assert fields["sticker_interior_color"] == "Black Onyx"
    assert fields["sticker_color_source"] == "window_sticker_photo"
    assert "window sticker" in fields["sticker_color_note"]
    # Distinct keys only: the feed's exterior_color/interior_color are never
    # rewritten by this overlay — the display supersedes, the row survives.
    assert "exterior_color" not in fields
    assert "interior_color" not in fields


def test_photo_observed_colors_never_qualify() -> None:
    assert color_overlay_public_fields(_facts(color_source="photo")) == {}


def test_missing_or_unsourced_facts_say_nothing() -> None:
    assert color_overlay_public_fields(None) == {}
    assert color_overlay_public_fields({}) == {}
    assert color_overlay_public_fields(_facts(color_source=None)) == {}


def test_one_sided_sticker_color_is_served_alone() -> None:
    fields = color_overlay_public_fields(_facts(interior_color_seen=None))
    assert fields["sticker_exterior_color"] == "Avalanche Gray"
    assert "sticker_interior_color" not in fields


def test_monroney_source_with_no_colors_is_nothing() -> None:
    assert (
        color_overlay_public_fields(
            _facts(exterior_color_seen=None, interior_color_seen="  ")
        )
        == {}
    )


# --- the batched car_image_text read carries the color facts -------------------


_CAR_IMAGE_TEXT_DDL = """
CREATE TABLE car_image_text (
    car_id       INTEGER PRIMARY KEY,
    vin          TEXT,
    version      INTEGER NOT NULL DEFAULT 0,
    summary      TEXT,
    has_sticker  INTEGER NOT NULL DEFAULT 0,
    sticker_msrp REAL,
    equipment_count INTEGER NOT NULL DEFAULT 0
);
"""


def _seed_image_text(path, rows):
    import json
    import sqlite3

    conn = sqlite3.connect(str(path))
    try:
        conn.executescript(_CAR_IMAGE_TEXT_DDL)
        conn.executemany(
            "INSERT INTO car_image_text (car_id, version, summary, sticker_msrp) "
            "VALUES (?,?,?,?)",
            [(cid, ver, json.dumps(summary), msrp) for cid, ver, summary, msrp in rows],
        )
        conn.commit()
    finally:
        conn.close()


def test_batched_read_serves_sticker_colors_even_without_a_total(sqlite_inventory) -> None:
    """A sticker whose colors were transcribed but whose total was refused (or
    never read) still yields an entry — ``value: None`` with the color facts —
    while a photo-only observation with no total yields nothing at all."""
    from backend.db.repositories.cars_repo import car_sticker_msrp_values

    _seed_image_text(sqlite_inventory.path, [
        # Total AND printed colors: one entry carries both.
        (1, 100, {
            "msrp_read_directly": True,
            "exterior_color_seen": "Midnight Sky",
            "interior_color_seen": "Global Black",
            "color_source": "monroney",
        }, 112_000.0),
        # Printed colors, no total on the document.
        (2, 100, {
            "exterior_color_seen": "Avalanche Gray",
            "color_source": "monroney",
        }, None),
        # Photo-observed only, no total: nothing servable.
        (3, 100, {
            "exterior_color_seen": "White",
            "interior_color_seen": "Brown",
            "color_source": "photo",
        }, None),
        # Local-OCR era row (version < 100): never served.
        (4, 1, {
            "exterior_color_seen": "Red",
            "color_source": "monroney",
        }, None),
        # Total survives its gates while the colors are photo-observed: the
        # entry says so and the overlay refuses them downstream.
        (5, 100, {
            "msrp_read_directly": True,
            "exterior_color_seen": "White",
            "color_source": "photo",
        }, 55_000.0),
    ])

    out = car_sticker_msrp_values([1, 2, 3, 4, 5])
    assert out[1]["value"] == 112_000
    assert out[1]["exterior_color_seen"] == "Midnight Sky"
    assert out[1]["interior_color_seen"] == "Global Black"
    assert out[1]["color_source"] == "monroney"
    assert out[2]["value"] is None
    assert out[2]["exterior_color_seen"] == "Avalanche Gray"
    assert 3 not in out
    assert 4 not in out
    assert out[5]["value"] == 55_000
    assert out[5]["color_source"] == "photo"

    assert color_overlay_public_fields(out[1])["sticker_exterior_color"] == "Midnight Sky"
    assert color_overlay_public_fields(out.get(3)) == {}
    assert color_overlay_public_fields(out[5]) == {}


def test_refused_total_still_serves_the_printed_colors(sqlite_inventory) -> None:
    """The MSRP provenance gates refuse the TOTAL, never the color lines."""
    from backend.db.repositories.cars_repo import car_sticker_msrp_values

    _seed_image_text(sqlite_inventory.path, [
        (7, 100, {
            "msrp_read_directly": False,  # reconstructed total: value refused
            "exterior_color_seen": "Sonic Gray Pearl",
            "interior_color_seen": "Black",
            "color_source": "monroney",
        }, 39_000.0),
    ])

    out = car_sticker_msrp_values([7])
    assert out[7]["value"] is None
    fields = color_overlay_public_fields(out[7])
    assert fields["sticker_exterior_color"] == "Sonic Gray Pearl"
    assert fields["sticker_interior_color"] == "Black"
