"""The writer-side filter that keeps a sticker's own price lines out of the registry.

``_is_vehicle_price_row`` guards ``record_package_observations``: without it the
sticker's BASE VEHICLE PRICE / TOTAL VEHICLE PRICE / destination lines were recorded
as option prices ('Base' at $198,300 on a Mercedes S-Class, 'i8Roadster' at
$163,300). See the block comment in backend/enrichment/package_registry.py.
"""

from __future__ import annotations

from backend.enrichment.window_sticker_service import _is_vehicle_price_row

_S_CLASS = {"make": "Mercedes-Benz", "model": "S-Class", "trim": "Maybach S 580"}
_I8 = {"make": "BMW", "model": "i8", "trim": "Roadster"}


def test_base_name_is_dropped() -> None:
    # The live 'Base' @ $198,300 row on a Mercedes S-Class.
    assert _is_vehicle_price_row("Base", 198300, _S_CLASS, None)
    assert _is_vehicle_price_row("BASE", 198300, _S_CLASS, None)
    assert _is_vehicle_price_row("  base  ", 50000, _S_CLASS, None)


def test_empty_name_is_dropped() -> None:
    assert _is_vehicle_price_row("", 1000, _S_CLASS, None)
    assert _is_vehicle_price_row(None, 1000, _S_CLASS, None)
    assert _is_vehicle_price_row("   ", 1000, _S_CLASS, None)


def test_model_trim_echo_is_dropped() -> None:
    # The live 'i8Roadster' @ $163,300 row: the car's own model+trim string
    # echoed back as an "option" name, case/space-insensitively.
    assert _is_vehicle_price_row("i8Roadster", 163300, _I8, None)
    assert _is_vehicle_price_row("i8 Roadster", 163300, _I8, None)
    assert _is_vehicle_price_row("I8 ROADSTER", 163300, _I8, None)
    assert _is_vehicle_price_row("S-Class", 100000, _S_CLASS, None)  # bare model
    # Bare trim is NOT identity: trims are frequently real package names that
    # stickers itemize with their own price (BMW trim 'M Sport' with an
    # 'M Sport' $2,550 line) — dropping them silently lost real option prices.
    assert not _is_vehicle_price_row(
        "M Sport", 2550, {"make": "BMW", "model": "330i", "trim": "M Sport"}, None
    )
    assert not _is_vehicle_price_row("Roadster", 163300, _I8, None)


def test_total_price_pattern_is_dropped() -> None:
    # The live SiriusXM row: parse noise glued neighbouring text onto the
    # TOTAL VEHICLE PRICE label, so the match must be a substring match.
    assert _is_vehicle_price_row(
        "WHICHEVER COMES FIRST ... SIRIUSXM AUDIO W/ TRIAL TOTAL VEHICLE PRICE*",
        161990,
        _S_CLASS,
        None,
    )
    assert _is_vehicle_price_row("Total Vehicle Price", 90000, _S_CLASS, None)
    assert _is_vehicle_price_row("TOTAL PRICE", 90000, _S_CLASS, None)
    assert _is_vehicle_price_row("Total MSRP", 90000, _S_CLASS, None)
    assert _is_vehicle_price_row("Base Price", 85000, _S_CLASS, None)
    assert _is_vehicle_price_row("BASE MSRP", 85000, _S_CLASS, None)
    assert _is_vehicle_price_row("MSRP", 85000, _S_CLASS, None)


def test_destination_and_fee_patterns_are_dropped() -> None:
    assert _is_vehicle_price_row("Destination Charge", 1150, _S_CLASS, None)
    assert _is_vehicle_price_row("Destination & Delivery", 1395, _S_CLASS, None)
    assert _is_vehicle_price_row("Destination and Delivery", 1395, _S_CLASS, None)
    assert _is_vehicle_price_row("Delivery Charge", 995, _S_CLASS, None)
    assert _is_vehicle_price_row("Gas Guzzler Tax", 1300, _S_CLASS, None)


def test_price_equal_to_sticker_msrp_is_dropped() -> None:
    # Whatever the line is labelled, a price identical to the sticker's own
    # parsed MSRP is the car's price, not an option's.
    assert _is_vehicle_price_row("Vehicle", 161990, _S_CLASS, 161990)
    assert _is_vehicle_price_row("Vehicle", 161990.0, _S_CLASS, 161990)
    # ...but only when the MSRP is actually known.
    assert not _is_vehicle_price_row("Premium Package", 4550, _S_CLASS, None)


def test_genuine_expensive_option_passes_through() -> None:
    # A real five-figure option must NOT be filtered: the registry exists to
    # learn exactly these prices.
    assert not _is_vehicle_price_row(
        "Executive Rear Seat Package Plus", 12500, _S_CLASS, 198300
    )
    assert not _is_vehicle_price_row("MAGIC SKY CONTROL", 4950, _S_CLASS, 198300)
    assert not _is_vehicle_price_row("Laserlight Package", 6300, _I8, 163300)
    # An option whose name merely CONTAINS the trim word but is not the car's
    # identity string also passes ('Roadster Package' is not 'Roadster').
    assert not _is_vehicle_price_row("Roadster Wind Deflector", 450, _I8, 163300)


def test_unpriced_named_option_passes_through() -> None:
    # Named-but-unpriced options are legitimate observations.
    assert not _is_vehicle_price_row("Premium Package", None, _S_CLASS, 198300)
