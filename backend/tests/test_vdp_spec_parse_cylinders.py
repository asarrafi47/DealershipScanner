"""B-018 combustion tokens beat EV words; B-068 hex swatches are not colours."""
from backend.scanner.utils.vdp_spec_parse import _cylinders_from_engine_blob, _valid_color


def test_hybrid_electric_v6_is_six_cylinders() -> None:
    assert _cylinders_from_engine_blob("3.5L V6 Hybrid Electric") == 6
    assert _cylinders_from_engine_blob("2.5L I4 Plug-in Electric") == 4


def test_pure_ev_is_zero_cylinders() -> None:
    assert _cylinders_from_engine_blob("Electric Motor 75 kWh") == 0


def test_hex_not_a_colour() -> None:
    assert _valid_color("#ffffff") is None
    assert _valid_color("FF00AA") is None
    assert _valid_color("#fff") is None
    assert _valid_color("Pearl White") == "Pearl White"
    assert _valid_color("Beige") == "Beige"
