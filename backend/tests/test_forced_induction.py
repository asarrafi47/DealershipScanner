"""NA engines must not be labelled turbo by make/model era inference (B-019/B-020/B-021)."""
from backend.utils.forced_induction import classify_forced_induction as fi


def _c(make, model, trim="", year=2022, cyl=None, el=None, ft=None):
    return fi(make, model, trim, year, cyl, el, fuel_type=ft)


def test_ford_na_engines_not_turbo():
    assert _c("Ford", "F-150", "XL STX", cyl=8, el=5.0) is None
    assert _c("Ford", "F-150", "XL", cyl=6, el=3.3) is None
    assert _c("Ford", "Focus", "SE", cyl=4, el=2.0) is None
    assert _c("Ford", "Focus", "ST-Line", cyl=4, el=2.0) is None
    assert _c("Ford", "Edge", "SEL", cyl=6, el=3.5) is None


def test_ford_turbo_evidence_still_labels_turbo():
    assert _c("Ford", "F-150", "XLT", cyl=6, el=2.7) == "Twin Turbocharged"
    assert _c("Ford", "Escape", "ST-Line EcoBoost", cyl=4, el=2.0) == "Turbocharged"
    assert _c("Ford", "Fiesta", "ST", cyl=4, el=1.6) == "Turbocharged"
    assert _c("Ford", "Explorer", "ST", cyl=6, el=3.0) == "Twin Turbocharged"
    assert _c("Ford", "Ecosport", "SE", cyl=3, el=1.0) == "Turbocharged"


def test_vw_na_vr6_not_turbo_but_tsi_is():
    assert _c("Volkswagen", "Atlas", "SEL", cyl=6, el=3.6) is None
    assert _c("Volkswagen", "Atlas", "SE") is None
    assert _c("Volkswagen", "Jetta", "S", cyl=4, el=1.4) == "Turbocharged"
    assert _c("Volkswagen", "GTI", "S") == "Turbocharged"


def test_genesis_lincoln_na_engines():
    assert _c("Genesis", "G80", "3.8", cyl=6, el=3.8) is None
    assert _c("Genesis", "G90", "5.0", cyl=8, el=5.0) is None
    assert _c("Genesis", "G70", "Sport", year=2022, cyl=6, el=3.3) == "Twin Turbocharged"
    assert _c("Genesis", "G70", "Advanced", cyl=4, el=2.0) == "Turbocharged"
    assert _c("Lincoln", "MKZ", "Reserve", year=2018, cyl=6, el=3.7) is None
    assert _c("Lincoln", "Aviator", "Reserve", cyl=6, el=3.0) == "Twin Turbocharged"
    assert _c("Lincoln", "Corsair", "Reserve", cyl=4, el=2.0) == "Turbocharged"


def test_hyundai_kia_na_1_6_not_turbo():
    assert _c("Hyundai", "Accent", "SE", cyl=4, el=1.6) is None
    assert _c("Kia", "Rio", "S", cyl=4, el=1.6) is None
    assert _c("Kia", "Niro", "LX", cyl=4, el=1.6, ft="Hybrid") is None
    assert _c("Kia", "Seltos", "S", cyl=4, el=1.6) == "Turbocharged"


def test_n_token_is_word_boundary():
    assert _c("Hyundai", "Santa Fe", "Nightfall Edition AWD", cyl=4, el=2.5) is None
    assert _c("Hyundai", "Tucson", "SEL Premium ", cyl=4, el=2.5) is None
    assert _c("Hyundai", "Elantra N", "", cyl=4, el=2.0) == "Turbocharged"
    assert _c("Hyundai", "Elantra", "N Line", cyl=4, el=1.6) == "Turbocharged"


def test_si_token_is_word_boundary():
    assert _c("Honda", "Pilot", "Elite Sensing", cyl=6, el=3.5) is None
    assert _c("Honda", "Civic", "Si", cyl=4, el=1.5) == "Turbocharged"


def test_honda_fit_and_lexus_ux_na():
    assert _c("Honda", "Fit", "LX", cyl=4, el=1.5) is None
    assert _c("Lexus", "UX 200", "", cyl=4, el=2.0) is None
    assert _c("Lexus", "UX 250h", "", cyl=4, el=2.0, ft="Hybrid") is None
    assert _c("Lexus", "IS 300", "", year=2018, cyl=4, el=2.0) == "Turbocharged"
