"""Drivetrain: storage value, shopper-facing label, and the verified flag."""

from __future__ import annotations

from backend.enrichment.verified_specs.sources import SpecSources


def resolve_drivetrain(src: SpecSources):
    """Priority: trim decoder > EPA, overridden by a disagreeing vPIC decode;
    then dealer > model_specs > vPIC when still blank.

    NHTSA vPIC outranks the feed for drivetrain (policy 2026-09-23). AWD and 4WD
    are the same wheels driven, so only a real disagreement (RWD vs 4WD, FWD vs
    AWD) is overridden; a blank decode leaves the cascade alone.
    """
    from backend.enrichment import knowledge_engine as ke

    regex, epa, vpic, dict_specs = src.regex, src.epa, src.vpic, src.dict_specs
    dealer_drive = src.dealer_drive
    drive_ver = regex.get("drivetrain") or epa.get("drivetrain")
    _vpic_drive = vpic.get("drivetrain")
    if _vpic_drive:
        from backend.vehicle_facts.drivetrain import normalize_drivetrain, same_drive_wheels

        _a = normalize_drivetrain(dealer_drive, "dealer")
        _b = normalize_drivetrain(_vpic_drive, "vpic")
        if _a and _b and not same_drive_wheels(_a, _b):
            drive_ver = _vpic_drive
    if not drive_ver and dealer_drive and not ke._is_na_spec(dealer_drive):
        drive_ver = dealer_drive
    if not drive_ver and dict_specs and dict_specs.get("drivetrain"):
        drive_ver = str(dict_specs["drivetrain"]).strip()
    if not drive_ver and vpic.get("drivetrain"):
        drive_ver = vpic["drivetrain"]
    return drive_ver


def drivetrain_display(src: SpecSources, drive_ver) -> tuple[str, bool]:
    """``(drivetrain_display, drivetrain_verified)``.

    Regex/EPA first so xDrive/4MATIC in title wins over dealer placeholders.
    """
    from backend.enrichment import knowledge_engine as ke

    dealer_drive = src.dealer_drive
    if drive_ver:
        display_drive = drive_ver
    elif not ke._is_na_spec(dealer_drive):
        display_drive = dealer_drive
    else:
        display_drive = ""

    verified = bool(
        drive_ver
        and (display_drive or "").strip().upper() == (drive_ver or "").strip().upper()
        and ke._is_na_spec(dealer_drive)
    )

    blob_full = f"{src.title} {src.trim} {src.model}".strip()
    return ke._drivetrain_ui_label(display_drive, src.make, blob_full), verified
