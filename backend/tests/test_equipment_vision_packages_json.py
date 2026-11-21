import json

from backend.vision.equipment_vision import analyze_car_equipment_from_gallery


def test_oversized_packages_stay_valid_json():
    big = {"observed_features": [f"feature number {i}" for i in range(2000)], "k": "v"}
    out, _ = analyze_car_equipment_from_gallery(
        {"packages": None},
        gallery_urls=["https://x.test/a.jpg"],
        analyze_url=lambda row, url: {"x": 1},
        merge_observations=lambda cur, vis: json.dumps(big),
        force=True,
    )
    assert out is not None and len(out) <= 8000
    pj = json.loads(out)
    assert pj["k"] == "v" and pj["observed_features"]
    assert "photo_equipment_scanned_at" in pj
