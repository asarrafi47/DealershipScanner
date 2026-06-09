"""Tests for VDP gallery URL junk filtering."""
from backend.scanner.utils.gallery_url_filter import filter_vdp_gallery_urls, is_junk_vdp_gallery_url


def test_rejects_certified_and_logo_urls() -> None:
    assert is_junk_vdp_gallery_url("https://cdn.example/bmw-certified-banner.jpg")
    assert is_junk_vdp_gallery_url("https://cdn.example/m-logo.png")
    assert is_junk_vdp_gallery_url("https://pictures.dealer.com/cfx/abc123.jpg")


def test_keeps_inventory_photo_urls() -> None:
    good = "https://pictures.dealer.com/irvinebmw/1234/2023/BMW/i4/photo.jpg"
    assert not is_junk_vdp_gallery_url(good)


def test_rejects_oem_stock_and_tracking_pixels() -> None:
    assert is_junk_vdp_gallery_url(
        "https://pictures.dealer.com/generic-bmw-OEM_VIN_STOCK_PHOTOS/abc.jpg?impolicy=resize&w=414"
    )
    assert is_junk_vdp_gallery_url("https://tags.srv.stackadapt.com/sa.jpeg")
    assert is_junk_vdp_gallery_url(
        "https://pictures.dealer.com/i/dealer/abc.jpg?impolicy=downsize_bkpt&w=325"
    )


def test_filter_preserves_order() -> None:
    urls = [
        "https://cdn.example/vehicle/1.jpg",
        "https://cdn.example/bmw-certified.jpg",
        "https://cdn.example/vehicle/2.jpg",
    ]
    out = filter_vdp_gallery_urls(urls)
    assert out == [
        "https://cdn.example/vehicle/1.jpg",
        "https://cdn.example/vehicle/2.jpg",
    ]
