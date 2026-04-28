from flask import render_template

from backend.db.inventory_db import get_filter_options


def listings_page(*, listings_poll_ms: int = 0):
    return render_template(
        "listings.html",
        options=get_filter_options(),
        active={},
        initial_grid_cars=[],
        listings_poll_ms=int(listings_poll_ms),
    )
