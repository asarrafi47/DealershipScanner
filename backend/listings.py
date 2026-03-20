from flask import render_template
from backend.db.inventory_db import search_cars, get_filter_options

def listings_page():
    results = search_cars()
    return render_template("listings.html", options=get_filter_options(),
                           results=results, active={})
