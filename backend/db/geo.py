import math
import pgeocode

_nomi = pgeocode.Nominatim("us")


def zip_to_coords(zip_code):
    """Return (lat, lon) for a US zip code, or None if not found."""
    result = _nomi.query_postal_code(str(zip_code).strip())
    if result is None or math.isnan(result.latitude):
        return None
    return (result.latitude, result.longitude)


def haversine(lat1, lon1, lat2, lon2):
    """Return distance in miles between two lat/lon points."""
    R = 3958.8
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    return R * 2 * math.asin(math.sqrt(a))
