"""Resolve customer-supplied weather sampling place names to WGS84 coordinates.

Only used when a user deliberately presses Find coordinates in Station Management.
Registration itself reads already-confirmed coordinates: it never silently guesses.
"""
from __future__ import annotations

import math
from functools import lru_cache

import requests

API = "https://geocoding-api.open-meteo.com/v1/search"
MAX_PLACES = 12
MAX_RESULTS = 5


class LocationLookupError(ValueError):
    pass


def validate_place_queries(queries: list[str]) -> list[str]:
    if not isinstance(queries, list):
        raise LocationLookupError("Provide place names, one per line.")
    clean = [q.strip() for q in queries if isinstance(q, str) and q.strip()]
    if not 1 <= len(clean) <= MAX_PLACES:
        raise LocationLookupError("Enter between 1 and 12 weather sampling places.")
    if any(len(q) > 120 for q in clean):
        raise LocationLookupError("Each location name must be at most 120 characters.")
    if len({q.casefold() for q in clean}) != len(clean):
        raise LocationLookupError("Remove duplicate place names before looking up coordinates.")
    return clean


@lru_cache(maxsize=256)
def _search_location(name: str) -> tuple[tuple, ...]:
    try:
        response = requests.get(
            API,
            params={"name": name, "count": MAX_RESULTS, "language": "en", "format": "json"},
            timeout=(4, 12),
        )
        if response.status_code == 429:
            raise LocationLookupError(
                "Open-Meteo location lookup is temporarily rate limited (HTTP 429). "
                "Retry later or enter coordinates manually."
            )
        response.raise_for_status()
        payload = response.json()
    except LocationLookupError:
        raise
    except (requests.RequestException, ValueError) as exc:
        raise LocationLookupError(
            f"Location search unavailable for {name!r}. Enter coordinates manually or try again later."
        ) from exc

    candidates = []
    for candidate in payload.get("results") or []:
        try:
            latitude = float(candidate["latitude"])
            longitude = float(candidate["longitude"])
        except (ValueError, TypeError, KeyError):
            continue
        if not (math.isfinite(latitude) and math.isfinite(longitude) and
                -90 <= latitude <= 90 and -180 <= longitude <= 180):
            continue
        label = str(candidate.get("name") or name).strip()
        region = str(candidate.get("admin1") or "").strip()
        country = str(candidate.get("country") or "").strip()
        display = ", ".join(item for item in (label, region, country) if item)
        candidates.append((label, latitude, longitude, display, str(candidate.get("id") or "")))
    return tuple(candidates)


def resolve_places(queries: list[str]) -> list[dict]:
    """Return possible locations; users must confirm before station registration."""
    clean = validate_place_queries(queries)
    result = []
    for name in clean:
        options = _search_location(name)
        if not options:
            raise LocationLookupError(
                f"No coordinates found for {name!r}. Try 'City, Country' or enter coordinates manually."
            )
        result.append({
            "query": name,
            "candidates": [
                {"location": item[0], "latitude": item[1], "longitude": item[2],
                 "display": item[3], "id": item[4]}
                for item in options
            ],
        })
    return result


def confirmed_location_lines(locations: list[dict]) -> str:
    """Serialize reviewed places to the existing backend weather-locations contract."""
    if not 1 <= len(locations) <= MAX_PLACES:
        raise LocationLookupError("Confirm between 1 and 12 locations.")
    lines = []
    for index, item in enumerate(locations, 1):
        try:
            name = str(item["location"]).strip()
            lat, lon = float(item["latitude"]), float(item["longitude"])
        except (KeyError, TypeError, ValueError) as exc:
            raise LocationLookupError(f"Invalid confirmed location #{index}.") from exc
        if not name or '\n' in name or not (-90 <= lat <= 90 and -180 <= lon <= 180) or not (math.isfinite(lat) and math.isfinite(lon)):
            raise LocationLookupError(f"Invalid confirmed location #{index}.")
        lines.append(f"{name}, {lat:.6f}, {lon:.6f}")
    return "\n".join(lines)
