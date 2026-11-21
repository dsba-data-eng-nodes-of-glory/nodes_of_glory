"""Location lookup and weather configuration acceptance tests. No internet needed."""
import pytest

from src.ingestion import station_location_lookup as locations
from src.ingestion.raw_station_loader import parse_weather_locations
from backend import station_api
from fastapi import HTTPException


class FakeResponse:
    status_code = 200

    def raise_for_status(self):
        return None

    def json(self):
        return {"results": [
            {"id": 1, "name": "Dortmund", "country": "Germany", "admin1": "North Rhine-Westphalia",
             "latitude": 51.5136, "longitude": 7.4653},
            {"id": 2, "name": "Dortmund", "country": "Otherland", "admin1": "Area",
             "latitude": 48.0, "longitude": 8.0},
        ]}


def test_location_lookup_returns_choices_and_coordinates(monkeypatch):
    seen = []
    def fake_get(url, **kwargs):
        seen.append((url, kwargs["params"]["name"]))
        return FakeResponse()
    locations._search_location.cache_clear()
    monkeypatch.setattr(locations.requests, "get", fake_get)
    payload = station_api.resolve_station_locations(station_api.ResolveStationLocationsRequest(
        locations=["Dortmund, Germany"]
    ))
    assert seen == [(locations.API, "Dortmund, Germany")]
    matches = payload["locations"][0]["candidates"]
    assert len(matches) == 2
    assert matches[0]["latitude"] == 51.5136
    assert "Germany" in matches[0]["display"]
    canonical = locations.confirmed_location_lines([matches[0]])
    config = parse_weather_locations(canonical)
    assert config["sites"] == [{"location": "Dortmund", "latitude": 51.5136, "longitude": 7.4653}]


def test_geocoder_429_has_manual_fallback(monkeypatch):
    class RateLimited:
        status_code = 429
    locations._search_location.cache_clear()
    monkeypatch.setattr(locations.requests, "get", lambda *args, **kw: RateLimited())
    with pytest.raises(HTTPException) as error:
        station_api.resolve_station_locations(station_api.ResolveStationLocationsRequest(locations=["Essen, Germany"]))
    assert error.value.status_code == 422
    assert "manually" in error.value.detail


@pytest.mark.parametrize("queries", [[], ["X"] * 13, ["Essen", "essen"], ["a" * 121]])
def test_bad_location_input_fails_before_network(queries):
    with pytest.raises(locations.LocationLookupError):
        locations.validate_place_queries(queries)


def test_manual_coordinates_not_dependent_on_geocoder():
    config = parse_weather_locations("Essen, 51.4556, 7.0116\nCologne, 50.9375, 6.9603")
    assert len(config["sites"]) == 2


def test_location_names_with_commas_can_be_saved():
    canonical = locations.confirmed_location_lines([{
        "location": "Dortmund, North Rhine-Westphalia, Germany",
        "latitude": 51.5136, "longitude": 7.4653
    }])
    weather = parse_weather_locations(canonical)
    assert weather["sites"][0]["location"] == "Dortmund, North Rhine-Westphalia, Germany"
