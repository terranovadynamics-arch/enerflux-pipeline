"""Tests de la couche transport (relais Cloudflare) — sans réseau."""
from __future__ import annotations

from dealhunter import transport
from dealhunter.config import get_settings


def test_target_url_merges_params():
    out = transport.target_url("https://api.ebay.com/s", {"q": "rolex", "limit": 50})
    assert out.startswith("https://api.ebay.com/s?")
    assert "q=rolex" in out
    assert "limit=50" in out


def test_target_url_no_params_unchanged():
    assert transport.target_url("https://x/y", None) == "https://x/y"


def test_relay_endpoint_encodes_target():
    ep = transport.relay_endpoint(
        "https://relay.example.workers.dev/",
        "https://api.ebay.com/buy?q=a b",
    )
    # pas de double slash, cible encodée (espace -> %20, ? -> %3F)
    assert ep.startswith("https://relay.example.workers.dev/fetch?url=")
    assert "%3A%2F%2Fapi.ebay.com" in ep
    assert "%20" in ep  # l'espace est encodé
    assert "/fetch?url=" in ep


def test_request_uses_relay_when_configured(monkeypatch):
    monkeypatch.setenv("RELAY_URL", "https://relay.example.workers.dev")
    monkeypatch.setenv("RELAY_SECRET", "s3cr3t")
    get_settings.cache_clear()

    captured = {}

    def fake_request(method, url, **kwargs):
        captured["method"] = method
        captured["url"] = url
        captured["headers"] = kwargs.get("headers", {})

        class R:  # réponse factice
            status_code = 200

        return R()

    monkeypatch.setattr(transport.httpx, "request", fake_request)
    transport.request("GET", "https://api.ebay.com/x", params={"q": "rolex"})
    get_settings.cache_clear()

    assert captured["url"].startswith("https://relay.example.workers.dev/fetch?url=")
    # la cible (et ses params) est encodée dans le paramètre url=
    assert "q%3Drolex" in captured["url"]
    assert captured["headers"].get("X-Relay-Secret") == "s3cr3t"


def test_request_direct_when_no_relay(monkeypatch):
    monkeypatch.delenv("RELAY_URL", raising=False)
    get_settings.cache_clear()

    captured = {}

    def fake_request(method, url, **kwargs):
        captured["url"] = url
        captured["headers"] = kwargs.get("headers", {})

        class R:
            status_code = 200

        return R()

    monkeypatch.setattr(transport.httpx, "request", fake_request)
    transport.request("GET", "https://api.ebay.com/x", params={"q": "rolex"})
    get_settings.cache_clear()

    assert captured["url"].startswith("https://api.ebay.com/x?")
    assert "X-Relay-Secret" not in captured["headers"]
