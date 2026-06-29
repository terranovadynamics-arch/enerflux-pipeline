"""Tests du moteur de scraping générique (JSON-LD schema.org) — sans réseau."""
from __future__ import annotations

from dealhunter.models import Query
from dealhunter.sources._scraper import ScraperSource, _guess_ref, _to_float


class _DummyScraper(ScraperSource):
    name = "dummy"
    search_url_template = "https://example.test/search?q={q}"


QUERY = Query(
    brand="Rolex", model="Submariner", reference="126610LN",
    keywords="Rolex Submariner 126610LN", max_price_usd=50000,
)

HTML = """
<html><head>
<script type="application/ld+json">
{"@type":"Product","name":"Rolex Submariner 126610LN 2022",
 "image":["https://img/a1.jpg","https://img/a2.jpg"],
 "offers":{"@type":"Offer","price":"12600","priceCurrency":"USD","url":"https://x/itm/1"}}
</script>
<script type="application/ld+json">
{"@graph":[
  {"@type":"WebPage"},
  {"@type":"Product","name":"Rolex Submariner","offers":{"price":"13900","priceCurrency":"USD","url":"https://x/itm/2"}}
]}
</script>
<script type="application/ld+json">
{"@type":"Product","name":"Rolex Submariner EUR","offers":{"price":"11000","priceCurrency":"EUR","url":"https://x/itm/eur"}}
</script>
<script type="application/ld+json">{ this is not valid json }</script>
</head><body></body></html>
"""


def test_jsonld_extracts_usd_products():
    src = _DummyScraper()
    listings = src._parse(HTML, QUERY)
    # 2 produits USD valides ; l'EUR est ignoré, le JSON invalide aussi.
    assert len(listings) == 2
    prices = sorted(l.asking_price_usd for l in listings)
    assert prices == [12600.0, 13900.0]


def test_jsonld_uses_query_identity_and_source():
    src = _DummyScraper()
    first = src._parse(HTML, QUERY)[0]
    assert first.brand == "Rolex"
    assert first.model == "Submariner"
    assert first.reference == "126610LN"
    assert first.source == "dummy"
    assert first.url.startswith("https://x/itm/")
    assert first.images and first.images[0].startswith("https://img/")


def test_jsonld_skips_non_usd_currency():
    src = _DummyScraper()
    listings = src._parse(HTML, QUERY)
    assert all(l.currency_original == "USD" for l in listings)


def test_empty_when_no_jsonld():
    src = _DummyScraper()
    assert src._parse("<html><body>rien</body></html>", QUERY) == []


def test_price_filter_respects_query_max():
    """Le filtre prix est appliqué au niveau Source.fetch/_match, pas au parse,
    mais le parseur ne doit jamais renvoyer de prix nul/négatif."""
    html = '<script type="application/ld+json">{"@type":"Product",' \
           '"name":"x","offers":{"price":"0","priceCurrency":"USD","url":"u"}}</script>'
    assert _DummyScraper()._parse(html, QUERY) == []


def test_helpers():
    assert _to_float("$12,600") == 12600.0
    assert _to_float(None) is None
    assert _to_float("n/a") is None
    assert _guess_ref("Rolex Submariner 126610LN full set") == "126610LN"
    assert _guess_ref("no reference here") is None
