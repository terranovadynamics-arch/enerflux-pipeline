"""Catalogue des cibles : modèles liquides à forte valeur de revente.

Adaptable librement : ajoutez/retirez des entrées sans toucher au reste du code.
Chaque cible génère une ou plusieurs `Query` envoyées aux sources.
"""
from __future__ import annotations

from .models import Query

# (marque, modèle, [références])
TARGETS: list[tuple[str, str, list[str]]] = [
    # --- Rolex ---------------------------------------------------------------
    ("Rolex", "Submariner", ["126610LN", "124060", "126610LV", "116610LN"]),
    ("Rolex", "GMT-Master II", ["126710BLRO", "126710BLNR", "116710LN"]),
    ("Rolex", "Daytona", ["116500LN", "126500LN", "116520"]),
    ("Rolex", "Datejust 41", ["126334", "126300"]),
    ("Rolex", "Explorer", ["124270", "214270"]),
    ("Rolex", "Sea-Dweller", ["126600", "116600"]),
    ("Rolex", "Oyster Perpetual", ["124300", "114300"]),
    # --- Audemars Piguet -----------------------------------------------------
    ("Audemars Piguet", "Royal Oak", ["15400", "15500", "15510", "15202"]),
    ("Audemars Piguet", "Royal Oak Offshore", ["26470", "15710"]),
    # --- Patek Philippe ------------------------------------------------------
    ("Patek Philippe", "Nautilus", ["5711", "5712", "5980"]),
    ("Patek Philippe", "Aquanaut", ["5167", "5168"]),
    ("Patek Philippe", "Calatrava", ["5227", "6119"]),
]


def build_queries(max_price_usd: float) -> list[Query]:
    """Construit la liste des requêtes à partir du catalogue."""
    queries: list[Query] = []
    for brand, model, refs in TARGETS:
        if refs:
            for ref in refs:
                queries.append(
                    Query(
                        brand=brand,
                        model=model,
                        reference=ref,
                        keywords=f"{brand} {model} {ref}",
                        max_price_usd=max_price_usd,
                    )
                )
        else:
            queries.append(
                Query(
                    brand=brand,
                    model=model,
                    keywords=f"{brand} {model}",
                    max_price_usd=max_price_usd,
                )
            )
    return queries
