"""Configuration du logging (clair, par source, succès/erreurs)."""
from __future__ import annotations

import logging
import sys


def setup_logging(level: str = "INFO") -> None:
    # Force l'UTF-8 sur la sortie (évite les UnicodeEncodeError sous Windows
    # PowerShell, dont l'encodage par défaut casse sur les emojis ⚠️ etc.).
    try:
        sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
    except (AttributeError, ValueError):
        pass
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        logging.Formatter(
            "%(asctime)s | %(levelname)-7s | %(name)-22s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    root = logging.getLogger()
    root.setLevel(getattr(logging, level.upper(), logging.INFO))
    # Évite les handlers dupliqués en cas de re-init (reprise après crash).
    root.handlers.clear()
    root.addHandler(handler)


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
