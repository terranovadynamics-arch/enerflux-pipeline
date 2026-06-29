"""Point d'entrée : `python -m dealhunter`.

Options :
  python -m dealhunter            # boucle continue (mode service)
  python -m dealhunter --once     # un seul cycle (utile pour cron / tests)
  python -m dealhunter --check    # vérifie la config et les sources puis quitte
"""
from __future__ import annotations

import argparse
import sys

from .config import get_settings
from .logging_conf import get_logger, setup_logging
from .orchestrator import Orchestrator
from .sources import load_enabled_sources


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="dealhunter", description="Deal Hunter — montres de luxe")
    parser.add_argument("--once", action="store_true", help="exécute un seul cycle puis quitte")
    parser.add_argument("--check", action="store_true", help="vérifie config/sources puis quitte")
    args = parser.parse_args(argv)

    settings = get_settings()
    setup_logging(settings.log_level)
    log = get_logger("main")

    if args.check:
        log.info("Vérification de la configuration...")
        sources = load_enabled_sources()
        log.info("%d source(s) active(s). Budget max=$%s, décote>=%.0f%%, poll=%dmin, fixtures=%s",
                 len(sources), f"{settings.budget_max_usd:,.0f}",
                 settings.discount_threshold * 100, settings.poll_interval_min,
                 settings.use_fixtures)
        log.info("Canaux de notification: %s", ", ".join(settings.notify_channels))
        return 0

    orch = Orchestrator()
    if args.once:
        deals = orch.run_once()
        log.info("Cycle unique terminé : %d deal(s).", len(deals))
        return 0

    orch.run_forever()
    return 0


if __name__ == "__main__":
    sys.exit(main())
