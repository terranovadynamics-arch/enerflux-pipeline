"""Deal Hunter — agent autonome de surveillance du marché des montres de luxe d'occasion.

Architecture agent principal + sous-agents :
    orchestrator  -> boucle de surveillance, dédoublonnage, scoring final
    scraper       -> récupère les annonces depuis chaque source
    pricer        -> établit le prix de référence marché (médiane) par référence
    scorer        -> calcule le deal_score et décide si on alerte
    notifier      -> envoie l'alerte formatée (email / telegram)
"""

__version__ = "1.0.0"
