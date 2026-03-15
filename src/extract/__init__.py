# Extraction module - provider scrapers

from src.extract.base_scraper import BaseScraper
from src.extract.pasta_evangelists import PastaEvangelistsScraper
from src.extract.comptoir_bakery import ComptoirBakeryScraper
from src.extract.caravan_coffee import CaravanCoffeeScraper

__all__ = [
    "BaseScraper",
    "PastaEvangelistsScraper",
    "ComptoirBakeryScraper",
    "CaravanCoffeeScraper",
]
