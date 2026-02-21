"""
Product catalog and per-(product, size) stock tracking.
"""

import random
import threading

PRODUCTS = [
    {"id": "AJ1-CHI",  "name": "Air Jordan 1 Retro High OG 'Chicago'",     "sku": "555088-101", "price": 180, "base_stock": 500,  "hype_level": 0.95},
    {"id": "YZ-350Z",  "name": "Yeezy Boost 350 V2 'Zebra'",               "sku": "CP9654",     "price": 220, "base_stock": 300,  "hype_level": 0.90},
    {"id": "DF-PANDA", "name": "Nike Dunk Low 'Panda'",                     "sku": "DD1391-100", "price": 110, "base_stock": 800,  "hype_level": 0.70},
    {"id": "AF1-WHT",  "name": "Air Force 1 '07 Triple White",              "sku": "CW2288-111", "price": 90,  "base_stock": 2000, "hype_level": 0.30},
    {"id": "AM90-INF", "name": "Air Max 90 'Infrared'",                     "sku": "CT1685-100", "price": 130, "base_stock": 600,  "hype_level": 0.65},
    {"id": "SB-TRAV",  "name": "SB Dunk Low x Travis Scott",                "sku": "CT5053-001", "price": 150, "base_stock": 200,  "hype_level": 0.98},
    {"id": "AJ4-BRD",  "name": "Air Jordan 4 Retro 'Bred'",                "sku": "308497-060", "price": 200, "base_stock": 400,  "hype_level": 0.88},
    {"id": "NB-550G",  "name": "New Balance 550 'Green'",                   "sku": "BB550WT1",   "price": 110, "base_stock": 700,  "hype_level": 0.55},
    {"id": "AM1-87",   "name": "Air Max 1/87 'Big Bubble'",                 "sku": "DQ3989-100", "price": 160, "base_stock": 350,  "hype_level": 0.75},
    {"id": "YZ-SLD",   "name": "Yeezy Slide 'Onyx'",                       "sku": "HQ6448",     "price": 70,  "base_stock": 1500, "hype_level": 0.60},
    {"id": "AJ11-CON", "name": "Air Jordan 11 Retro 'Concord'",            "sku": "378037-100", "price": 220, "base_stock": 350,  "hype_level": 0.92},
    {"id": "DF-LOTV",  "name": "Nike Dunk Low x Off-White 'Lot 50'",       "sku": "DM1602-001", "price": 180, "base_stock": 150,  "hype_level": 0.97},
    {"id": "AF1-TS",   "name": "Air Force 1 Low x Travis Scott 'Cactus'",  "sku": "AQ4211-002", "price": 160, "base_stock": 250,  "hype_level": 0.96},
    {"id": "AM97-SW",  "name": "Air Max 97/1 Sean Wotherspoon",            "sku": "AJ4219-400", "price": 160, "base_stock": 180,  "hype_level": 0.94},
    {"id": "SB-HEIN",  "name": "SB Dunk Low 'Heineken'",                   "sku": "304292-302", "price": 130, "base_stock": 100,  "hype_level": 0.85},
    {"id": "AJ1-UNC",  "name": "Air Jordan 1 Retro High OG 'UNC'",        "sku": "555088-117", "price": 180, "base_stock": 450,  "hype_level": 0.82},
    {"id": "NKR-VPR",  "name": "Nike Vaporfly NEXT% 3",                    "sku": "DV4129-100", "price": 250, "base_stock": 600,  "hype_level": 0.45},
    {"id": "AJ3-WCM",  "name": "Air Jordan 3 Retro 'White Cement'",       "sku": "DN3707-100", "price": 200, "base_stock": 400,  "hype_level": 0.86},
    {"id": "DF-ARGN",  "name": "Nike Dunk Low 'Argon'",                    "sku": "DM0121-400", "price": 110, "base_stock": 900,  "hype_level": 0.50},
    {"id": "AM-PLUS",  "name": "Nike Air Max Plus 'Hyper Blue'",           "sku": "BQ4629-003", "price": 175, "base_stock": 550,  "hype_level": 0.40},
    {"id": "AJ1-TS",   "name": "Air Jordan 1 x Travis Scott 'Mocha'",     "sku": "CD4487-100", "price": 175, "base_stock": 200,  "hype_level": 0.99},
    {"id": "SB-STSY",  "name": "SB Dunk Low x Stussy 'Cherry'",           "sku": "DO9395-600", "price": 130, "base_stock": 250,  "hype_level": 0.80},
    {"id": "AJ5-OW",   "name": "Air Jordan 5 x Off-White 'Sail'",         "sku": "DH8565-100", "price": 225, "base_stock": 300,  "hype_level": 0.93},
    {"id": "NB-2002R", "name": "New Balance 2002R 'Protection Pack'",      "sku": "M2002RDB",   "price": 130, "base_stock": 650,  "hype_level": 0.58},
]

SIZES = ["6", "6.5", "7", "7.5", "8", "8.5", "9", "9.5", "10", "10.5", "11", "11.5", "12", "13", "14"]

# Size distribution weights (bell curve centered on 10)
SIZE_WEIGHTS = {
    "6": 0.02, "6.5": 0.03, "7": 0.05, "7.5": 0.07, "8": 0.08,
    "8.5": 0.10, "9": 0.12, "9.5": 0.13, "10": 0.13, "10.5": 0.10,
    "11": 0.07, "11.5": 0.04, "12": 0.03, "13": 0.02, "14": 0.01,
}


class StockTracker:
    """Thread-safe stock tracker per (product_id, size)."""

    def __init__(self):
        self._stock = {}
        self._lock = threading.Lock()

    def reset_for_drop(self, product: dict, rng: random.Random = None):
        """Initialize stock levels for a product drop."""
        r = rng or random
        base = product["base_stock"]
        with self._lock:
            for size in SIZES:
                # Distribute stock by size popularity
                weight = SIZE_WEIGHTS[size]
                qty = max(1, int(base * weight * r.uniform(0.8, 1.2)))
                self._stock[(product["id"], size)] = qty

    def try_reserve(self, product_id: str, size: str) -> tuple[bool, int]:
        """Attempt to reserve one unit. Returns (success, remaining)."""
        with self._lock:
            key = (product_id, size)
            current = self._stock.get(key, 0)
            if current <= 0:
                return False, 0
            self._stock[key] = current - 1
            return True, current - 1

    def get_stock(self, product_id: str, size: str) -> int:
        with self._lock:
            return self._stock.get((product_id, size), 0)

    def get_total_stock(self, product_id: str) -> int:
        with self._lock:
            return sum(v for (pid, _), v in self._stock.items() if pid == product_id)

    def force_negative(self, product_id: str, size: str, amount: int = 5):
        """Force stock negative for overselling incident simulation."""
        with self._lock:
            key = (product_id, size)
            self._stock[key] = self._stock.get(key, 0) - amount


def select_drop_product(drop_index: int, rng: random.Random = None) -> dict:
    """Select a product for a given drop, weighted by hype level."""
    r = rng or random
    weights = [p["hype_level"] for p in PRODUCTS]
    # Cycle through products but with randomness
    r_state = r.getstate()
    r.seed(drop_index * 7919)  # deterministic per drop
    product = r.choices(PRODUCTS, weights=weights, k=1)[0]
    r.setstate(r_state)
    return product
