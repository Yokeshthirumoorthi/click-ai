"""
User population generator — 10k realistic SNKRS users with persistent profiles.
"""

import random
from faker import Faker

fake = Faker()
Faker.seed(42)
random.seed(42)

REGIONS = [
    ("us-east", 0.40),
    ("us-west", 0.25),
    ("eu-west", 0.20),
    ("ap-southeast", 0.05),
    ("ap-northeast", 0.05),
    ("sa-east", 0.03),
    ("af-south", 0.02),
]

DEVICES = [
    ("ios", 0.55),
    ("android", 0.30),
    ("web", 0.15),
]

PAYMENT_METHODS = [
    ("card", 0.55),
    ("apple_pay", 0.25),
    ("paypal", 0.15),
    ("google_pay", 0.05),
]

BEHAVIOR_TYPES = [
    ("casual_browser", 0.35),
    ("hype_buyer", 0.25),
    ("reseller", 0.15),
    ("window_shopper", 0.15),
    ("new_user", 0.10),
]

LOYALTY_TIERS = [
    ("bronze", 0.40),
    ("silver", 0.30),
    ("gold", 0.20),
    ("platinum", 0.10),
]

SIZES = ["6", "6.5", "7", "7.5", "8", "8.5", "9", "9.5", "10", "10.5", "11", "11.5", "12", "13", "14"]


def _weighted_choice(options):
    """Pick from list of (value, weight) tuples."""
    values, weights = zip(*options)
    return random.choices(values, weights=weights, k=1)[0]


def generate_user(idx: int) -> dict:
    """Generate a single user profile."""
    behavior = _weighted_choice(BEHAVIOR_TYPES)
    account_age = random.randint(1, 2000)

    # Bots are mostly resellers
    is_bot = (behavior == "reseller" and random.random() < 0.20) or random.random() < 0.01

    # Fraud risk correlates with new accounts, bots, resellers
    base_fraud = random.uniform(0.01, 0.15)
    if is_bot:
        base_fraud += random.uniform(0.3, 0.5)
    if behavior == "reseller":
        base_fraud += random.uniform(0.1, 0.2)
    if account_age < 30:
        base_fraud += random.uniform(0.1, 0.3)
    if behavior == "new_user":
        base_fraud += random.uniform(0.05, 0.15)
    fraud_risk = min(1.0, base_fraud)

    # New users get bronze
    loyalty = "bronze" if behavior == "new_user" else _weighted_choice(LOYALTY_TIERS)

    return {
        "user_id": f"user_{100000 + idx}",
        "name": fake.name(),
        "email": fake.email(),
        "region": _weighted_choice(REGIONS),
        "device": _weighted_choice(DEVICES),
        "payment_method": _weighted_choice(PAYMENT_METHODS),
        "account_age_days": account_age,
        "is_bot": is_bot,
        "behavior_type": behavior,
        "shoe_size": random.choice(SIZES),
        "fraud_risk": round(fraud_risk, 3),
        "loyalty_tier": loyalty,
    }


def generate_users(count: int = 10000) -> list[dict]:
    """Generate the full user population."""
    # Reset seeds for reproducibility
    Faker.seed(42)
    random.seed(42)
    return [generate_user(i) for i in range(count)]


# Pre-built population for import
_USERS = None


def get_users(count: int = 10000) -> list[dict]:
    """Get or generate the user population (cached)."""
    global _USERS
    if _USERS is None or len(_USERS) != count:
        _USERS = generate_users(count)
    return _USERS
