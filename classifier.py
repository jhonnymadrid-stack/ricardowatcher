import re
from dataclasses import dataclass
from typing import Optional


@dataclass
class ProductMatch:
    category: str
    display: str
    storage: Optional[str] = None


STORAGE_PRODUCTS = {"iphone", "samsung", "ipad", "macbook", "mac_mini", "imac"}

MIN_PRICE: dict[str, float] = {
    "ps5":         150.0,
    "ps4_pro":      70.0,
    "ps4_slim":     50.0,
    "switch_2":    100.0,
    "switch_oled":  60.0,
    "switch_lite":  40.0,
    "switch_1":     40.0,
    "gameboy":      25.0,
    "iphone":       80.0,
    "samsung":      80.0,
    # iPad — más específico primero para que el prefix matching no colisione
    "ipad_pro":    150.0,
    "ipad_air":    100.0,
    "ipad_mini":    80.0,
    "ipad":         80.0,
    # Mac
    "macbook_pro": 300.0,
    "macbook_air": 200.0,
    "mac_mini":    150.0,
    "imac":        300.0,
}


def min_price(category: str) -> float:
    for prefix, price in MIN_PRICE.items():
        if category.startswith(prefix):
            return price
    return 0.0


def extract_storage(text: str) -> str:
    t = text.lower()
    m = re.search(r'(\d+)\s*tb', t)
    if m:
        return f"{int(m.group(1))}TB"
    # Use finditer so RAM size (e.g. "8GB") doesn't shadow storage (e.g. "256GB")
    for m in re.finditer(r'(\d+)\s*gb', t):
        val = int(m.group(1))
        if val in (32, 64, 128, 256, 512):
            return f"{val}GB"
    return "256GB"


def _detect_chip(t: str) -> Optional[str]:
    for chip in ("m4", "m3", "m2", "m1"):
        if re.search(rf'\b{chip}\b', t):
            return chip
    return None


def classify(title: str) -> Optional[ProductMatch]:
    t = title.lower()

    # ── PS5 ──────────────────────────────────────────────────────────────────
    if any(k in t for k in ["ps5 pro", "playstation 5 pro"]):
        return ProductMatch("ps5_pro", "PS5 Pro")

    if any(k in t for k in ["ps5 slim disc", "ps5 slim disk", "slim disc edition",
                              "slim disk edition", "ps5 slim mit laufwerk"]):
        return ProductMatch("ps5_slim_disc", "PS5 Slim Disc")

    if any(k in t for k in ["ps5 slim digital", "slim digital edition",
                              "ps5 slim ohne laufwerk"]):
        return ProductMatch("ps5_slim_digital", "PS5 Slim Digital")

    if any(k in t for k in ["ps5 disc", "ps5 disk", "ps5 mit laufwerk",
                              "playstation 5 disc", "playstation 5 disk",
                              "disc edition", "disk edition"]):
        if "slim" not in t:
            return ProductMatch("ps5_disc", "PS5 Disc")

    if any(k in t for k in ["ps5 digital", "ps5 ohne laufwerk",
                              "playstation 5 digital", "digital edition"]):
        if "slim" not in t:
            return ProductMatch("ps5_digital", "PS5 Digital")

    # ── PS4 ──────────────────────────────────────────────────────────────────
    if any(k in t for k in ["ps4 pro", "playstation 4 pro"]):
        return ProductMatch("ps4_pro", "PS4 Pro")

    if any(k in t for k in ["ps4 slim", "playstation 4 slim"]):
        return ProductMatch("ps4_slim", "PS4 Slim")

    # ── Nintendo Switch ───────────────────────────────────────────────────────
    if re.search(r'switch\s*2|nintendo\s+switch\s+2', t):
        return ProductMatch("switch_2", "Nintendo Switch 2")

    if "switch oled" in t:
        return ProductMatch("switch_oled", "Nintendo Switch OLED")

    if "switch lite" in t:
        return ProductMatch("switch_lite", "Nintendo Switch Lite")

    if "nintendo switch" in t:
        return ProductMatch("switch_1", "Nintendo Switch")

    # ── GameBoy ───────────────────────────────────────────────────────────────
    if re.search(r'game\s*boy|gameboy', t):
        return ProductMatch("gameboy", "GameBoy")

    # ── iPhone (11+) ─────────────────────────────────────────────────────────
    m = re.search(r'iphone\s*(\d+)', t)
    if m:
        model = int(m.group(1))
        if model >= 11:
            if "pro max" in t:
                variant, label = "pro_max", "Pro Max"
            elif "pro" in t:
                variant, label = "pro", "Pro"
            elif "plus" in t:
                variant, label = "plus", "Plus"
            elif "mini" in t:
                variant, label = "mini", "Mini"
            else:
                variant, label = "standard", ""
            storage = extract_storage(title)
            cat = f"iphone_{model}_{variant}_{storage.lower()}"
            display = f"iPhone {model}{' ' + label if label else ''} {storage}"
            return ProductMatch(cat, display, storage)

    # ── Samsung Galaxy S21+ ───────────────────────────────────────────────────
    m = re.search(r'(?:samsung\s+)?galaxy\s+s(\d+)', t)
    if m:
        model = int(m.group(1))
        if model >= 21:
            if "ultra" in t:
                variant, label = "ultra", " Ultra"
            elif re.search(r's\d+\+|plus', t):
                variant, label = "plus", "+"
            elif "fe" in t:
                variant, label = "fe", " FE"
            else:
                variant, label = "standard", ""
            storage = extract_storage(title)
            cat = f"samsung_s{model}_{variant}_{storage.lower()}"
            display = f"Samsung Galaxy S{model}{label} {storage}"
            return ProductMatch(cat, display, storage)

    # ── iPad ─────────────────────────────────────────────────────────────────
    if "ipad" in t:
        storage = extract_storage(title)

        if "ipad pro" in t:
            chip = _detect_chip(t)
            if chip is None:
                chip = "m2" if "2022" in t else ("m4" if "2024" in t else "m1")
            size = "13" if re.search(r'12[,.]9|13\s*(?:"|zoll|inch)', t) else "11"
            cat = f"ipad_pro_{size}_{chip}_{storage.lower()}"
            display = f"iPad Pro {size}\" {chip.upper()} {storage}"
            return ProductMatch(cat, display, storage)

        if "ipad air" in t:
            chip = _detect_chip(t)
            if chip is None:
                m_gen = re.search(r'air\s+(\d)', t)
                if m_gen:
                    chip = {5: "m1", 6: "m2", 7: "m3"}.get(int(m_gen.group(1)), "m1")
                else:
                    chip = "m1"
            cat = f"ipad_air_{chip}_{storage.lower()}"
            display = f"iPad Air {chip.upper()} {storage}"
            return ProductMatch(cat, display, storage)

        if "ipad mini" in t:
            m_gen = re.search(r'mini\s*(\d)', t)
            gen = int(m_gen.group(1)) if m_gen and int(m_gen.group(1)) in (5, 6, 7) else 6
            cat = f"ipad_mini_{gen}_{storage.lower()}"
            display = f"iPad mini {gen} {storage}"
            return ProductMatch(cat, display, storage)

        # Standard iPad (no pro/air/mini)
        if not any(x in t for x in ["pro", "air", "mini"]):
            m_gen = re.search(r'(\d+)[.\s]*(?:gen|generation)', t)
            if m_gen and int(m_gen.group(1)) in (9, 10, 11):
                gen = int(m_gen.group(1))
            elif "2024" in t:
                gen = 11
            elif "2022" in t:
                gen = 10
            else:
                gen = 10
            cat = f"ipad_{gen}_{storage.lower()}"
            display = f"iPad {gen}ª gen {storage}"
            return ProductMatch(cat, display, storage)

    # ── Mac ──────────────────────────────────────────────────────────────────
    if re.search(r'macbook\s*pro', t):
        chip = _detect_chip(t)
        if chip is None:
            chip = "m1"
        m_size = re.search(r'\b(13|14|16)\b', t)
        size = m_size.group(1) if m_size else "14"
        storage = extract_storage(title)
        cat = f"macbook_pro_{size}_{chip}_{storage.lower()}"
        display = f"MacBook Pro {size}\" {chip.upper()} {storage}"
        return ProductMatch(cat, display, storage)

    if re.search(r'macbook\s*air', t):
        chip = _detect_chip(t)
        if chip is None:
            chip = "m3" if "2024" in t else ("m2" if "2022" in t else "m1")
        storage = extract_storage(title)
        cat = f"macbook_air_{chip}_{storage.lower()}"
        display = f"MacBook Air {chip.upper()} {storage}"
        return ProductMatch(cat, display, storage)

    if re.search(r'mac\s*mini', t):
        chip = _detect_chip(t)
        if chip is None:
            chip = "m1"
        storage = extract_storage(title)
        cat = f"mac_mini_{chip}_{storage.lower()}"
        display = f"Mac mini {chip.upper()} {storage}"
        return ProductMatch(cat, display, storage)

    if re.search(r'\bimac\b', t):
        chip = _detect_chip(t)
        if chip is None:
            chip = "m1"
        storage = extract_storage(title)
        cat = f"imac_{chip}_{storage.lower()}"
        display = f"iMac {chip.upper()} {storage}"
        return ProductMatch(cat, display, storage)

    return None


# ── Variant tables ────────────────────────────────────────────────────────────

IPHONE_MODELS = {
    11: ["standard", "pro", "pro_max"],
    12: ["standard", "mini", "pro", "pro_max"],
    13: ["standard", "mini", "pro", "pro_max"],
    14: ["standard", "plus", "pro", "pro_max"],
    15: ["standard", "plus", "pro", "pro_max"],
    16: ["standard", "plus", "pro", "pro_max"],
}
IPHONE_VARIANT_LABEL = {
    "standard": "", "mini": " Mini", "plus": " Plus",
    "pro": " Pro", "pro_max": " Pro Max",
}
IPHONE_STORAGES = ["64GB", "128GB", "256GB", "512GB", "1TB"]

SAMSUNG_MODELS = [21, 22, 23, 24, 25]
SAMSUNG_VARIANTS = [("standard", ""), ("plus", "+"), ("ultra", " Ultra"), ("fe", " FE")]
SAMSUNG_STORAGES = ["128GB", "256GB", "512GB"]

IPAD_PRO_SIZES = ["11", "13"]
IPAD_PRO_CHIPS = ["m1", "m2", "m4"]
IPAD_PRO_STORAGES = ["128GB", "256GB", "512GB", "1TB", "2TB"]

IPAD_AIR_CHIPS = ["m1", "m2", "m3"]
IPAD_AIR_STORAGES = ["128GB", "256GB", "512GB"]

IPAD_MINI_GENS = [5, 6, 7]
IPAD_MINI_STORAGES = ["64GB", "128GB", "256GB"]

IPAD_GENS = [9, 10, 11]
IPAD_STORAGES = ["64GB", "128GB", "256GB"]

MACBOOK_AIR_CHIPS = ["m1", "m2", "m3", "m4"]
MACBOOK_AIR_STORAGES = ["256GB", "512GB", "1TB", "2TB"]

MACBOOK_PRO_SIZES = ["13", "14", "16"]
MACBOOK_PRO_CHIPS = ["m1", "m2", "m3", "m4"]
MACBOOK_PRO_STORAGES = ["256GB", "512GB", "1TB", "2TB"]

MAC_MINI_CHIPS = ["m1", "m2", "m4"]
MAC_MINI_STORAGES = ["256GB", "512GB", "1TB", "2TB"]

IMAC_CHIPS = ["m1", "m3", "m4"]
IMAC_STORAGES = ["256GB", "512GB", "1TB", "2TB"]


def iphone_variants() -> list[tuple[str, str]]:
    result = []
    for model, variants in IPHONE_MODELS.items():
        for variant in variants:
            for storage in IPHONE_STORAGES:
                cat = f"iphone_{model}_{variant}_{storage.lower()}"
                label = IPHONE_VARIANT_LABEL[variant]
                display = f"iPhone {model}{label} {storage}"
                result.append((cat, display))
    return result


def samsung_variants() -> list[tuple[str, str]]:
    result = []
    for model in SAMSUNG_MODELS:
        for variant, label in SAMSUNG_VARIANTS:
            for storage in SAMSUNG_STORAGES:
                cat = f"samsung_s{model}_{variant}_{storage.lower()}"
                display = f"Samsung Galaxy S{model}{label} {storage}"
                result.append((cat, display))
    return result


def ipad_pro_variants() -> list[tuple[str, str]]:
    result = []
    for size in IPAD_PRO_SIZES:
        for chip in IPAD_PRO_CHIPS:
            for storage in IPAD_PRO_STORAGES:
                cat = f"ipad_pro_{size}_{chip}_{storage.lower()}"
                display = f"iPad Pro {size}\" {chip.upper()} {storage}"
                result.append((cat, display))
    return result


def ipad_air_variants() -> list[tuple[str, str]]:
    result = []
    for chip in IPAD_AIR_CHIPS:
        for storage in IPAD_AIR_STORAGES:
            cat = f"ipad_air_{chip}_{storage.lower()}"
            display = f"iPad Air {chip.upper()} {storage}"
            result.append((cat, display))
    return result


def ipad_mini_variants() -> list[tuple[str, str]]:
    result = []
    for gen in IPAD_MINI_GENS:
        for storage in IPAD_MINI_STORAGES:
            cat = f"ipad_mini_{gen}_{storage.lower()}"
            display = f"iPad mini {gen} {storage}"
            result.append((cat, display))
    return result


def ipad_variants() -> list[tuple[str, str]]:
    result = []
    for gen in IPAD_GENS:
        for storage in IPAD_STORAGES:
            cat = f"ipad_{gen}_{storage.lower()}"
            display = f"iPad {gen}ª gen {storage}"
            result.append((cat, display))
    return result


def macbook_air_variants() -> list[tuple[str, str]]:
    result = []
    for chip in MACBOOK_AIR_CHIPS:
        for storage in MACBOOK_AIR_STORAGES:
            cat = f"macbook_air_{chip}_{storage.lower()}"
            display = f"MacBook Air {chip.upper()} {storage}"
            result.append((cat, display))
    return result


def macbook_pro_variants() -> list[tuple[str, str]]:
    result = []
    for size in MACBOOK_PRO_SIZES:
        for chip in MACBOOK_PRO_CHIPS:
            for storage in MACBOOK_PRO_STORAGES:
                cat = f"macbook_pro_{size}_{chip}_{storage.lower()}"
                display = f"MacBook Pro {size}\" {chip.upper()} {storage}"
                result.append((cat, display))
    return result


def mac_mini_variants() -> list[tuple[str, str]]:
    result = []
    for chip in MAC_MINI_CHIPS:
        for storage in MAC_MINI_STORAGES:
            cat = f"mac_mini_{chip}_{storage.lower()}"
            display = f"Mac mini {chip.upper()} {storage}"
            result.append((cat, display))
    return result


def imac_variants() -> list[tuple[str, str]]:
    result = []
    for chip in IMAC_CHIPS:
        for storage in IMAC_STORAGES:
            cat = f"imac_{chip}_{storage.lower()}"
            display = f"iMac {chip.upper()} {storage}"
            result.append((cat, display))
    return result


# Search queries to run on Ricardo
SEARCH_QUERIES = [
    "ps5",
    "playstation 5",
    "ps4",
    "playstation 4",
    "nintendo switch",
    "gameboy",
    "iphone",
    "samsung galaxy s",
    "ipad",
    "macbook",
    "mac mini",
    "imac",
]

# Display groups for /articulos command: (group_name, [(category_pattern, display_name)])
PRODUCT_GROUPS = [
    ("PlayStation 5", [
        ("ps5_pro",          "PS5 Pro"),
        ("ps5_slim_disc",    "PS5 Slim Disc"),
        ("ps5_slim_digital", "PS5 Slim Digital"),
        ("ps5_disc",         "PS5 Disc"),
        ("ps5_digital",      "PS5 Digital"),
    ]),
    ("PlayStation 4", [
        ("ps4_pro",  "PS4 Pro"),
        ("ps4_slim", "PS4 Slim"),
    ]),
    ("Nintendo Switch", [
        ("switch_2",    "Nintendo Switch 2"),
        ("switch_oled", "Nintendo Switch OLED"),
        ("switch_lite", "Nintendo Switch Lite"),
        ("switch_1",    "Nintendo Switch"),
    ]),
    ("GameBoy", [
        ("gameboy", "GameBoy"),
    ]),
    ("iPhone 11+", iphone_variants()),
    ("Samsung Galaxy S21+", samsung_variants()),
    ("iPad Pro", ipad_pro_variants()),
    ("iPad Air", ipad_air_variants()),
    ("iPad mini", ipad_mini_variants()),
    ("iPad", ipad_variants()),
    ("MacBook Air", macbook_air_variants()),
    ("MacBook Pro", macbook_pro_variants()),
    ("Mac mini", mac_mini_variants()),
    ("iMac", imac_variants()),
]
