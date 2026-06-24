"""Indian UPI domain data — cities, banks, merchants, devices.

Hardcoded (rather than fully random) so generated transactions look like real
UPI behaviour: plausible cities with real coordinates, real bank handles,
category-appropriate merchants and amount ranges.
"""

# City -> (latitude, longitude). Used for impossible-travel distance maths.
CITIES = {
    "Mumbai":     (19.0760, 72.8777),
    "Pune":       (18.5204, 73.8567),
    "Delhi":      (28.7041, 77.1025),
    "Bengaluru":  (12.9716, 77.5946),
    "Hyderabad":  (17.3850, 78.4867),
    "Chennai":    (13.0827, 80.2707),
    "Kolkata":    (22.5726, 88.3639),
    "Ahmedabad":  (23.0225, 72.5714),
    "Jaipur":     (26.9124, 75.7873),
    "Lucknow":    (26.8467, 80.9462),
}

# Bank -> UPI handle suffix(es).
BANKS = {
    "HDFC Bank":        ["okhdfcbank", "okhdfc"],
    "State Bank of India": ["oksbi", "sbi"],
    "ICICI Bank":       ["okicici", "icici"],
    "Axis Bank":        ["okaxis", "axisbank"],
    "Kotak Mahindra":   ["okkotak"],
    "Punjab National":  ["okpnb"],
    "Paytm Payments":   ["paytm", "ptyes"],
    "Yes Bank":         ["yespay", "okyesbank"],
}

# merchant_category -> (merchants, (min_amount, max_amount))
MERCHANTS = {
    "Food & Beverage":  (["Starbucks", "McDonald's", "Cafe Coffee Day", "Domino's", "Haldiram's"], (80, 900)),
    "Groceries":        (["Blinkit", "Zepto", "BigBasket", "DMart", "Instamart"], (150, 2500)),
    "Transport":        (["Uber", "Ola", "Rapido", "IRCTC", "Namma Metro"], (40, 1200)),
    "Shopping":         (["Amazon", "Flipkart", "Myntra", "Ajio", "Nykaa"], (300, 8000)),
    "Bills & Utilities": (["Airtel", "Jio", "Tata Power", "BESCOM", "Mahanagar Gas"], (200, 4000)),
    "Entertainment":    (["BookMyShow", "Netflix", "Spotify", "PVR", "Hotstar"], (149, 1500)),
    "Health":           (["Apollo Pharmacy", "PharmEasy", "1mg", "Practo"], (100, 3000)),
    "P2P Transfer":     (["Friend", "Family", "Roommate", "Landlord"], (100, 15000)),
}

DEVICE_MODELS = {
    "Android": ["Samsung Galaxy S23", "OnePlus 11", "Redmi Note 12", "Realme 11", "Vivo V29", "Motorola Edge 40"],
    "iOS":     ["iPhone 13", "iPhone 14", "iPhone 15", "iPhone SE"],
}

STATUSES = ["SUCCESS", "SUCCESS", "SUCCESS", "SUCCESS", "FAILED", "PENDING"]  # weighted toward success
