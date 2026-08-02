"""Reference data for the trimmed demo — same spirit as producer/domain.py,
kept self-contained so this folder deploys independently of the rest of the repo.
"""

CITIES = {
    "Mumbai":    (19.0760, 72.8777),
    "Pune":      (18.5204, 73.8567),
    "Delhi":     (28.7041, 77.1025),
    "Bengaluru": (12.9716, 77.5946),
    "Hyderabad": (17.3850, 78.4867),
    "Chennai":   (13.0827, 80.2707),
    "Kolkata":   (22.5726, 88.3639),
    "Jaipur":    (26.9124, 75.7873),
}

BANKS = {
    "HDFC Bank": "okhdfcbank",
    "State Bank of India": "oksbi",
    "ICICI Bank": "okicici",
    "Axis Bank": "okaxis",
    "Kotak Mahindra": "okkotak",
    "Paytm Payments": "paytm",
}

MERCHANTS = {
    "Food & Beverage":  (["Starbucks", "McDonald's", "Domino's", "Haldiram's"], (80, 900)),
    "Groceries":        (["Blinkit", "Zepto", "BigBasket", "DMart"], (150, 2500)),
    "Transport":        (["Uber", "Ola", "Rapido", "IRCTC"], (40, 1200)),
    "Shopping":         (["Amazon", "Flipkart", "Myntra", "Nykaa"], (300, 8000)),
    "Bills & Utilities": (["Airtel", "Jio", "Tata Power"], (200, 4000)),
    "Entertainment":    (["BookMyShow", "Netflix", "Spotify", "PVR"], (149, 1500)),
}

FIRST_NAMES = ["Amit", "Priya", "Rahul", "Sneha", "Vikram", "Anjali", "Karan", "Divya"]
DEVICE_MODELS = {
    "Android": ["Galaxy S23", "OnePlus 11", "Redmi Note 12"],
    "iOS": ["iPhone 13", "iPhone 14", "iPhone 15"],
}
