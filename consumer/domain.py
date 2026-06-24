"""Reference data the consumer needs. Kept in sync with producer/domain.py.

(In a later iteration this becomes a shared package or a lookup table in
Postgres; duplicated here to keep each service's Docker build context isolated.)
"""

# City -> (latitude, longitude). Used for validation and distance maths.
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
