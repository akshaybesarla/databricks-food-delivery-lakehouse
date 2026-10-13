"""Business constants shared by the pipeline, the streaming job and tests."""

ORDER_STATUSES = ("PLACED", "ACCEPTED", "PICKED_UP", "DELIVERED", "CANCELLED")

# Used as a tie-breaker in AUTO CDC when two events share the same timestamp:
# a DELIVERED event must always win over a PICKED_UP event with the same ts.
STATUS_RANK = {
    "PLACED": 1,
    "ACCEPTED": 2,
    "PICKED_UP": 3,
    "DELIVERED": 4,
    "CANCELLED": 5,
}

PAYMENT_MODES = ("UPI", "CARD", "CASH", "WALLET")
RIDER_STATUSES = ("IDLE", "ON_TRIP")

# Promised delivery time shown to the customer in the app.
DELIVERY_SLA_MINUTES = 40

# How late an event may arrive and still be de-duplicated / aggregated.
EVENT_WATERMARK = "30 minutes"
DEMAND_WINDOW = "5 minutes"

CITIES = {
    "Hyderabad": ["Gachibowli", "Madhapur", "Kukatpally", "Banjara Hills", "Secunderabad"],
    "Bengaluru": ["Koramangala", "Indiranagar", "Whitefield", "HSR Layout", "Jayanagar"],
    "Chennai": ["T Nagar", "Adyar", "Velachery", "Anna Nagar", "OMR"],
    "Mumbai": ["Andheri", "Bandra", "Powai", "Lower Parel", "Thane"],
}

CUISINES = ("Biryani", "South Indian", "North Indian", "Chinese", "Pizza", "Burgers", "Desserts", "Healthy")
