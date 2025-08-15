from datetime import datetime
from geopy.distance import geodesic

HIGH_AMOUNT_THRESHOLD = 4000

def add_anomaly(txn, anomaly_type, reason=None):
    txn.setdefault("detections", [])
    txn["detections"].append({
        "anomaly_type": anomaly_type,
        "reason": reason
    })
    txn["is_anomaly"] = True
    return txn

def inject_rule_high_amount(txn):
    if txn.get("amount", 0) > HIGH_AMOUNT_THRESHOLD:
        return add_anomaly(txn, "high_amount")
    return txn

def inject_rule_currency_mismatch(txn):
    if txn.get("currency") != txn.get("terminal_currency"):
        return add_anomaly(txn, "currency_mismatch")
    return txn

def inject_rule_card_expiring_with_high_amount(txn):
    try:
        card_expiry = datetime.strptime(txn.get("card_expire_date", "01/2100"), "%m/%Y")
        if (card_expiry - datetime.now()).days <= 30 and txn.get("amount", 0) > HIGH_AMOUNT_THRESHOLD:
            return add_anomaly(txn, "card_expiring_high_amount")
    except:
        pass
    return txn

def inject_ip_address(txn):
    ip = txn.get("ip_address", "")
    if ip.startswith("10.") or ip.startswith("192.168.") or ip.startswith("172.16."):
        return add_anomaly(txn, "Invalid IP address")
    return txn


def distance_from_home(txn):
    """
    Calculate distance in km between transaction location and customer's usual location.
    Expects:
      txn['geo_location'] = "lat,lon" of transaction
      txn['customer_location'] = "lat,lon" of customer's usual location
    """
    try:
        # Transaction location
        txn_lat, txn_lon = map(float, txn.get("geo_location", "0,0").split(","))

        # Customer location
        cust_lat, cust_lon = map(float, txn.get("customer_location", "0,0").split(","))

        # Distance in km
        return geodesic((txn_lat, txn_lon), (cust_lat, cust_lon)).km
    except:
        return -1  # fallback if invalid coordinates

def inject_geo_location(txn, max_distance_km=50):
    """
    Flag transaction as anomaly if it occurs far from customer's usual location.
    """
    distance = distance_from_home(txn)
    txn['distance_from_home_km'] = distance

    if distance > max_distance_km:
        return add_anomaly(
            txn,
            f"geo_location={txn.get('geo_location')} (distance={distance:.1f} km from customer location)"
        )

    return txn

# List of rules
anomaly_rules = [
    inject_rule_high_amount,
    inject_rule_currency_mismatch,
    inject_rule_card_expiring_with_high_amount,
    inject_ip_address,
    inject_geo_location
]
