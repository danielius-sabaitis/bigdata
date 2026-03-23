import math
from collections import defaultdict

# Anomaly B (Loitering & Transfers): 
# Detect two distinct, valid MMSI numbers located within 500 meters of each other, 
# maintaining a speed (SOG) of < 1 knot, for > 2 hours.

LOITERING_SPEED_THRESHOLD = 1  # Knots
LOITERING_DURATION = 2*3600 # Seconds
PROXIMITY_METERS = 500

# Not finished yet !
# Needs to inclue the distance check between two vessels, so only checks loitering for one vessel.

def loitering_check(track):
    """Checks if the vessel is moving at a slow speed for an extended period."""
    start_time = None

    for timestamp, lat, lon, sog, draught, is_carryover in track:
        if sog is not None and not math.isnan(sog) and sog < LOITERING_SPEED_THRESHOLD:
            if start_time is None:
                start_time = timestamp
            duration = (timestamp - start_time).total_seconds()

            if duration > LOITERING_DURATION:
                return True
        else:
            start_time = None
    return False
