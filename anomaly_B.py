import math
import haversine_dist
from collections import defaultdict

# Anomaly B (Loitering & Transfers):
# Detect two distinct, valid MMSI numbers located within 500 meters of each other,
# maintaining a speed (SOG) of < 1 knot, for > 2 hours.

LOITERING_SPEED_THRESHOLD = 1  # Knots
LOITERING_DURATION = 2*3600 # Seconds
PROXIMITY_METERS = 500

def loitering_check(track1, track2):
    """Detects if two vessels loiter together within 500m for >2 hours."""
    i, j = 0, 0
    start_time = None

    while i < len(track1) and j < len(track2):
        t1, lat1, lon1, sog1, _, _ = track1[i]
        t2, lat2, lon2, sog2, _, _ = track2[j]

        # Sync timestamps
        if t1 < t2:
            i += 1
            continue
        elif t2 < t1:
            j += 1
            continue

        if (sog1 < LOITERING_SPEED_THRESHOLD and
            sog2 < LOITERING_SPEED_THRESHOLD):

            distance_nm = haversine_dist.haversine_distance(lat1, lon1, lat2, lon2)
            distance_m = distance_nm * 1852

            if distance_m <= PROXIMITY_METERS:
                if start_time is None:
                    start_time = t1
                else:
                    duration = (t1 - start_time).total_seconds()
                    if duration >= LOITERING_DURATION:
                        return True
            else:
                start_time = None
        else:
            start_time = None

        i += 1
        j += 1

    return False
