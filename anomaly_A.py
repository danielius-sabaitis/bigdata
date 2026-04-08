from haversine_dist import haversine_distance

# Anomaly A ("Going Dark") 
# Find AIS gaps of > 4 hours where the geographic distance between the disappearance and 
# reappearance coordinates implies the ship kept moving (it was not simply anchored).

GOING_DARK_THRESHOLD_SECONDS = 4 * 3600

def going_dark_check(track):
    """Checks for significant gaps between pings where the ship kept moving."""
    if len(track) < 2:
        return 0, 0.0, "No movement during blackout"

    max_gap_hours = 0.0
    max_gap_event = "No movement during blackout"  # This stores info on the longest gap
    count = 0

    for previous, current in zip(track, track[1:]): # Loop through consecutive pings
        t1, lat1, lon1, _, _, _ = previous
        t2, lat2, lon2, _, _, current_is_carryover = current

        if current_is_carryover:
            continue

        gap_seconds = (t2 - t1).total_seconds()
        if gap_seconds <= GOING_DARK_THRESHOLD_SECONDS:
            continue

        distance_nm = haversine_distance(lat1, lon1, lat2, lon2)
        gap_hours = gap_seconds / 3600.0

        if distance_nm > 0:
            count += 1
            if gap_hours > max_gap_hours:
                max_gap_hours = gap_hours
                max_gap_event = (
                    f"[{t1}] Lat: {lat1}, Lon: {lon1} ---> [{t2}] Lat: {lat2}, Lon: {lon2} | "
                    f"(Blackout gap: {gap_hours:.2f} h; Travel distance: {distance_nm * 1.852:.1f} km)"
                )

    return count, max_gap_hours, max_gap_event
