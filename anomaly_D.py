import haversine_dist

# Anomaly D: Impossible travel speed showing Identity Cloning; Commercial ships cannot travel faster than threshold
SPEED_THRESHOLD_KNOTS = 60.0

def impossible_jumps_check(track):
    """
    Calculates speed between pings to find impossible jumps. Returns:
    has_anomaly_d (int): 1 if anomaly is found, 0 if not.
    total_jump_distance_nm (float): The total distance of the impossible jumps.
    """
    
    # A ship needs at least 2 pings to calculate speed/distance.
    if len(track) < 2:
        return 0, 0.0 

    impossible_jumps_count = 0
    total_jump_distance_nm = 0.0

    # Loop through the track, comparing ping 1 to ping 2, ping 2 to ping 3, etc.
    for i in range(1, len(track)):
        
        # 0. Extract data for the two consecutive pings
        # use *_ to ignore the extra data - like SOG and Draught
        t1, lat1, lon1, *_ = track[i-1]
        t2, lat2, lon2, *_ = track[i]

        # 1. Calculate time difference in h
        # .total_seconds() gets the exact time gap, then divide by 3600 to get h
        time_diff_hours = (t2 - t1).total_seconds() / 3600.0

        # 2. Calculate true Earth distance in nautical miles using haversine_dist
        distance_nm = haversine_dist.haversine_distance(lat1, lon1, lat2, lon2)

        # 3. Edge cases: If two pings happen at the exact same second, we cannot divide by zero
        if time_diff_hours == 0:
            if distance_nm > 0:
                # If time is 0 but distance moved > 0, that is teleportation
                impossible_jumps_count += 1
                total_jump_distance_nm += distance_nm
            continue

        # 4. Calculate Speed (Knots = Nautical Miles / Hours)
        speed_knots = distance_nm / time_diff_hours

        # 5. Check against the Anomaly D rule (60 knots)
        if speed_knots > SPEED_THRESHOLD_KNOTS:
            impossible_jumps_count += 1
            total_jump_distance_nm += distance_nm

    # 6. Final Result: If we found at least 1 jump, Anomaly D is triggered
    has_anomaly_d = 1 if impossible_jumps_count > 0 else 0
    
    return has_anomaly_d, total_jump_distance_nm