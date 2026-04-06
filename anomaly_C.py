import math

# Anomaly C (Draught Changes at Sea): 
# Detect vessels whose draught (depth in water) changes by more than 5% during 
# an AIS blackout of > 2 hours (implying cargo was loaded/unloaded illegally).

AIS_BLACKOUT_THRESHOLD = 2
DRAUGHT_CHANGE_THRESHOLD = 0.05

def draught_changes_check(track):
    """Checks for significant draught changes during AIS blackouts."""
    if len(track) < 2:
        return 0

    significant_changes = 0

    for i in range(1, len(track)):
        t1, _, _, _, draught1, _ = track[i-1]
        t2, _, _, _, draught2, _ = track[i]

        time_diff_hours = (t2 - t1).total_seconds() / 3600.0
        if time_diff_hours > AIS_BLACKOUT_THRESHOLD and not math.isnan(draught1) and not math.isnan(draught2):
            if draught1 > 0 and abs(draught2 - draught1) / draught1 > DRAUGHT_CHANGE_THRESHOLD:
                significant_changes += 1

    return significant_changes