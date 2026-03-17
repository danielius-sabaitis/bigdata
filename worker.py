from collections import defaultdict
import anomaly_B

ANOMALY_CHECKS = {"B": anomaly_B.loitering_check}

def process_chunk(task):
    """Processes a chunk of data, performs anomaly checks, and returns the results."""
    chunk_index, rows = task
    vessels = defaultdict(list)

    for mmsi, timestamp, lat, lon, sog, draught in rows:
        vessels[mmsi].append((timestamp, lat, lon, sog, draught))

    results = {}

    for mmsi, track in vessels.items():
        track.sort(key=lambda x: x[0])  # Sort by timestamp.
        anomalies = {"A":0, "B":0, "C":0, "D":0}

        # Generalized anomaly checks loop, just add to dictionary and import.
        for category, check_function in ANOMALY_CHECKS.items():
            if check_function(track):
                anomalies[category] += 1

        results[mmsi] = anomalies

    return {"chunk": chunk_index, "vessels": results}
