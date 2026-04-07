from collections import defaultdict
from datetime import datetime
import csv
import math
import anomaly_A
import anomaly_B
import anomaly_C
import anomaly_D

INVALID_MMSI_VALUES = {"000000000",
                       "111111111",
                       "222222222",
                       "333333333",
                       "444444444",
                       "555555555",
                       "666666666",
                       "777777777",
                       "888888888",
                       "999999999",
                       "123456789"
}

def is_valid_mmsi(mmsi):
    if not mmsi:
        return False
    normalized = mmsi.strip()
    if len(normalized) != 9 or not normalized.isdigit():
        return False
    if normalized in INVALID_MMSI_VALUES:
        return False
    return True

def skip_mobile_type(mobile_type):
    """Skips rows unless Type of mobile is exactly Class A."""
    value = mobile_type or ""
    return value != "Class A"

def iter_range_lines(file_obj, end_pos, header, carryover_rows):
    """Yields header, carryover rows, and rows from the file."""
    yield header
    for row in carryover_rows:
        yield row
    while file_obj.tell() < end_pos:
        line = file_obj.readline()
        if not line:
            break
        yield line.decode("utf-8")

def process_chunk(task):
    """Processes a chunk of data, performs anomaly checks, and returns the results."""
    chunk_index, input_path, start_pos, end_pos, header, carryover_rows = task
    vessels = defaultdict(list)
    parse_timestamp = datetime.strptime
    carryover_len = len(carryover_rows)

    with open(input_path, "rb") as file:
        file.seek(start_pos)
        line_iter = iter_range_lines(file, end_pos, header, carryover_rows)
        reader = csv.DictReader(line_iter)
        row_index = 0

        if reader.fieldnames and reader.fieldnames[0].startswith("#"): # Remove the "#" and whitespace from the first column name.
            fieldnames = list(reader.fieldnames)
            fieldnames[0] = fieldnames[0].lstrip("#").strip()
            reader.fieldnames = fieldnames
        
        for row in reader:
            is_carryover = row_index < carryover_len
            row_index += 1

            if skip_mobile_type(row.get("Type of mobile")):
                continue

            mmsi = row["MMSI"]
            if not is_valid_mmsi(mmsi): # Skip rows with invalid MMSI
                continue

            mmsi = mmsi.strip()

            try: # Try to safely parse the columns
                timestamp = parse_timestamp(row["Timestamp"], "%d/%m/%Y %H:%M:%S")
                lat = float(row["Latitude"])
                lon = float(row["Longitude"])
                sog = float(row["SOG"]) if row["SOG"] else math.nan
                draught = float(row["Draught"]) if row["Draught"] else math.nan
            except ValueError:
                continue
        
            if not (53 <= lat <= 66) or not (9 <= lon <= 33): # Coordinates validation logic (Baltic Sea boundaries) 
                continue

            vessels[mmsi].append((timestamp, lat, lon, sog, draught, is_carryover))

    results = {}

    for mmsi, track in vessels.items():
        track.sort(key=lambda x: x[0])  # Sort by timestamp.
        
    for mmsi, track in vessels.items():
        going_dark, max_gap_hours, max_gap_event = anomaly_A.going_dark_check(track)
        loitering = anomaly_B.loitering_check(track)
        draught_changes = anomaly_C.draught_changes_check(track)
        has_anomaly_d, impossible_jump_nm = anomaly_D.impossible_jumps_check(track) 

        anomalies = {"A":going_dark, 
                     "B":loitering, 
                     "C":draught_changes,
                     "D":has_anomaly_d, 
        }

        results[mmsi] = {"anomalies": anomalies,
                         "max_gap_hours": max_gap_hours,
                         "max_gap_event": max_gap_event, 
                         "draught_changes": draught_changes, 
                         "impossible_jumps_nm": impossible_jump_nm, 
                        }

    return {"chunk": chunk_index, "vessels": results}
