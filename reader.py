from datetime import datetime
import csv
import math

# This module defines the chunk reader that reads the input CSV file in chunks, 
# performs basic data validation, and yields the data for the worker.

def chunk_reader(input_path, chunksize=50000):
    with open(input_path, encoding="utf-8") as file:
        reader = csv.DictReader(file)

        if reader.fieldnames and reader.fieldnames[0].startswith("#"):
            reader.fieldnames[0] = reader.fieldnames[0].lstrip("#").strip() # Remove the "#" and whitespace from the first column name.

        chunk = []
        chunk_index = 0
        parse_timestamp = datetime.strptime

        for row in reader:
            mmsi = row["MMSI"]
            if not mmsi: # Skip rows with possibly missing MMSI.
                continue

            timestamp = parse_timestamp(row["Timestamp"], "%d/%m/%Y %H:%M:%S") # Change the format for easier sorting.
            lat = float(row["Latitude"]) 
            lon = float(row["Longitude"]) 
            sog = float(row["SOG"]) if row["SOG"] else math.nan # There are NaN values in the column, so adjust if needed.  
            draught = float(row["Draught"]) if row["Draught"] else math.nan # There are NaN values in the column, so adjust if needed.

            if not (-90 <= lat <= 90) or not (-180 <= lon <= 180): # Coordinates validation logic
                continue

            chunk.append((mmsi, timestamp, lat, lon, sog, draught))
            
            if len(chunk) >= chunksize:
                chunk_index += 1
                yield chunk_index, chunk
                chunk = []
                
        if chunk:
            chunk_index += 1
            yield chunk_index, chunk