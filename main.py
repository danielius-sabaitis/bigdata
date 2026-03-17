from multiprocessing import Pool
from collections import defaultdict
from reader import chunk_reader
from worker import process_chunk
import time
import psutil
import csv

# Aggregate results are written to a final CSV file after processing all chunks.
# Chunk size and number of workers can be adjusted to tune performance and later create graphics.

INPUT_PATH = ["aisdk-2025-04-22/aisdk-2025-04-22.csv"]
CHUNK_SIZE = 50000
WORKERS = 4

def fix_mb (value): 
    """This is a helper function to get megabytes from bytes."""
    return value/(1024*1024) 

def main():
    """Main function that controls the reading, processing, and aggregating final results."""
    process = psutil.Process()
    start = time.perf_counter()
    max_mem = 0
    global_results = defaultdict(lambda: {"A":0, "B":0, "C":0, "D":0})

    for file in INPUT_PATH:
        chunks = chunk_reader(file, CHUNK_SIZE)
        
        with Pool(WORKERS) as pool:
            for result in pool.imap_unordered(process_chunk, chunks):
                mem_rss = process.memory_info().rss
                max_mem = max(max_mem, mem_rss)
                if result["chunk"] % 10 == 0: # To save time and console space, it prints a progress message every 10 chunks, adjust if needed..
                    print(f"Chunk {result['chunk']} processed | RAM: {fix_mb(mem_rss):.2f} MB")

                for mmsi, anomalies in result["vessels"].items():
                    for category, count in anomalies.items():
                        global_results[mmsi][category] += count

    with open("final_results.csv", "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["MMSI", "A", "B", "C", "D"])
        for mmsi, anomalies in global_results.items():
            writer.writerow([mmsi, anomalies["A"], anomalies["B"], anomalies["C"], anomalies["D"]])

    total_time = time.perf_counter() - start

    print(f"\nFinished in {total_time:.2f}s")
    print(f"Peak RAM usage: {fix_mb(max_mem):.2f} MB")

if __name__ == "__main__":
    main()
