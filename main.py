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

def main(input_path=INPUT_PATH,
         chunk_size=CHUNK_SIZE,
         workers=WORKERS,
         output_path="final_results.csv",
         progress_interval=10,):
    """Main function that controls the reading, processing, and aggregating final results."""
    process = psutil.Process()
    start = time.perf_counter()
    max_mem = 0
    global_results = defaultdict(lambda: {"A":0, 
                                          "B":0, 
                                          "C":0, 
                                          "D":0,
                                          "max_gap_hrs":0.0, # Will be needed for DFSI score
                                          "impossible_jumps_nm": 0.0,}) # Will be needed for DFSI score

    for file in input_path:
        chunks = chunk_reader(file, chunk_size, overlap_rows=2000)
        
        with Pool(workers) as pool:
            for result in pool.imap_unordered(process_chunk, chunks):
                mem_rss = process.memory_info().rss
                max_mem = max(max_mem, mem_rss)
                if result["chunk"] % progress_interval == 0: # To save time and console space, it prints a progress message every XX chunks, adjust if needed..
                    print(f"Chunk {result['chunk']} processed | RAM: {fix_mb(mem_rss):.2f} MB")

                for mmsi, vessel_results in result["vessels"].items():
                    anomalies = vessel_results["anomalies"]

                    for category, count in anomalies.items():
                        global_results[mmsi][category] += count

                    global_results[mmsi]["max_gap_hrs"] = max(global_results[mmsi]["max_gap_hrs"], vessel_results["max_gap_hours"],)
                    global_results[mmsi]["impossible_jumps_nm"] += vessel_results.get("impossible_jumps_nm", 0.0)

    scored_rows = []
    for mmsi, metrics in global_results.items():
        dfsi = (metrics["max_gap_hrs"] / 2.0 + metrics["impossible_jumps_nm"] / 10.0 + metrics["C"] * 15)

        scored_rows.append({"MMSI": mmsi,
                            "A": metrics["A"],
                            "B": metrics["B"],
                            "C": metrics["C"],
                            "D": metrics["D"],
                            "Max_Gap_Hrs": metrics["max_gap_hrs"],
                            "Impossible_Jumps_Nm": metrics["impossible_jumps_nm"],
                            "DFSI_Score": dfsi,})
        
    scored_rows.sort(key=lambda x: x["DFSI_Score"], reverse=True)

    with open(output_path, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["MMSI", "A", "B", "C", "D", "Max_Gap_Hrs", "Impossible_Jumps_Nm", "DFSI_Score"])
        for row in scored_rows:
            writer.writerow([row["MMSI"], 
                             row["A"], 
                             row["B"], 
                             row["C"], 
                             row["D"], 
                             f"{row['Max_Gap_Hrs']:.4f}", 
                             f"{row['Impossible_Jumps_Nm']:.4f}", 
                             f"{row['DFSI_Score']:.4f}"])

    top5 = scored_rows[:5] # Get top 5 suspicious vessels by DFSI
    total_vessels = len(global_results) # Just for interest, total number of ships processed.
    total_time = time.perf_counter() - start

    print(f"\nFinished in {total_time:.2f}s")
    print(f"Peak RAM usage: {fix_mb(max_mem):.2f} MB")
    print(f"Total vessels processed: {total_vessels}")

if __name__ == "__main__":
    main()
