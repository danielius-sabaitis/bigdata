from multiprocessing import Pool
from collections import defaultdict
from reader import chunk_reader
from worker import process_chunk
import time
import psutil
import csv

# Aggregate results are written to a final CSV file after processing all chunks.
# Chunk size and number of workers can be adjusted to tune performance and later create graphics.

INPUT_PATH = ["PATH"]
CHUNK_SIZE = 50000
WORKERS = 4

def fix_mb (value): 
    """This is a helper function to get megabytes from bytes."""
    return value/(1024*1024) 

def get_memory_snapshot(main_process):
    """Returns main RSS, total RSS (main + workers), and max single worker RSS in bytes."""
    main_rss = main_process.memory_info().rss
    total_rss = main_rss
    max_worker_rss = 0

    for worker in main_process.children(recursive=True):
        try:
            worker_rss = worker.memory_info().rss
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue

        total_rss += worker_rss
        max_worker_rss = max(max_worker_rss, worker_rss)

    return main_rss, total_rss, max_worker_rss

def main(input_path=INPUT_PATH,
         chunk_size=CHUNK_SIZE,
         workers=WORKERS,
         output_path="final_results.csv",
         progress_interval=25):
    """Main function that controls the reading, processing, and aggregating final results."""
    print(f"Starting pipeline with {workers} workers \n---------------------------------")

    process = psutil.Process()
    start = time.perf_counter()
    max_main_mem = 0
    max_total_mem = 0
    max_single_worker_mem = 0
    global_results = defaultdict(lambda: {"A":0, 
                                          "B":0, 
                                          "C":0, 
                                          "D":0,
                                          "max_gap_hrs":0.0, # Will be needed for DFSI score
                                          "illicit_draught_changes":0.0, # Will be needed for DFSI score
                                          "impossible_jumps_nm": 0.0,}) # Will be needed for DFSI score

    for file in input_path:
        chunks = chunk_reader(file, chunk_size, overlap_rows=2000)
        
        with Pool(workers) as pool:
            for result in pool.imap_unordered(process_chunk, chunks):
                main_rss, total_rss, max_worker_rss = get_memory_snapshot(process)
                max_main_mem = max(max_main_mem, main_rss)
                max_total_mem = max(max_total_mem, total_rss)
                max_single_worker_mem = max(max_single_worker_mem, max_worker_rss)
                if result["chunk"] % progress_interval == 0: # To save time and console space, it prints a progress message every XX chunks, adjust if needed..
                    print(f"Chunk {result['chunk']} processed | Main RAM: {fix_mb(main_rss):.2f} MB | Total RAM: {fix_mb(total_rss):.2f} MB")

                for mmsi, vessel_results in result["vessels"].items():
                    anomalies = vessel_results["anomalies"]

                    for category, count in anomalies.items():
                        global_results[mmsi][category] += count
                        
                    global_results[mmsi]["max_gap_hours"] = max(
                            global_results[mmsi]["max_gap_hours"],
                            vessel_results.get("max_gap_hours", 0.0),) 
                    global_results[mmsi]["illicit_draught_changes"] += vessel_results.get("draught_changes", 0.0)
                    global_results[mmsi]["impossible_jumps_nm"] += vessel_results.get("impossible_jumps_nm", 0.0)

    scored_rows = []
    for mmsi, metrics in global_results.items():
        dfsi = (metrics["max_gap_hours"] / 2.0 + metrics["impossible_jumps_nm"] / 10.0 + metrics["illicit_draught_changes"] * 15)

        scored_rows.append({"MMSI": mmsi,
                            "A": metrics["A"],
                            "B": metrics["B"],
                            "C": metrics["C"],
                            "D": metrics["D"],
                            "Max_Gap_Hrs": metrics["max_gap_hours"],
                            "Illicit_Draught_Changes": metrics["illicit_draught_changes"],
                            "Impossible_Jumps_Nm": metrics["impossible_jumps_nm"],
                            "DFSI_Score": dfsi,})
        
    scored_rows.sort(key=lambda x: x["DFSI_Score"], reverse=True)

    with open(output_path, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["MMSI", "A", "B", "C", "D", "Max_Gap_Hrs", "Illicit_Draught_Changes", "Impossible_Jumps_Nm", "DFSI_Score"])
        for row in scored_rows:
            writer.writerow([row["MMSI"], 
                             row["A"], 
                             row["B"], 
                             row["C"], 
                             row["D"], 
                             f"{row['Max_Gap_Hrs']:.4f}", 
                             f"{row['Illicit_Draught_Changes']:.4f}", 
                             f"{row['Impossible_Jumps_Nm']:.4f}", 
                             f"{row['DFSI_Score']:.4f}"])

    top5 = scored_rows[:5] # Get top 5 suspicious vessels by DFSI
    total_vessels = len(global_results) # Just for interest, total number of ships processed.
    total_time = time.perf_counter() - start

    print(f"\nFinished in {total_time:.2f}s")
    print(f"Peak main RAM usage: {fix_mb(max_main_mem):.2f} MB")
    print(f"Peak total RAM usage (main + workers): {fix_mb(max_total_mem):.2f} MB")
    print(f"Peak single worker RAM usage: {fix_mb(max_single_worker_mem):.2f} MB")
    print(f"Total vessels processed: {total_vessels}")

if __name__ == "__main__":
    main()
