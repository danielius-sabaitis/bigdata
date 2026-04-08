# Big Data 2026, 1st assignment: Maritime Shadow Fleet Detection

**Description**

This project processes large AIS vessel datasets to detect anomalies in vessel behavior. The system reads large CSV datasets in chunks, distributes processing across workers, and applies anomaly detection logic to identify suspicious vessel data. The final output aggregates anomaly counts and computes a Shadow Fleet Suspicion Index (DFSI) per vessel (MMSI).

**Input Data**

Dataset used: [Danish Maritime Authority AIS data](http://aisdata.ais.dk/)

Days used in the assignment: (*2025-04-22, 2025-04-23*)

*Note: the large datasets are not included in the repository. Provide the path to your CSV file when running the pipeline. A small dataset ```testing.csv``` is provided for testing without loading the whole large dataset.*

Required data fields for analysis:
 - MMSI
 - Type of mobile
 - Timestamp
 - Latitude
 - Longitude
 - SOG
 - Draught

**Project Architecture and Module Description:**
---

<img src="./docs/project_diagram.drawio.svg">

- ```reader.py```  
  Reads large input AIS CSV files in chunks and creates tasks which are distributed across workers.
  
- ```worker.py```
  Groups rows by vessel (MMSI) and performs data validation by checking for invalid MMSI, coordinates outside of the Baltic sea, and skipping non-commercial ships. Then, applies anomaly detection modules and gathers results for each vessel.

- Anomaly detection modules:
    - ```anomaly_A.py``` **"Going Dark"** - 
      Detects gaps longer than 4 hours between AIS where the vessel appears to have continued moving. The module uses geographic distance between consecutive pings to identify suspicious periods.
    - ```anomaly_B.py``` **Loitering and Transfers** - 
      Detects pairs of vessels moving close to each other and maintaining very slow speed for extended periods. 
    - ```anomaly_C.py``` **Draft Changes at Sea** - 
      Identifies signifiant draught changes during AIS blackouts which indicate possible cargo transfers or unloading at sea.
    - ```anomaly_D.py``` **Identity Cloning/Teleportation** - 
      Detects unrealistic vessel movement by using geographic distance between consecutive pings to calculate speed.

  Each anomaly module analyzes vessel tracks and returns both quantitative metrics and a description of the most significant detected event. This allows to later examine the context of suspicious activity and provide an additional interpretation. To calculate geographic distance, the haversine distance is used, provided in the ```haversine_dist.py``` helper function.

- ```main.py```
  Controls multiprocessing, aggregates partial results received from workers, and writes output to CSV files. 

**Instructions on how to run the pipeline:**
---

1. Install dependencies:
   
```
pip install -r requirements.txt
```
2. Specify input data path(s) in ```main.py```:

```
INPUT_PATH = ["path/to/your/file.csv"]
```

3. Run the pipeline:

```
python main.py
```

Outputs are saved to *final_results.csv* and *top_suspicious_vessels.csv*.

---
Group members:
- Justina Pečiulytė, justina.peciulyte@mif.stud.vu.lt
- Danielius Sabaitis, danielius.sabaitis@mif.stud.vu.lt
