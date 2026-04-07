# Big Data 2026, 1st assignment: Maritime Shadow Fleet Detection

**Description**

This project processes large AIS vessel datasets to detect anomalies in vessel behavior. The system reads large CSV datasets in chunks, distributes processing across workers, and applies anomaly detection logic to identify suspicious vessel data. The final output aggregates anomaly counts and computes a Shadow Fleet Suspicion Index (DFSI) per vessel (MMSI).

**Input Data**

Dataset used: [Danish Maritime Authority AIS data](http://aisdata.ais.dk/)

Days used in the assignment: (*2025-04-22, 2025-04-23*)

*Note: the large datasets are not included in the repository. Provide the path to your CSV file when running the pipeline.*

Required data fields for analysis:
 - MMSI
 - Type of mobile
 - Timestamp
 - Latitude
 - Longitude
 - SOG
 - Draught

**Project architecture:**
---



**Project structure and file descriptions:**
---

Ar reikia??

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
- Justina Pečiulytė
- Danielius Sabaitis
