# Big Data 2026, 1st assignment: Maritime Shadow Fleet Detection

**Description**

This project processes large AIS vessel datasets to detect anomalies in vessel behavior. The pipeline reads large CSV datasets in chunks, distributes them across workers, and applies anomaly detection logic to identify suspicious vessel data. The final output aggregates anomaly counts per vessel (MMSI).

Dataset used: [Danish Maritime Authority AIS data](http://aisdata.ais.dk/)

Days used in the assignment: (*2025-04-22, 2025-04-23*)

*Note: the large datasets are not included in the repository. Provide the path to your CSV file when running the program.*

**Instructions on how to run the pipeline:**
---

To be explained

1. Install dependencies:
   
```
pip install -r requirements.txt
```

2. Run main.py:

```
python main.py
```

Results are saved to *final_results.csv*.

---
Group members:
- Justina Pečiulytė
- Danielius Sabaitis
