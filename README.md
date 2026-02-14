# Tree Cover Loss Analysis ETL

An ETL (Extract, Transform, Load) pipeline built with Python to analyze tree cover loss and associated CO₂ emissions across districts in Sri Lanka. This project processes structured CSV datasets, transforms them into analysis-ready format, and generates meaningful environmental insights at the district level.

---

## Project Flow


**Extract**

* Reads raw tree cover loss dataset
* Reads district metadata dataset

**Transform**

* Merges datasets using district identifiers
* Aggregates tree cover loss by district
* Calculates total tree cover loss per district
* Sorts districts by severity of tree cover loss

**Load**

* Outputs transformed dataset into a clean CSV file
* Records ETL execution logs

---

## Version 1 – Current Analysis

Version 1 focuses on district-level tree cover loss analysis.

### Metrics Generated

* Total tree cover loss per district
* Ranking of districts by total tree cover loss
* Clean aggregated dataset ready for further analysis

This identifies the most affected districts based on cumulative tree cover loss.

---


## How to Run

Activate virtual environment (optional):

```
python -m venv venv
```

Run the ETL pipeline:

```
python analysis_etl.py
```

Output files will be generated:

```
transformed_data.csv
log_file.txt
```


