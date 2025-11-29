# Academic Performance Analytics | ETL Project

## Overview
This project implements an **ETL pipeline** to analyze student performance across subjects.  
It extracts data from CSV files, transforms it (cleans, validates, computes grades), and loads it into an **Oracle Database** for analysis.

---

## Folder Structure
Academic_Performance_Project/
├── 01_raw_datasets/
│ ├── students.csv
│ ├── subjects.csv
│ ├── results.csv
│ ├── etl.py
│ └── etl_load_oracle.py
├── diagrams/
│ └── schema_diagram.png
└── README.md

- `01_raw_datasets/`: contains raw CSV files and ETL scripts  
- `diagrams/`: database schema diagram  
- `README.md`: project documentation

---

## ETL Pipeline

### 1. Extract & Transform (`etl.py`)
- Reads `students.csv`, `subjects.csv`, and `results.csv`
- Cleans missing values and duplicates
- Validates marks (0–100)
- Converts columns to correct datatypes
- Computes `percentage` and assigns `grade`
- Output: cleaned & transformed DataFrames

### 2. Load (`etl_load_oracle.py`)
- Connects to Oracle XE as SYSDBA
- Creates tables (`students`, `subjects`, `results`) if not exists
- Inserts transformed data into the database

---

## Database Schema
![Schema Diagram](diagrams/schema_diagram.png)

- students(student_id PK, name, department, year)
- subjects(subject_id PK, subject_name, department)  
- results(result_id PK, student_id FK, subject_id FK, marks)

---

## How to Run
1. Ensure Python 3.x and oracledb library are installed
2. Place CSV files in 01_raw_datasets/
3. Run **extract & transform**:

```bash
python 01_raw_datasets/etl.py
