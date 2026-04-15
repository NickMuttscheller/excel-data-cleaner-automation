# CSV and Excel Data Cleaner Automation

## Overview

This project is a production-style Python automation workflow designed to clean, validate, and structure messy business data exported from CSV or Excel files.

It reflects a real freelance scenario where raw data contains inconsistencies such as missing values, duplicate records, invalid dates, inconsistent formatting, and corrupted numeric fields.

The script processes raw input data and produces clean, analysis-ready datasets, structured reports, and detailed data quality insights.

---

## Features

- Supports CSV and Excel input files  
- Automatically detects and loads input data  
- Standardizes column names to a consistent format  
- Cleans and normalizes text fields (names, emails, products, etc.)  
- Safely converts and validates date fields  
- Cleans and validates numeric fields (currency, quantities, totals)  
- Detects and removes fully empty rows  
- Removes duplicate records  
- Recalculates totals where inconsistencies are found  
- Detects invalid records and flags them for review (instead of silently removing them)  
- Generates structured issue tracking columns  
- Produces formatted Excel reports with multiple sheets  
- Exports cleaned data to CSV  
- Generates a detailed text-based summary report  
- Includes a full logging system  

---

## Project Structure

scripts/
├── excel_data_cleaner_automation/
│   ├── input/
│   │   └── your_data_file.csv
│   ├── output/
│   ├── log/
│   ├── README.md
│   └── data_cleaner.py

---

## How It Works

1. Automatically detects the first CSV or Excel file in the input folder  
2. Loads raw data into memory  
3. Standardizes column names  
4. Replaces placeholder missing values (e.g. "N/A", "null", "-")  
5. Removes fully empty rows  
6. Cleans and formats text fields  
7. Normalizes identifiers (e.g. customer IDs, phone numbers)  
8. Validates and normalizes status values  
9. Parses and validates date fields  
10. Cleans and validates numeric values  
11. Removes duplicate records  
12. Recalculates totals where needed  
13. Flags problematic rows instead of removing them  
14. Generates issue tracking columns and notes  
15. Splits clean vs flagged records logically  
16. Exports all outputs (Excel, CSV, summary report)

---

## Data Validation & Issue Tracking

The script does not simply remove problematic data. Instead, it flags issues and preserves records for review.

Each row is evaluated and enriched with:

- issue_missing_key_field  
- issue_invalid_signup_date  
- issue_invalid_numeric_data  
- issue_negative_values  
- issue_status_review_required  
- issue_notes  
- record_status (Clean / Flagged)

This approach ensures no critical business data is lost while still maintaining high data quality standards.

---

## Output Files

### Excel Report

A professionally formatted Excel workbook containing:

- Cleaned Data  
- Data Quality Summary  
- Issue Summary  

### CSV Export

- Fully cleaned dataset ready for analysis or reporting  

### Summary Report (TXT)

- Data quality metrics  
- Cleaning results  
- Issue breakdown  

All files are saved in the output folder with timestamped filenames.

---

## Logging

The script includes a structured logging system that records:

- execution steps  
- row counts and transformations  
- cleaning actions performed  
- detected issues and anomalies  
- output file creation  
- full error tracebacks (if failures occur)

Logs are stored in the log folder.

---

## How to Run (Important)

### 1. Install dependencies

pip install pandas openpyxl

---

### 2. Add your data

Place your CSV or Excel file inside:

input/

---

### 3. Run the script (correct way)

#### Option A — Terminal (recommended)

Open a terminal in the project folder and run:

python data_cleaner.py

---

#### Option B — VS Code

Right-click the file and select:

Run Python File in Terminal

---

### ⚠️ Important Note

Do NOT run the script inside the Python interpreter (>>>).

This will cause an error because system commands are not valid Python syntax.

---

## Tech Stack

- Python  
- pandas  
- openpyxl  
- pathlib  
- logging  

---

## Portfolio Value

This project demonstrates real freelance-level automation skills:

- cleaning messy client data  
- building reusable data pipelines  
- validating business datasets  
- generating structured outputs and reports  
- writing maintainable, production-style Python code  

It reflects the type of work required in:

- data cleaning projects  
- reporting automation  
- Excel workflow optimization  
- business analytics pipelines  

---

## Use Case

Typical workflow:

Client exports messy data → places file into input folder → runs script → receives clean dataset, Excel report, and data quality summary.

This reduces manual cleaning time and ensures consistent, reliable data for downstream analysis.