# Data File Converter (CSV → JSON)

## Overview

This project is a simple data processing pipeline that converts raw CSV files into structured JSON format using predefined schemas.

It is created for learning and practice purposes to simulate a real-world data engineering workflow where raw datasets are ingested, transformed, and written to a target format for downstream use.

---

## Features

* Reads raw CSV files from a source directory
* Applies schema-based column mapping
* Converts CSV data batch into JSON 
* Saves the results to a target directory
* Handles multiple datasets automatically

---

## Project Structure

```text
project-folder/
│
├── .gitignore
├── .env                     # environment variables (source/target directories)
├── app.py                   # main CSV → JSON pipeline
├── requirements.txt         # Python dependencies
├── README.md
└── data/
    ├── retail_db/           # raw CSV input
    │   └── schemas.json     # dataset metadata + column definitions
    └── retail_db_json/      # JSON output (generated)
```

### File and Folder Details

* **app.py** – main pipeline script 
* **schemas.json** – JSON file defining metadata and column details for each dataset
* **.env** – environment variables for specifying source and target base directories 
* **data/retail_db/** – contains raw CSV datasets used as input for the pipeline
* **data/retail_db_json/** – output directory for processed the JSON files
* **requirements.txt** – required Python dependencies 

---

## How It Works

1. Load schema definitions from schemas.json
2. Read CSV datasets using schema-based column definitions
3. Transform rows into pandas DataFrames
4. Export standardized JSON output for each dataset

---

## Example Transformation

**CSV input:**

```csv
product_id,product_category_id,product_name,product_description,product_price,product_image
1,2,Quest Q64 10 FT. x 10 FT. Slant Leg Instant U,,59.98,http://images.acmesports.sports/Quest+Q64+10+FT.+x+10+FT.+Slant+Leg+Instant+Up+Canopy
````

**JSON output:**

```json
[
  {
    "product_id": 1,
    "product_category_id": 2,
    "product_name": "Quest Q64 10 FT. x 10 FT. Slant Leg Instant U",
    "product_description": null,
    "product_price": 59.98,
    "product_image": "http://images.acmesports.sports/Quest+Q64+10+FT.+x+10+FT.+Slant+Leg+Instant+Up+Canopy"
  }
]
```

---

## Setup

### 1. Install dependencies

```
pip install -r requirements.txt
```

### 2. (Optional) Configure environment variables

Change the `.env` file if needed:

```
SRC_BASE_DIR=path/to/source/data
TGT_BASE_DIR=path/to/output/data
```

You can customize these paths to point to your own datasets.

Default `.env`:

```
SRC_BASE_DIR=data/retail_db
TGT_BASE_DIR=data/retail_db_json
```

---

## Usage

### Process all datasets

```
python app.py
```

### Process specific datasets

```
python app.py '["orders", "customers"]'
```

---

## Technologies Used

* Python

---

## Notes

* This project focuses on demonstrating core data engineering concepts such as schema enforcement and batch processing
* This project is mainly for learning purposes and to practice production-ready data engineering skills.

---

## Future Improvements

* Add logging instead of print statements
* Support additional file formats (Parquet, Avro)
* Integrate with cloud storage (AWS S3, GCP)
* Add unit tests

---

