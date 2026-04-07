***

# Data File Converter Pipeline (CSV → JSON)

## Overview

This project is a lightweight **data ingestion and transformation pipeline** that converts raw CSV files into structured, schema‑validated JSON output.

It is designed as a **learning and practice project** to simulate real‑world data engineering workflows, including schema enforcement, batch processing, validation, logging, and fault‑tolerant file handling.

***

## Key Features

*   Reads raw CSV files from a configurable source directory
*   Applies **schema‑driven column mapping and ordering**
*   Validates data types (integer, float, timestamp)
*   Converts CSV batches into **line‑delimited JSON**
*   Writes dataset‑organized output to a target directory
*   Processes **multiple datasets automatically**
*   Robust error handling and structured logging

***

## Project Structure

    project-folder/
    │
    ├── .gitignore
    ├── .env                     # Environment variables (source / target directories)
    ├── app.py                   # Main CSV → JSON pipeline
    ├── requirements.txt         # Python dependencies
    ├── README.md
    └── data/
        ├── retail_db/           # Raw CSV input
        │   ├── orders/
        │   │   └── part-00000
        │   ├── customers/
        │   │   └── part-00000
        │   └── schemas.json     # Dataset schemas and column definitions
        └── retail_db_json/      # Generated JSON output

***

## Schema‑Driven Design

All datasets are defined in a single `schemas.json` file.  
Each dataset schema specifies:

*   Column name
*   Column position
*   Expected data type (`integer`, `float`, `string`, `timestamp`)

The pipeline uses this schema to:

1.  Order columns correctly
2.  Validate data
3.  Enforce consistent output structure

***

## How the Pipeline Works

1.  Load dataset schemas from `schemas.json`
2.  Discover CSV `part-*` files per dataset
3.  Read CSV files using schema‑defined column names
4.  Validate column data types
5.  Convert validated data into pandas DataFrames
6.  Write **newline‑delimited JSON** output to the target directory
7.  Log errors without stopping the entire pipeline

***

## Example Transformation

### CSV Input

    1,2,Quest Q64 10 FT. x 10 FT. Slant Leg Instant U,,59.98,http://images.acmesports.sports/Quest+Q64...

### JSON Output (Line‑Delimited)

```json
{"product_id":"1","product_category_id":"2","product_name":"Quest Q64 10 FT. x 10 FT. Slant Leg Instant U","product_description":null,"product_price":"59.98","product_image":"http://images.acmesports.sports/Quest+Q64..."}
```

✅ Output format is **newline‑delimited JSON (JSONL)** for scalability and downstream processing.

***

## Setup

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

***

### 2. Configure Environment Variables

Create or update the `.env` file:

```env
SRC_BASE_DIR=data/retail_db
TGT_BASE_DIR=data/retail_db_json
SCHEMAS_PATH=data/retail_db/schemas.json
```

These variables control where input data is read from and where output data is written.

***

## Usage

### Process All Datasets

```bash
python app.py
```

***

### Process Specific Datasets

Pass a JSON list of dataset names:

```bash
python app.py '["orders", "customers"]'
```

Only the specified datasets will be processed.

***

## Logging & Error Handling

*   Application logs are written to `app.log`
*   Invalid files do **not** stop the pipeline
*   Validation errors are logged with row counts and column names
*   Missing schemas or malformed files are skipped safely

***

## Technologies Used

*   Python
*   pandas
*   python‑dotenv
*   JSON / CSV

***

## Limitations

*   Schema enforcement is validation‑based (does not auto‑coerce types)
*   JSON output uses string values for consistency with raw CSV input
*   Not optimized for very large files (single‑file pandas reads)

***

## Future Improvements

*   ✅ Add unit and integration tests
*   ✅ Support additional formats (Parquet, Avro)
*   ✅ Add type coercion after validation
*   ✅ Add incremental / streaming ingestion
*   ✅ Integrate with cloud storage (S3 / GCS / Azure Blob)
*   ✅ Add metadata and processing statistics

***

## Purpose

This project focuses on **core data engineering fundamentals**:

*   Schema enforcement
*   Batch ingestion patterns
*   Fault‑tolerant pipelines
*   Clean, maintainable Python code

It is intended for **learning, practice, and portfolio demonstration**.

***

