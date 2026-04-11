from dotenv import load_dotenv
from pathlib import Path
import pandas as pd
import json
import os
import sys
import logging

logger = logging.getLogger(__name__)

DATA_TYPE_MAP = {
    "integer": int,
    "float": float,
    "string": str,
    "timestamp": "timestamp"
}

def load_schemas(schemas_path):
    try:
        with open(schemas_path) as file:
            return json.load(file)
        
    except FileNotFoundError as e:
        logger.error(f"Schema file not found: {schemas_path}")
        raise

    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in schema file: {schemas_path}")
        raise

    except PermissionError as e:
        logger.error(f"Permission denied when reading schema file: {schemas_path}")
        raise

    except Exception as e:
        logger.error(f"Unexpected error while loading schemas: {e}")
        raise


def validate_dataframe(df, schemas, ds_name):
    errors = []
    schema_cols = schemas[ds_name]

    for col_def in schema_cols:
        col_name = col_def["column_name"]
        dtype = col_def["data_type"]

        # Allow empty data type
        if dtype == "":
            continue

        elif dtype == "integer":
            invalid_rows = df[~df[col_name].astype(str).str.match(r"^-?\d+$")]
            if not invalid_rows.empty:
                errors.append(f"{len(invalid_rows)} rows have invalid integer in column '{col_name}'")
                
        elif dtype == "float":
            invalid_rows = df[~df[col_name].astype(str).str.match(r"^-?\d+(\.\d+)?$")]
            if not invalid_rows.empty:
                errors.append(f"{len(invalid_rows)} rows have invalid float in column '{col_name}'")

        elif dtype == "timestamp":
            try:
                pd.to_datetime(df[col_name], errors="raise")
            except Exception:
                errors.append(f"Invalid timestamp format in column '{col_name}'")

        elif dtype == "string":
            pass

    return errors


def get_column_names(schemas, ds_name, sorting_key="column_position"):
    column_details = sorted(schemas[ds_name], key=lambda col: col[sorting_key])
    return [col["column_name"] for col in column_details]


def read_csv(file_path, schemas):
    ds_name = Path(file_path).parent.name
    
    try:
        column_names = get_column_names(schemas, ds_name)
    except KeyError as e:
        logger.error(f"Schema missing or invalid for dataset '{ds_name}'. File path: {file_path}")
        raise

    try:
        df = pd.read_csv(file_path, names=column_names, header=None, dtype=str)
    
    except FileNotFoundError as e:
        logger.error(f"CSV file not found: {file_path}")
        raise
    
    except pd.errors.EmptyDataError:
        logger.warning(f"CSV file is empty: {file_path}. Skipping.")
        raise

    except pd.errors.ParserError as e:
        logger.error(f"Failed to parse CSV '{file_path}': {e}")
        raise

    except PermissionError:
        logger.error(f"Permission denied reading CSV file: {file_path}")
        raise

    except Exception as e:
        logger.error(f"Unexpected error reading CSV '{file_path}': {e}")
        raise
    
    validation_errors = validate_dataframe(df, schemas, ds_name)
    if validation_errors:
        for err in validation_errors:
            logger.error(f"Validation error in {file_path}: {err}")
        
        raise ValueError(f"Validation failed for file '{file_path}'. See log for details.")

    return df

def to_json(df, tgt_base_dir, ds_name, file_name):
    json_file_dir = Path(tgt_base_dir) / ds_name

    try:
        json_file_dir.mkdir(parents=True, exist_ok=True)

    except PermissionError:
        logger.error(f"Permission denied creating directory: {json_file_dir}")
        raise

    except OSError as e:
        logger.error(f"Failed to create directory '{json_file_dir}': {e}")
        raise

    json_file_path = json_file_dir / file_name

    try:
        df.to_json(
            json_file_path,
            orient="records",
            lines=True
        )

    except PermissionError:
        logger.error(f"Permission denied writing JSON file: {json_file_path}")
        raise

    except OSError as e:
        logger.error(f"OS error writing JSON file '{json_file_path}': {e}")
        raise

    except ValueError as e:
        logger.error(f"Invalid data when writing JSON file '{json_file_path}': {e}")
        raise

    except TypeError as e:
        logger.error(f"Type error during JSON serialization '{json_file_path}: {e}'")
        raise

    except Exception as e:
        logger.error(f"Unexpected error writing JSON '{json_file_path}': {e}")
        raise


def file_converter(src_base_dir, tgt_base_dir, ds_name, schemas):
    src_path_list = Path(src_base_dir).glob(f"{ds_name}/part-*")

    files_found = False

    for file in src_path_list:
        files_found = True
        logger.info(f"Processing file: {file}")

        try:
            df = read_csv(file, schemas)

        except Exception as e:
            logger.error(f"Failed to read CSV file '{file}': {e}")
            continue

        file_name = Path(file).name

        try:
            to_json(df, tgt_base_dir, ds_name, file_name)

        except Exception as e:
            logger.error(f"Failed to write JSON for '{file}': {e}")
            continue
    
    if not files_found:
        logger.warning(f"No part-files found for dataset '{ds_name}'")


def process_files(src_base_dir, tgt_base_dir, schemas, ds_names=None):
    if not ds_names:
        ds_names = schemas.keys()

    for ds_name in ds_names:
        logger.info(f"Processing dataset: {ds_name}")

        try:
            file_converter(src_base_dir, tgt_base_dir, ds_name, schemas)
        except KeyError as e:
            logger.error(f"Schema missing for dataset '{ds_name}'. Skipping dataset.")
            continue

        except Exception as e:
            logger.error(f"Dataset '{ds_name}' failed due to unexpected error: {e}")
            continue



if __name__ == "__main__":

    
    load_dotenv()
    logging.basicConfig(
        filename="app.log", 
        filemode="w", 
        format="%(asctime)s - %(levelname)s:%(message)s",
        datefmt='%m/%d/%Y %I:%M:%S %p', 
        encoding="utf-8", 
        level=logging.DEBUG
    )

    src_base_dir = Path(os.environ.get("SRC_BASE_DIR"))
    tgt_base_dir = Path(os.environ.get("TGT_BASE_DIR"))
    schemas_path = Path(os.environ.get("SCHEMAS_PATH"))
   
    schemas = load_schemas(schemas_path)

    if len(sys.argv) == 2:
        ds_names = json.loads(sys.argv[1])
        process_files(src_base_dir, tgt_base_dir, schemas, ds_names)
    else:
        process_files(src_base_dir, tgt_base_dir, schemas)