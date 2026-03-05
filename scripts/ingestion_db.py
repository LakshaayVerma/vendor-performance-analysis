import pandas as pd
import os
from sqlalchemy import create_engine
import logging
import time

# Configure logging
logging.basicConfig(
    filename="logs/ingestion_db.log",
    level=logging.DEBUG, 
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)

engine = create_engine("sqlite:///inventory.db")

def ingest_db_chunk(df, table_name, first_chunk, engine):
    """Example ingestion function, replace with your actual implementation"""
    if first_chunk:
        df.to_sql(table_name, engine, if_exists='replace', index=False)
    else:
        df.to_sql(table_name, engine, if_exists='append', index=False)

def load_raw_data():
    """Process all CSV files in 'data' folder and ingest into SQLite with timing"""
    overall_start = time.time()
    
    for file in os.listdir("data"):
        if file.endswith(".csv"):
            file_path = os.path.join("data", file)
            table_name = file[:-4]  # table name same as file
            first_chunk = True

            logging.info(f"Started processing file: {file}")
            file_start = time.time()

            # Read CSV in small chunks to avoid memory / SQLite errors
            for chunk in pd.read_csv(file_path, chunksize=5000):
                chunk_start = time.time()
                logging.debug(f"{file} -> Processing chunk with shape {chunk.shape}")
                ingest_db_chunk(chunk, table_name, first_chunk, engine)
                first_chunk = False
                chunk_end = time.time()
                logging.debug(f"{file} -> Chunk processed in {chunk_end - chunk_start:.2f} seconds")

            file_end = time.time()
            logging.info(f"Finished processing file: {file} in {file_end - file_start:.2f} seconds")

    overall_end = time.time()
    logging.info(f"All files loaded successfully in {overall_end - overall_start:.2f} seconds")

if __name__ == '__main__':
    load_raw_data()