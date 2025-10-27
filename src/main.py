import io
import boto3
import os
import pandas as pd
from sqlalchemy import create_engine
import logging
from logging.handlers import TimedRotatingFileHandler


def setup_logger():

    logger = logging.getLogger("data_pipeline")
    logger.setLevel(logging.INFO)

    if logger.handlers:
        return logger

    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    os.makedirs("logs", exist_ok=True)

    file_handler = TimedRotatingFileHandler(
        "logs/data_pipeline.log", when="midnight", interval=1, backupCount=7
    )

    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger

logger = setup_logger()

def main():

    logger.info("Starting data pipeline...")

    s3 = boto3.client(
        "s3",
        endpoint_url="http://localhost:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
    )
    logger.info("Connected to MinIO")

    logger.info("Loading customers.csv...")
    customers_file = s3.get_object(Bucket="raw", Key="customers.csv")
    customers = pd.read_csv(io.BytesIO(customers_file["Body"].read()))
    logger.info(f"Loaded {len(customers)} rows from customers.csv")

    logger.info("Loading products.json...")
    products_file = s3.get_object(Bucket="raw", Key="products.json")
    products = pd.read_json(io.BytesIO(products_file["Body"].read()))
    logger.info(f"Loaded {len(products)} rows from products.json")

    logger.info("Loading sales.parquet...")
    sales_file = s3.get_object(Bucket="raw", Key="sales.parquet")
    sales = pd.read_parquet(io.BytesIO(sales_file["Body"].read()))
    logger.info(f"Loaded {len(sales)} rows from sales.parquet")

    logger.info("Connecting to Postgres...")
    engine = create_engine("postgresql://myuser:mypassword@localhost:5432/postgres")
    
    logger.info("inserting customers into database...")
    customers.to_sql("customers", engine, if_exists="replace", index=False)

    logger.info("inserting products into database...")
    products.to_sql("products", engine, if_exists="replace", index=False)

    logger.info("inserting sales into database...")
    sales.to_sql("sales", engine, if_exists="replace", index=False)

    logger.info("Data inserted into the database successfully!")
    logger.info("Data pipeline completed.")
    
if __name__ == "__main__":
    main()
