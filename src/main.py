import os
import ssl
import psycopg2
import psycopg2.extras
import gc
from snowflake.snowpark import Session
from dotenv import load_dotenv
from loguru import logger
from datetime import datetime
from src.utils import query_data, load_data
from src.config import QUERY_TABLES
from memory_profiler import profile


# Disable SSL verification
ssl._create_default_https_context = ssl._create_unverified_context
log_file= open('memory_logs/memory_main.log', 'w+')

@profile(stream=log_file)
def main(e, c) -> None:
    start_time = datetime.now()

    load_dotenv()

    logger.info("Initializing RDS connection...")

    pg_conn = psycopg2.connect(

        host = os.environ.get('RDS_HOST'),
        dbname = os.environ.get('RDS_DATABASE'),
        user = os.environ.get('RDS_USER'),
        password = os.environ.get('RDS_PASSWORD')

    )

    logger.info("Initializing Snowflake connection...")

    snowflake_connection_params = {
        'account' : os.environ.get('SNOWFLAKE_ACCOUNT'),
        'user' : os.environ.get('SNOWFLAKE_USER'),
        'password': os.environ.get('SNOWFLAKE_PASSWORD'),
        'role': os.environ.get('SNOWFLAKE_ROLE'),
        'warehouse' : os.environ.get('SNOWFLAKE_WAREHOUSE'),
        'database' : os.environ.get('SNOWFLAKE_DATABASE'),
        'schema' : os.environ.get('SNOWFLAKE_SCHEMA')
    }

    try:
        logger.info('Connecting to Snowflake...')
        session = Session.builder.configs(snowflake_connection_params).create()
        session.sql(f'USE WAREHOUSE {os.environ.get("SNOWFLAKE_WAREHOUSE")}').collect()
        logger.success('Successfully connected to Snowflake!')

        logger.info('Altering timezone to UTC...')
        session.sql("ALTER SESSION SET TIMEZONE = 'UTC'").collect()

        for i, item in enumerate(QUERY_TABLES):
            logger.info('Connecting to RDS...')
            pg_cursor = pg_conn.cursor(name=f'rds_cursor_{i+1}')

            rds_table = item["rds_table"]
            query = item["query"]
            snowflake_table = item["snowflake_table"].upper()

            logger.info(f"Querying data from {rds_table} in RDS...")
            data = query_data(pg_cursor,
                        query,
                        rds_table,
                        session,
                        snowflake_table)

            logger.info('Loading data into Snowflake...')
            load_data(    
                      data,
                      snowflake_table)

            logger.info(f"Tables : {i+1} / {len(QUERY_TABLES)}")

            # Manually de-allocates memory
            data = None
            gc.collect()
    except Exception as e:
        logger.error('Closing connections to RDS and Snowflake...')
        logger.error(e)
    finally:
        pg_cursor.close()
        pg_conn.close()
        session.close()
         
    end_time = datetime.now()
    total_runtime = end_time - start_time

    logger.info(f"Start of run time: {start_time}")
    logger.info(f"End tme: {end_time}")
    logger.info(f"Total runtim: {total_runtime}")


if __name__ == "__main__":
    main(0,0)