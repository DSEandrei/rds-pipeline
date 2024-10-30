import os
import ssl
import psycopg2
import psycopg2.extras
from _dtype_dictionary import Dtype_Dictionary
from snowflake.snowpark import Session
from dotenv import load_dotenv
from loguru import logger
from datetime import datetime
from utils import query_data, get_prev_records, update_insert, load_data
from config import QUERY_TABLES

# Disable SSL verification
ssl._create_default_https_context = ssl._create_unverified_context

def main(e, c) -> None:

    start_time = datetime.now()

    load_dotenv()

    dtype_dict = Dtype_Dictionary()

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

        logger.info('Connecting to RDS...')
        pg_cursor = pg_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        logger.info('Connecting to Snowflake...')
        session = Session.builder.configs(snowflake_connection_params).create()
        session.sql(f'USE WAREHOUSE {os.environ.get("SNOWFLAKE_WAREHOUSE")}').collect()
        logger.success('Successfully connected to Snowflake!')

        logger.info('Altering timezone to UTC...')
        session.sql("ALTER SESSION SET TIMEZONE = 'UTC'").collect()

        for item in QUERY_TABLES:
            
            rds_table = item["rds_table"]
            query = item["query"]
            snowflake_table = item["snowflake_table"].upper()

            logger.info('Querying data to RDS...')
            data = query_data(pg_cursor, query, rds_table, dtype_dict)

            logger.info('Making a backup of Snowflale Tables...')
            main_table_backup = get_prev_records(session, snowflake_table, dtype_dict)

            logger.info('Doing UpSert...')
            updated_data = update_insert(data, main_table_backup, item['primmary_column'])

            if not main_table_backup.is_empty():

                logger.info('Making a backup of Archive Table...')
                archive_table_backup = get_prev_records(session, f'{snowflake_table}_ARCHIVE', dtype_dict)

                logger.info('Doing UpSert for Archive Table...')
                updated_archive_data = update_insert(main_table_backup, archive_table_backup, item['primmary_column'])

                logger.info('Loading data into Snowflake Archive...')
                load_data(session, updated_archive_data, f'{snowflake_table}_ARCHIVE')

            logger.info('Loading data into Snowflake...')
            load_data(session, updated_data, snowflake_table)
            
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