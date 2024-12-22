from dtypes_dictionary import dtype_dict
from loguru import logger
from snowflake.snowpark import Session
from snowflake.snowpark.functions import lit
from datetime import datetime
from memory_profiler import profile


# log_file= open('memory_logs/memory7.log', 'w+')
# log_file1= open('memory_logs/memory7.1.log', 'w+')
# log_file2= open('memory_logs/memory7.2.log', 'w+')
 
# @profile(stream=log_file)
def query_data(
        pg_cursor,
        query: str,
        rds_table: str,
        snowflake_session : Session,
        snowflake_table: str,
        ) -> None:
    pg_cursor.execute(f"{query}")
    logger.info(f"Fetching data from {rds_table} in RDS...")
    rows = pg_cursor.fetchall()

    schema = dtype_dict[snowflake_table]

    snowpark_df = snowflake_session.create_dataframe(
        rows, schema)
    
    new_df = append_date(snowpark_df)
    
    return new_df

def append_date(data):
    curr_datetime = datetime.now()

    df_with_date = data.with_column("INGESTED_DATE", lit(curr_datetime))

    return df_with_date
    
# @profile(stream=log_file2)
def load_data(
        data,
        snowflake_table: str) -> None:

    snowflake_table = snowflake_table.upper()
    row_count = data.count()

    if row_count == 0:
        logger.warning(f"No data to load to {snowflake_table}.")
        return

    data.write.save_as_table(snowflake_table, mode='overwrite', block=True)
 