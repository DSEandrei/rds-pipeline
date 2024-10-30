import polars as pl
from _dtype_dictionary import Dtype_Dictionary
from loguru import logger
from typing import List, Tuple
from snowflake.snowpark import Session



def query_data(
        pg_cursor, 
        query: str, 
        rds_table: str,
        dtype_dictionary: Dtype_Dictionary) -> List[Tuple]:
    
    logger.info(f"Querying data from {rds_table} in RDS...")
    pg_cursor.execute(f"{query}")
    columns = list(pg_cursor.description)
    rows = pg_cursor.fetchall()
    
    columns_dict = {}
   
    for col in columns:
        if col.type_code in (1184, 1114):                                           # The type code for datetime is typically 
            columns_dict[col.name.upper()] = pl.Datetime                            # represented as 1114 for timestamp and 1184 for timestamptz
        elif col.type_code == 3802:                                                 # represented as 3802 for Object type
            columns_dict[col.name.upper()] = pl.Object                              # represented as 1082 for Date type
        elif col.type_code == 1082:                                                 # - ChatGPT
            columns_dict[col.name.upper()] = pl.Date                                     
        else:                                                                           
            columns_dict[col.name.upper()] = pl.String  

    dtype_dictionary.set_dictionary(columns_dict)   

    data = pl.DataFrame(
        rows,
        schema = columns_dict,
        orient='row'
    )

    logger.info(f"Done querying data from {rds_table} in RDS...")

    return data

def get_prev_records(session: Session, 
                     table_name : str,
                     dtype_dictionary : Dtype_Dictionary) -> pl.DataFrame:

    df_backup = pl.DataFrame()

    try:
        df_backup = session.sql(f'SELECT * from \"{table_name}\"')

        rows = df_backup.collect()          
        
        data = [list(row) for row in rows]

        columns_dict = dtype_dictionary.get_dictionary()


        polars_df = pl.DataFrame(
            data, 
            schema=columns_dict, 
            orient='row',
            )
         
        return polars_df
    except Exception as e:
        empty_df = pl.DataFrame()
        return empty_df
    

def update_insert (
        incoming_data : pl.DataFrame,
        current_data : pl.DataFrame,
        primary_column : str) -> pl.DataFrame:
    
    new_dataframe = pl.DataFrame()

    if not current_data.is_empty():
    
        new_dataframe = current_data.update(             
            incoming_data,                            
            on=primary_column.upper(),                          
            how='full',                                     
            include_nulls=True
        )                             
        
        unique_new_df = new_dataframe.unique(               
            subset=primary_column.upper(),                     
            keep='last',                                   
            maintain_order=True                             
        )

        return unique_new_df
    else:
        unique_incoming_df = incoming_data.unique(
            subset=primary_column.upper(),                                  
            keep='last',
            maintain_order=True
        )
        return unique_incoming_df

def load_data(
        snowflake_session : Session, 
        data : pl.DataFrame, 
        snowflake_table: str) -> None:
    
    snowflake_table = snowflake_table.upper()

    if data.is_empty():
        logger.warning(f"No data to load to {snowflake_table}.")
        return
    
    logger.info(f"Populating {snowflake_table} Table...")

    table_dict = data.to_dicts()

    snowpark_df = snowflake_session.create_dataframe(table_dict)     

    logger.info(f'Truncating {snowflake_table} table...')

    snowpark_df.write.save_as_table(snowflake_table, mode='overwrite')
 