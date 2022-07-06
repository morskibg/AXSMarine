import asyncio
from asyncio.events import AbstractEventLoop
from concurrent.futures import ProcessPoolExecutor
from functools import partial
import re
import functools
import time
from typing import List, NamedTuple
import aiosqlite
import pandas as pd
import numpy as np

from utils import calculate_coordinates_and_update_df
from logger import get_logger

module_logger = get_logger(__name__) 


def async_timed():
    """
    Async decorator measurng elapsed time of the decorated function.
    """    
    def wrapper(func):
        @functools.wraps(func)
        async def wrapped(*args, **kwargs):            
            t1 = time.perf_counter()
            try:
                return await func(*args, **kwargs)
            finally:
                t2 = time.perf_counter()
                total = t2 - t1
                module_logger.info(f'Finished running async {func.__name__} in {total:.4f} second(s)')
        return wrapped
    return wrapper


async def async_upsert_to_sqlite(
    df: pd.DataFrame, 
    db_connection: aiosqlite.Connection, 
    db_name: str,
    table_name: str,     
    remove_nan: bool = False) -> None:

    """Perform upsert(insert on duplicate update) trough specific to Sqlite query creation. 
    Some simple sanitizaion of input data is performed.
    Requirements: 1.Dataframe columns MUST be exactly the same and in the same order as SQL table.

    Args:
        df (pd.DataFrame): data to be written into sqllite db
        db_connection (aiosqlite.Connection): async connection to sqlite db
        db_name (str): sqlite db name
        table_name (str): sqlite table name
        remove_nan (bool, optional): Need None in the input dataframe to be replaced with zeros. Defaults to False.

    Raises:
        ValueError: empty data frame passed
        ValueError: error writing to sqlite file
    """    
    
    if df.empty:
        raise ValueError('The input dataframe is empty. Nothing to write to sqlite file.')

    df = df.fillna(value='NULL') if not remove_nan else df.fillna(0)
    fields = str(tuple(df.columns.values))
    fields = re.sub('[\']', "", fields)
    fields = re.sub('(,\))', ")", fields)
    tuples = [tuple(x) for x in df.to_numpy()]

    for t in tuples:
        tuples_to_insert = str(t)
        tuples_to_insert = re.sub('(NULL)', "#NULL#", tuples_to_insert)
        tuples_to_insert = re.sub('[\[\]]|(\'#)|(#\')', "", tuples_to_insert)
        tuples_to_insert = re.sub('(,\))', ")", tuples_to_insert)
        tuples_to_insert = re.sub("%", "%%", tuples_to_insert)

        sql_str = f"INSERT INTO {table_name} {fields} VALUES {tuples_to_insert} ON CONFLICT(id) DO UPDATE SET {','.join([x + ' = EXCLUDED.' + x  for x in df.columns.values])} "
        
        try:            
            await db_connection.execute(sql_str)
            await db_connection.commit()            
        except Exception as ex:            
            raise ValueError(f'Error writing to table {table_name} from {db_name}.\n{ex}')

@async_timed()
async def multuproc_multithread_handler(
        batches: List[List[int]], kwargs_tupple: NamedTuple) -> None:
    """Creates coroutines from batches and then awaits them

    Args:
        batches (List[List[int]]): all cities indicies as list of lists
        kwargs_tupple (NamedTuple): input arguments
    """

    coroutines = [proceed_batch_data(batch, kwargs_tupple, idx + 1) for idx,batch in enumerate(batches)]
    await asyncio.gather(*coroutines)

@async_timed()
async def proceed_batch_data(
    indices: List[int], 
    kwargs_tupple: NamedTuple, 
    number_of_thread:int) -> None:

    """Reading from db for given batch of cities' indices and creating dataframe.
    Creating multiprocess executor with specified number of workers and proceed with coordinates and distance calculations.
    Finaly updating dataframe and asynchronously.
    
    Args:
        indices (List[int]): _description_
        kwargs_tupple (NamedTuple): db_name, table_name, max_workers
        number_of_thread (int) number of calling thread

    Raises:
        ValueError: can't get data from sqlite table
        ValueError: error writing to sqlite file
    """
    async with aiosqlite.connect(f'{kwargs_tupple.db_name}.db') as connection:
        try:
            async with connection.execute(f'SELECT * FROM {kwargs_tupple.table_name} WHERE id IN {indices}') as cursor:
                columns = [col_info[0] for col_info in cursor.description]
                all_data = await cursor.fetchall()
        except :
            module_logger.info(f"Exception: Can't connect to specified table: {kwargs_tupple.table_name} from {kwargs_tupple.db_name}.db")
            raise ValueError(f"Can't connect to specified table: {kwargs_tupple.table_name} from {kwargs_tupple.db_name}.db")
        else:
            curr_df = pd.DataFrame.from_records(all_data, columns=columns)
            df_splits = np.array_split(curr_df, kwargs_tupple.max_workers)                
            updated_df = pd.DataFrame
            with ProcessPoolExecutor(max_workers = kwargs_tupple.max_workers) as process_pool:
                loop: AbstractEventLoop = asyncio.get_running_loop()
                calls = [
                    partial(
                        partial(
                            partial(calculate_coordinates_and_update_df, df)
                            ,idx + 1),
                            number_of_thread)
                            for idx, df in enumerate(df_splits)
                ]
                coroutines = [loop.run_in_executor(process_pool, call) for call in calls]
                updated_df = pd.concat(await asyncio.gather(*coroutines))
            try:
                await async_upsert_to_sqlite(updated_df, connection, kwargs_tupple.db_name, kwargs_tupple.table_name)
            except ValueError as ex:
                module_logger.info(f'Exception: {ex}')
                raise
        


