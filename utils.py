import sys
import time
import re
from contextlib import closing
import sqlite3
from typing import List, NamedTuple
from collections import namedtuple
import functools
import pandas as pd
from haversine import haversine

import store_locator
from settings import DB_NAME, TABLE_NAME, BATCH_SIZE, MAX_WORKERS, MODE
from logger import get_logger

module_logger = get_logger(__name__) 

def sync_timed(*args):
    """
    Sync decorator measurng elapsed time of the decorated function.
    """    
    def wrapper(func):
        @functools.wraps(func)
        def wrapped(*args, **kwargs) :            
            t1 = time.perf_counter()
            try:
                return func(*args, **kwargs)
            finally:
                t2 = time.perf_counter()
                total = t2 - t1
                module_logger.info(f'Finished running sync {func.__name__} in {total:.4f} second(s)')
        return wrapped
    return wrapper

def input_args_parser(argv: List[str]) -> NamedTuple :
    """Input CLI parametters parser. If are not specified, default values from settings.py will be applied.
    After correct parsing keyword arguments creates namedtuple with all arguments

    Args:
        argv (List[str]): command line arguments

    Raises:
        ValueError: raises if keyword argument flag (--kwarg or -k) is missing. Exit program.
        ValueError: raises if wrong named or type argument is passed. Exit program.

    Returns:
        NamedTuple: anmed tuple with folowing members: db_name, table_name, batch_size, max_workers
    """
    USAGE_MSG =(
    """
        Usage: main.py --kwargs | -k db_name=task table_name=cities batch_size=64 max_workers=10 mode=async
        db_name: str = 'task' -> name of the sqlite databse,
        table_name: str = 'cities' -> name of the table,
        batch_size: int = 64 -> number of db rows to be procesed its own thread,
        max_workers: int = 10 -> number of proceses for each batch of db rows,
        mode: str = 'async' -> may be 'async' or 'sync'. If sync is choosen all 
        data retriving and computations will be done sequentialy.
    """)
    kwargs = {
        'file':argv[0],
        'db_name':DB_NAME,
        'table_name':TABLE_NAME,
        'batch_size':BATCH_SIZE,
        'max_workers':MAX_WORKERS,
        'mode':MODE
    }
    
    if len(argv) > 1:
        try:
            if argv[1] not in ['--kwargs','-k']:
                raise ValueError('Keyword argument flag (--kwargs or -k) is missing.')

            input_dict = {arg.split('=')[0]:arg.split('=')[1] for arg in argv[2:] }
            for item in input_dict.items():
                if kwargs.get(item[0]):
                    kwargs[item[0]]= item[1] if item[0] in ['db_name', 'table_name', 'mode'] else int(item[1])
                else:
                    raise ValueError(f"Wrong argument name or type for {item[0]} - {item[1]}")
        except ValueError as ex:
            print(f'Exception: {ex}')
            print(USAGE_MSG)
            sys.exit(1)
           
    Kwargs = namedtuple("Kwargs", "db_name, table_name, batch_size, max_workers, mode")
    return Kwargs(*list(kwargs.values())[1:])

def get_all_cities_indices(db_name:str, table_name:str) -> List[int]:
    """Read all rows from sqlite db syncroniously.

    Args:
        db_name (str): sqlite db name
        table_name (str): table name
    Raises:
        ValueError: empty table 

    Returns:
        List[int]: List of all cities' indices
    """    
    
    with closing(sqlite3.connect(f'{db_name}.db')) as connection:
        with closing(connection.cursor()) as cursor:
            try:
                rows = cursor.execute(
                    f'SELECT id FROM {table_name}').fetchall()
                return tuple(x[0] for x in rows)
            except:
                raise ValueError(f'Missing data for table {table_name} in {db_name}.db file. Is there {db_name}.db file file in the working directory?')

def get_batches_from_cities_indices(indices: List[int], batch_size: int) -> List[List[int]]:
    """Create List of Lists of cities' indices. Each batch (except maybe the last one)
    contains number od indice specified from pargument 'batch_size'

    Args:
        indices (List[int]): List of all cities' indices
        batch_size (int): how many indices per batch

    Returns:
        List[List[int]]: List of Lists of cities' indices
    """    
    if indices:
        batch_size = BATCH_SIZE if len(indices) % BATCH_SIZE != 1 else BATCH_SIZE - 1
        batches = [indices[i:i + BATCH_SIZE]
                   for i in range(0, len(indices), batch_size)]   

        return batches
    else:
        return []

def calculate_coordinates_and_update_df(curr_df: pd.DataFrame, proces_num: int, thread_num: int) -> pd.DataFrame:   
    """Iterate through dataframe and call calculate_store_coordinates_and_distance for each paair of city's coordinates 

    Args:
        curr_df (pd.DataFrame): input dataframe from separate process
        proces_num (int): process number
        thread_num (int): thread number

    Returns:
        pd.DataFrame: updated dataframe 
    """
    t1 = time.perf_counter()  

    
    curr_df[['store_latitude', 'store_longitude', 'store_distance']] = curr_df.apply(
        lambda x: calculate_store_coordinates_and_distance(
                    x['latitude'], x['longitude']), axis=1
        )
    # use comented line to fill store data with None
    # curr_df[['store_latitude', 'store_longitude', 'store_distance']] = None, None, None
    t2 = time.perf_counter()   
    print(
        'Finished running coordinates and distance calculation from ' +
        f'{thread_num=} and {proces_num=} for {curr_df.shape[0]} records in {t2 - t1:.4f} second(s)')

    return curr_df

def calculate_store_coordinates_and_distance(city_latitude: float, city_longitude: float) -> pd.Series:
    """Calucalte store coordinate from input city's coordinates, and distence between city and the nearest store (in km)

    Args:
        city_latitude (float): latitude of the city
        city_longitude (float): longitude of the city

    Returns:
        pd.Series: Pandas Series with calculated nearest_store latitude, nearest_store longitude 
        and the distance between the city and the nearest store.
    """    

    try:
        nearest_store = store_locator.get_nearest_store(
            city_longitude, city_latitude)
    except ValueError:
        raise
    else:
        distance = haversine(
            (city_latitude, city_longitude), (nearest_store['latitude'], nearest_store['longitude']))
        return pd.Series([nearest_store['latitude'],
                          nearest_store['longitude'], distance])

def sync_upsert_to_sqlite(
    df: pd.DataFrame, 
    db_connection: sqlite3.Connection, 
    db_name: str,
    table_name: str,     
    remove_nan: bool = False) -> None:

    """Perform upsert(insert on duplicate update) trough specific to Sqlite query creation. 
    Some simple sanitizaion of input data is performed.
    Requirements: 1.Dataframe columns MUST be exactly the same and in the same order as SQL table.

    Args:
        df (pd.DataFrame): data to be written into sqllite db
        db_connection (sqlite3.Connection): async connection to sqlite db
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
            db_connection.execute(sql_str)
            db_connection.commit()            
        except Exception as ex:            
            raise ValueError(f'Error writing to table {table_name} from {db_name}.\n{ex}')

def sequential_handler(db_name:str, table_name: str) -> None:
    """Sequental data processing

    Args:
        db_name (str):sqlite db name_
        table_name (str): table name
    """   
    try: 
        connection = sqlite3.connect(f'{db_name}.db')
        df = pd.read_sql_query(f'SELECT * FROM {table_name}', connection)
        df[['store_latitude', 'store_longitude', 'store_distance']] = df.apply(
            lambda x: calculate_store_coordinates_and_distance(
                        x['latitude'], x['longitude']), axis=1
            )
        sync_upsert_to_sqlite(df,connection, db_name, table_name)
    except Exception as ex:
        module_logger.info(f'Exception from sequental handler {ex}')
        raise ValueError(f'Exception from sequental handler {ex}')
