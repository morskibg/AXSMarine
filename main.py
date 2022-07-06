import sys
import asyncio
import time
from typing import List, NamedTuple

from utils import get_all_cities_indices, get_batches_from_cities_indices, input_args_parser, sequential_handler
from async_utils import multuproc_multithread_handler
from logger import get_logger

module_logger = get_logger('main' if __name__ == '__main__' else __name__) 

async def main(kwargs_tupple: NamedTuple) -> None:
    """Main async routine performing:
    1.Getting all cities' indices in the database.
    2.Create batches from all cities' indices with the specified length    
    3. Concurently execution
    """   
    t1 = time.perf_counter() 
    print(f'Working in {kwargs_tupple.mode} mode ...')
    if kwargs_tupple.mode == 'async':
        try:
            cities_indices = get_all_cities_indices(kwargs_tupple.db_name, kwargs_tupple.table_name)
        except ValueError as ex:
            print(f'Exception: {ex}')
            sys.exit(1)
        batches = get_batches_from_cities_indices(cities_indices, kwargs_tupple.batch_size)    
        try:
            module_logger.info(f'Starting with batch_size={kwargs_tupple.batch_size} and max_workers={kwargs_tupple.max_workers}')
            await multuproc_multithread_handler(batches, kwargs_tupple)
        except ValueError as ex:
            print(f'Exception: {ex}')
    elif kwargs_tupple.mode == 'sync':
        try:
            sequential_handler(kwargs_tupple.db_name, kwargs_tupple.table_name)
        except ValueError as ex:
            print(f'Exception: {ex}')
            sys.exit(1)

    t2 = time.perf_counter()
    module_logger.info(f"Total elapsed time {t2-t1:.4f} second(s)")
    print(f"Total elapsed time {t2-t1:.4f} second(s)")

if __name__ == "__main__":
    kwargs_tupple = input_args_parser(sys.argv)
    asyncio.run(main(kwargs_tupple))