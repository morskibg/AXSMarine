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
   

    # with ProcessPoolExecutor() as process_pool:
          
    #     loop: AbstractEventLoop = asyncio.get_running_loop()
    #     calls = [partial(partial(proceed_batch_data, batch),idx) for idx,batch in enumerate(batches)] 
    #     call_coros = []
    #     for call in calls:
    #         call_coros.append(await loop.run_in_executor(process_pool, call))
    #     await asyncio.gather(*call_coros)

  
    

    # for idx,batch in enumerate(batches):
    #     proceed_batched(batch,idx)

    # coroutines = [proceed_batch_data(batch, idx)
    #               for idx, batch in enumerate(batches)]

    # await asyncio.gather(*coroutines)





    # with ProcessPoolExecutor() as process_pool:
    #     nums = [1, 100000000, 3, 5, 22]
    #     loop: AbstractEventLoop = asyncio.get_running_loop()
    #     calls: List[partial[int]] = [partial(partial(count, num),idx) for idx,num in enumerate(nums)] 
    #     call_coros = []
    #     for call in calls:
    #         call_coros.append(loop.run_in_executor(process_pool, call))
    #     await asyncio.gather(*call_coros)
        
     
    
    # coroutines = [proceed_batch_data(batch, idx)
    #               for idx, batch in enumerate(batches)]

    # await asyncio.gather(*coroutines)

  

if __name__ == "__main__":
    kwargs_tupple = input_args_parser(sys.argv)
    asyncio.run(main(kwargs_tupple))