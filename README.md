
1. Create virtual enviroment and activate it (optional)
2. Install dependencies from 'requirements.txt'
3. Run python main.py
```
Usage: main.py --kwargs | -k db_name=task table_name=cities batch_size=64 max_workers=10 mode=async
db_name: str = 'task' -> name of the sqlite databse,
table_name: str = 'cities' -> name of the table,
batch_size: int = 64 -> number of db rows to be procesed its own thread,
max_workers: int = 10 -> number of proceses for each batch of db rows,
mode: str = 'async' -> may be 'async' or 'sync'. If sync is choosen all 
data retriving and computations will be done sequentialy.
```
Added interactive 'playground.ipynb' for simple visualizations.
