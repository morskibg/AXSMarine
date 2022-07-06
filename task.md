### Python task

The company "XYZ" needs to have the information where is the nearest brand store to a collection of cities.

**Input**:
- There is a SQLite file `task.db` with `cities` table containing city records.
```SQLite
CREATE TABLE `cities` (
  `id` mediumint unsigned NOT NULL,
  `name` varchar(255) NOT NULL,
  `state_id` mediumint unsigned NOT NULL,
  `state_code` varchar(255) NOT NULL,
  `country_id` mediumint unsigned NOT NULL,
  `country_code` char(2) NOT NULL,
  `latitude` decimal(10,8) NOT NULL,
  `longitude` decimal(11,8) NOT NULL,
  `created_at` timestamp NOT NULL,
  `updated_at` timestamp NOT NULL,
  `flag` tinyint(1) NOT NULL DEFAULT '1',
  `wikiDataId` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`)
)
```
Note that this is a limited sample amount of city records. The provided solution should be applicable on much larger collection.
- There is a provided python module `store_locator` that gives the nearest store by given coordinates.

```Python
import store_locator

nearest_store = store_locator.get_nearest_store(42.50779, 1.52109)
print('Output: ', nearest_store) #Output:  {'store_id': 3520278, 'latitude': 1.32335, 'longitude': 36.98178} 
```

**Expected result**:

A Python script (written on Python 3.8+) that stores the following information into a database for a given collection of cities: 
- the nearest store for each city
- the distance from the city to the store?

########################################### _Solution_ ####################################
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
