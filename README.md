[Russian version](README_RU.md)

# Integration with Logs API
This script can help you to integrate Yandex.Metrica Logs API with ClickHouse.

If you have any questions, feel free to write comments, create issues on GitHub or write me (e-mail: miptgirl@yandex-team.ru).

## Requirements
Script uses Python 2.7 and also requires `requests` library. You can install this library using package manager [pip](https://pip.pypa.io/en/stable/installing/)
```bash
pip install requests
```

Also, you need a running ClickHouse instance to load data into it. Instruction how to install ClickHouse can be found on [official site](https://clickhouse.yandex/).

## Setting up
First of all, you need to fill in [config](./configs/config.json)
```javascript
{
	"token" : "<your_token>", // token to access Yandex.Metrica API
	"counter_id": "<your_counter_id>",
	"visits_fields": [ // list of params for visits
	    "ym:s:counterID",
	    "ym:s:dateTime",
	    "ym:s:date",
	    "ym:s:firstPartyCookie"
	],
	"hits_fields": [ // list of params for hits
	    "ym:pv:counterID",
	    "ym:pv:dateTime",
	    "ym:pv:date",
	    "ym:pv:firstPartyCookie"
	],
	"log_level": "INFO", 
	"retries": 1, 
	"retries_delay": 60, // delay between retries
	"clickhouse": {
		"host": "http://localhost:8123", 
		"user": "", 
		"password": "",
		"visits_table": "visits_all", // table name for visits
		"hits_table": "hits_all", // table name for hits
		"database": "default" // database name
	}
}
```

On first execution script creates all tables in database according to config. So if you change parameters, you need to drop all tables and load data again or add new columns manually using [ALTER TABLE](https://clickhouse.yandex/reference_ru.html#ALTER).

## Running a program

When running the program you need to specify a souce (hits or visits) using option `-source`.

Script has several modes:
 * __history__ - loads all the data from day one to the day before yesterday
 * __regular__ - loads data only for day before yesterday (recommended for regular downloads)
 * __regular_early__ - loads yesterday data (yesterday data may be not complete: some visits can lack page views)
 
Example:
```bash
python metrica_logs_api.py -mode history -source visits
```

Also you can load data for particular time period:
```bash
python metrica_logs_api.py -source hits -start_date 2016-10-10 -end_date 2016-10-18
```
