# datadiff
[![Go Report Card](https://goreportcard.com/badge/github.com/arturom/datadiff)](https://goreportcard.com/report/github.com/arturom/datadiff)

A library and CLI tool to find differences between a master data source and a slave data source. Two data sources are equal if they contain the same numeric IDs. Rather than comparing record by record, this library compares [histogram](https://en.wikipedia.org/wiki/Histogram)-like aggregations.

### Strategy
 - Create a histogram of the numeric IDs on the master data source.
 - Create a histogram of the numeric IDs on the slave data source.
 - Merge and compare the histograms.
 - If the bin capacities are full, mark this range as resolved.
 - Fetch the histogram of the unresolved bins with smaller bin sizes.
 - Merge and compare the histograms.
 - Fetch the ids of the unresolved bins.
 - Diff the numeric IDs of unresolved bins and output the results.

### Supported Data Sources
  - mysql
  - elasticsearch ~0.90.13, ~1.0

#### MySQL

### Usage
```bash
$ ./datadiff -h
Usage of ./datadiff:
  -interval int
        Initial histogram interval (default 1000)
  -mconf string
        Master configuration string (default "{}")
  -mconn string
        Master connection string
  -mdriver string
        Master driver [elasticsearch|mysql]
  -sconf string
        Slave configuration string (default "{}")
  -sconn string
        Slave connection string
  -sdriver string
        Slave driver [elasticsearch|mysql]
```

### Sample Command Line Usage
```bash
 datadiff -interval 200 \
 -mdriver 'mysql' \
 -mconn 'root:root@(localhost:3306)/my_db_name?charset=utf8' \
 -mconf '{"table_name":"my_table_name", "field_name":"my_id_field_name", "conditions":["`active` = 1", "`user_id` = 100"]}' \
 -sdriver 'elasticsearch' \
 -sconn 'http://localhost:9200' \
 -sconf '{"index":"my_index_name", "type":"my_type_name", "field":"my_id_field_path"}'
 ```
