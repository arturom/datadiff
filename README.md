# datadiff
[![Go Report Card](https://goreportcard.com/badge/github.com/arturom/datadiff)](https://goreportcard.com/report/github.com/arturom/datadiff)

Datadiff is a library and CLI tool to find differences between two data sources. This is useful when there is a primary data source and a secondary data source and they both need to contain the same records.

This tool considers two data sources to be qual if they contain the same numeric IDs. This approach does not compare any other field value.


### Strategy
Rather than comparing record by record, this library compares the [histograms](https://en.wikipedia.org/wiki/Histogram) of the numeric IDs from both sources. These are the steps taken:

 - Create a histogram of the numeric IDs from the primary data source.
 - Create a histogram of the numeric IDs from the secondary data source.
 - Merge and compare the histograms.
 - If the bin capacities are full, mark this range as resolved.
 - Fetch the histogram of the unresolved bins with smaller bin sizes.
 - Merge and compare the histograms.
 - Fetch the ids of the unresolved bins.
 - Compare the numeric IDs of unresolved bins and output the results.

### Supported Data Sources
  - mysql
  - elasticsearch

### Usage
Run `datadiff -h` to get usage information
```bash
$ ./datadiff -h
```
```
Usage of ./datadiff:
  -interval int
        Initial histogram interval (default 1000)
  -mconf string
        Primary configuration string (default "{}")
  -mconn string
        Primary connection string
  -mdriver string
        Primary driver [elasticsearch|mysql]
  -sconf string
        Secondary configuration string (default "{}")
  -sconn string
        Secondary connection string
  -sdriver string
        Secondary driver [elasticsearch|mysql]
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


```
mysql://root:root@localhost:3306/dbname?table=tablename&field=id
es://http://localhost:9200?index=indexname&field=id
```
