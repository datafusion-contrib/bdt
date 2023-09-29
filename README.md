# Boring Data Tool (bdt) 🤓

Command-line tool for viewing, querying, converting, and comparing files in popular data formats (CSV, Parquet, JSON,
and Avro).

Powered by [Apache Arrow](https://arrow.apache.org/) and [DataFusion](https://crates.io/crates/datafusion).

## Features

- View file schemas
- View contents of files
- Run SQL queries against files
- Convert between file formats
- Compare contents of two files, allowing an epsilon to be provided for floating point comparisons
- View Parquet file metadata (statistics)
- Supports CSV, JSON, Parquet, and Avro file formats

## Installation

### Mac

```shell
brew tap andygrove/bdt
brew install bdt
```

### Other Platforms

Rust must be installed first. Follow instructions at [https://rustup.rs/](https://rustup.rs/).

```bash
cargo install bdt
```

## Usage

```bash
Boring Data Tool

USAGE:
    bdt <SUBCOMMAND>

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

SUBCOMMANDS:
    compare              Compare the contents of two files
    convert              Convert a file to a different format
    count                Show the row count of the file
    help                 Prints this message or the help of the given subcommand(s)
    query                Run a SQL query against one or more files
    schema               View schema of a file
    view                 View contents of a file
    view-parquet-meta    View Parquet metadata
```

## Examples

### View File Schema

```bash
bdt schema /mnt/bigdata/nyctaxi/yellow_tripdata_2022-01.parquet
+-----------------------+-----------------------------+-------------+
| column_name           | data_type                   | is_nullable |
+-----------------------+-----------------------------+-------------+
| VendorID              | Int64                       | YES         |
| tpep_pickup_datetime  | Timestamp(Nanosecond, None) | YES         |
| tpep_dropoff_datetime | Timestamp(Nanosecond, None) | YES         |
| passenger_count       | Float64                     | YES         |
| trip_distance         | Float64                     | YES         |
| RatecodeID            | Float64                     | YES         |
| store_and_fwd_flag    | Utf8                        | YES         |
| PULocationID          | Int64                       | YES         |
| DOLocationID          | Int64                       | YES         |
| payment_type          | Int64                       | YES         |
| fare_amount           | Float64                     | YES         |
| extra                 | Float64                     | YES         |
| mta_tax               | Float64                     | YES         |
| tip_amount            | Float64                     | YES         |
| tolls_amount          | Float64                     | YES         |
| improvement_surcharge | Float64                     | YES         |
| total_amount          | Float64                     | YES         |
| congestion_surcharge  | Float64                     | YES         |
| airport_fee           | Float64                     | YES         |
+-----------------------+-----------------------------+-------------+
```

### View File Contents

```bash
$ bdt view /path/to/file.parquet --limit 10
+-----------+------------------+--------+--------+----------+----------+---------+---------+-------------+-------------+
| t_time_sk | t_time_id        | t_time | t_hour | t_minute | t_second | t_am_pm | t_shift | t_sub_shift | t_meal_time |
+-----------+------------------+--------+--------+----------+----------+---------+---------+-------------+-------------+
| 0         | AAAAAAAABAAAAAAA | 0      | 0      | 0        | 0        | AM      | third   | night       |             |
| 1         | AAAAAAAACAAAAAAA | 1      | 0      | 0        | 1        | AM      | third   | night       |             |
| 2         | AAAAAAAADAAAAAAA | 2      | 0      | 0        | 2        | AM      | third   | night       |             |
| 3         | AAAAAAAAEAAAAAAA | 3      | 0      | 0        | 3        | AM      | third   | night       |             |
| 4         | AAAAAAAAFAAAAAAA | 4      | 0      | 0        | 4        | AM      | third   | night       |             |
| 5         | AAAAAAAAGAAAAAAA | 5      | 0      | 0        | 5        | AM      | third   | night       |             |
| 6         | AAAAAAAAHAAAAAAA | 6      | 0      | 0        | 6        | AM      | third   | night       |             |
| 7         | AAAAAAAAIAAAAAAA | 7      | 0      | 0        | 7        | AM      | third   | night       |             |
| 8         | AAAAAAAAJAAAAAAA | 8      | 0      | 0        | 8        | AM      | third   | night       |             |
| 9         | AAAAAAAAKAAAAAAA | 9      | 0      | 0        | 9        | AM      | third   | night       |             |
+-----------+------------------+--------+--------+----------+----------+---------+---------+-------------+-------------+
```

### Run SQL Query

Queries can be run against one or more tables. Table names are inferred from file names.

```bash
$ bdt query --table /mnt/bigdata/nyctaxi/yellow_tripdata_2022-01.parquet \
  --sql "SELECT COUNT(*) FROM yellow_tripdata_2022_01"
Registering table 'yellow_tripdata_2022_01' for /mnt/bigdata/nyctaxi/yellow_tripdata_2022-01.parquet
+-----------------+
| COUNT(UInt8(1)) |
+-----------------+
| 2463931         |
+-----------------+
```

Use the `--tables` option to register all files/directories in one directory as tables, and use the `--sql-file` option 
to load a query from disk.

```bash
$ bdt query --tables /mnt/bigdata/tpch/sf10-parquet/ --sql-file /home/andy/git/sql-benchmarks/sqlbench-h/queries/sf=10/q1.sql`
Registering table 'supplier' for /mnt/bigdata/tpch/sf10-parquet/supplier.parquet
Registering table 'part' for /mnt/bigdata/tpch/sf10-parquet/part.parquet
Registering table 'partsupp' for /mnt/bigdata/tpch/sf10-parquet/partsupp.parquet
Registering table 'nation' for /mnt/bigdata/tpch/sf10-parquet/nation.parquet
Registering table 'region' for /mnt/bigdata/tpch/sf10-parquet/region.parquet
Registering table 'orders' for /mnt/bigdata/tpch/sf10-parquet/orders.parquet
Registering table 'lineitem' for /mnt/bigdata/tpch/sf10-parquet/lineitem.parquet
Registering table 'customer' for /mnt/bigdata/tpch/sf10-parquet/customer.parquet
+--------------+--------------+--------------+------------------+--------------------+----------------------+-----------+--------------+----------+-------------+
| l_returnflag | l_linestatus | sum_qty      | sum_base_price   | sum_disc_price     | sum_charge           | avg_qty   | avg_price    | avg_disc | count_order |
+--------------+--------------+--------------+------------------+--------------------+----------------------+-----------+--------------+----------+-------------+
| A            | F            | 377518277.00 | 566065563002.85  | 537758943278.1740  | 559276505545.688411  | 25.500977 | 38237.155374 | 0.050006 | 14804071    |
| N            | F            | 9851614.00   | 14767438399.17   | 14028805792.2114   | 14590490998.366737   | 25.522448 | 38257.810660 | 0.049973 | 385998      |
| N            | O            | 730783087.00 | 1095795289143.27 | 1041001162690.9297 | 1082653834336.561576 | 25.497622 | 38233.198852 | 0.049999 | 28660832    |
| R            | F            | 377732634.00 | 566430710070.73  | 538110604499.8196  | 559634448619.890015  | 25.508381 | 38251.211480 | 0.049996 | 14808177    |
+--------------+--------------+--------------+------------------+--------------------+----------------------+-----------+--------------+----------+-------------+
```

Query results can also be written to disk by specifying an `--output` path.

```bash
$ bdt query --table /mnt/bigdata/nyctaxi/yellow_tripdata_2022-01.parquet \
  --sql "SELECT COUNT(*) FROM yellow_tripdata_2022_01" \
  --output results.csv
Registering table 'yellow_tripdata_2022_01' for /mnt/bigdata/nyctaxi/yellow_tripdata_2022-01.parquet
Writing results in CSV format to results.csv
```

### Convert Parquet to newline-delimited JSON

```bash
$ bdt convert /path/to/input.parquet /path/to/output.json
$ cat /path/to/output.json
{"d_date_sk":2415022,"d_date_id":"AAAAAAAAOKJNECAA","d_date":"1900-01-02","d_month_seq":0,"d_week_seq":1,"d_quarter_seq":1,"d_year":1900,"d_dow":1,"d_moy":1,"d_dom":2,"d_qoy":1,"d_fy_year":1900,"d_fy_quarter_seq":1,"d_fy_week_seq":1,"d_day_name":"Monday","d_quarter_name":"1900Q1","d_holiday":"N","d_weekend":"N","d_following_holiday":"Y","d_first_dom":2415021,"d_last_dom":2415020,"d_same_day_ly":2414657,"d_same_day_lq":2414930,"d_current_day":"N","d_current_week":"N","d_current_month":"N","d_current_quarter":"N","d_current_year":"N"}
{"d_date_sk":2415023,"d_date_id":"AAAAAAAAPKJNECAA","d_date":"1900-01-03","d_month_seq":0,"d_week_seq":1,"d_quarter_seq":1,"d_year":1900,"d_dow":2,"d_moy":1,"d_dom":3,"d_qoy":1,"d_fy_year":1900,"d_fy_quarter_seq":1,"d_fy_week_seq":1,"d_day_name":"Tuesday","d_quarter_name":"1900Q1","d_holiday":"N","d_weekend":"N","d_following_holiday":"N","d_first_dom":2415021,"d_last_dom":2415020,"d_same_day_ly":2414658,"d_same_day_lq":2414931,"d_current_day":"N","d_current_week":"N","d_current_month":"N","d_current_quarter":"N","d_current_year":"N"}
```

### View Parquet File Metadata

```bash
$ bdt view-parquet-meta /mnt/bigdata/tpcds/sf100-parquet/store_sales.parquet/part-00000-cff04137-32a6-4e5b-811a-668f5d4b1802-c000.snappy.parquet

+------------+----------------------------------------------------------------------------+
| Key        | Value                                                                      |
+------------+----------------------------------------------------------------------------+
| Version    | 1                                                                          |
| Created By | parquet-mr version 1.10.1 (build a89df8f9932b6ef6633d06069e50c9b7970bebd1) |
| Rows       | 40016                                                                      |
| Row Groups | 1                                                                          |
+------------+----------------------------------------------------------------------------+

Row Group 0 of 1 contains 40016 rows and has 190952 bytes:

+-----------------------+--------------+---------------+-----------------+-------+-----------------------------------------------------+------------------------------------+
| Column Name           | Logical Type | Physical Type | Distinct Values | Nulls | Min                                                 | Max                                |
+-----------------------+--------------+---------------+-----------------+-------+-----------------------------------------------------+------------------------------------+
| cd_demo_sk            | N/A          | INT32         | N/A             | 0     | 1520641                                             | 1560656                            |
| cd_gender             | N/A          | BYTE_ARRAY    | N/A             | 0     | [70]                                                | [77]                               |
| cd_marital_status     | N/A          | BYTE_ARRAY    | N/A             | 0     | [68]                                                | [87]                               |
| cd_education_status   | N/A          | BYTE_ARRAY    | N/A             | 0     | [50, 32, 121, 114, 32, 68, 101, 103, 114, 101, 101] | [85, 110, 107, 110, 111, 119, 110] |
| cd_purchase_estimate  | N/A          | INT32         | N/A             | 0     | 500                                                 | 10000                              |
| cd_credit_rating      | N/A          | BYTE_ARRAY    | N/A             | 0     | [71, 111, 111, 100]                                 | [85, 110, 107, 110, 111, 119, 110] |
| cd_dep_count          | N/A          | INT32         | N/A             | 0     | 0                                                   | 6                                  |
| cd_dep_employed_count | N/A          | INT32         | N/A             | 0     | 3                                                   | 4                                  |
| cd_dep_college_count  | N/A          | INT32         | N/A             | 0     | 5                                                   | 5                                  |
+-----------------------+--------------+---------------+-----------------+-------+-----------------------------------------------------+------------------------------------+
```
