# Boring Data Tool (bdt) 🤓

Simple tools for viewing files and converting between different file formats (csv, avro, json, parquet, etc). 

Powered by [DataFusion](https://crates.io/crates/datafusion).

## Prerequisites

- [Install Rust](https://rustup.rs/)

## Installation

``` bash
cargo install bdt
```

## Examples

### View Parquet Schema

```
bdt schema /mnt/bigdata/nyctaxi/yellow_tripdata_2022-01.parquet`
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

### View Parquet File

```
$ bdt view /path/to/file.parquet
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

### Convert Parquet to newline-delimited JSON

```
$ bdt convert /path/to/input.parquet /path/to/output.json
$ cat /path/to/output.json
{"d_date_sk":2415022,"d_date_id":"AAAAAAAAOKJNECAA","d_date":"1900-01-02","d_month_seq":0,"d_week_seq":1,"d_quarter_seq":1,"d_year":1900,"d_dow":1,"d_moy":1,"d_dom":2,"d_qoy":1,"d_fy_year":1900,"d_fy_quarter_seq":1,"d_fy_week_seq":1,"d_day_name":"Monday","d_quarter_name":"1900Q1","d_holiday":"N","d_weekend":"N","d_following_holiday":"Y","d_first_dom":2415021,"d_last_dom":2415020,"d_same_day_ly":2414657,"d_same_day_lq":2414930,"d_current_day":"N","d_current_week":"N","d_current_month":"N","d_current_quarter":"N","d_current_year":"N"}
{"d_date_sk":2415023,"d_date_id":"AAAAAAAAPKJNECAA","d_date":"1900-01-03","d_month_seq":0,"d_week_seq":1,"d_quarter_seq":1,"d_year":1900,"d_dow":2,"d_moy":1,"d_dom":3,"d_qoy":1,"d_fy_year":1900,"d_fy_quarter_seq":1,"d_fy_week_seq":1,"d_day_name":"Tuesday","d_quarter_name":"1900Q1","d_holiday":"N","d_weekend":"N","d_following_holiday":"N","d_first_dom":2415021,"d_last_dom":2415020,"d_same_day_ly":2414658,"d_same_day_lq":2414931,"d_current_day":"N","d_current_week":"N","d_current_month":"N","d_current_quarter":"N","d_current_year":"N"}
...
```
