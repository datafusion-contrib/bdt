# Boring Data Tool (dbt) 🤓

Simple tools for viewing files and converting between different file formats (csv, avro, json, parquet, etc).

## Prerequisites

- Install Rustup

## Install

``` bash
cargo install bdt
```

## Example

```sql
$ bdt /path/to/file.parquet
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