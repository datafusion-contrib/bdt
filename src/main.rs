// Copyright 2022 Andy Grove
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use comfy_table::{Cell, Table};
use datafusion::arrow::array;
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::parquet::basic::LogicalType;
use datafusion::parquet::file::reader::{FileReader, SerializedFileReader};
use datafusion::parquet::file::statistics::Statistics;
use datafusion::prelude::*;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "bdt", about = "Boring Data Tool")]
enum Command {
    /// View contents of a file
    View {
        #[structopt(parse(from_os_str))]
        filename: PathBuf,
        #[structopt(short, long)]
        limit: Option<usize>,
    },
    /// View schema of a file
    Schema {
        #[structopt(parse(from_os_str))]
        filename: PathBuf,
    },
    /// Convert a file to a different format
    Convert {
        #[structopt(parse(from_os_str))]
        input: PathBuf,
        #[structopt(parse(from_os_str))]
        output: PathBuf,
    },
    /// Show the row count of the file
    Count {
        #[structopt(parse(from_os_str), long)]
        table: PathBuf,
    },
    /// Run a SQL query against one or more files
    Query {
        #[structopt(parse(from_os_str), long)]
        table: Vec<PathBuf>,
        #[structopt(long)]
        sql: String,
        #[structopt(short, long)]
        verbose: bool,
    },
    /// View Parquet metadata
    ViewParquetMeta {
        #[structopt(parse(from_os_str))]
        input: PathBuf,
    },
    /// Compare the contents of two files
    Compare {
        #[structopt(parse(from_os_str))]
        input1: PathBuf,
        #[structopt(parse(from_os_str))]
        input2: PathBuf,
        #[structopt(short, long)]
        epsilon: Option<f64>,
    },
}

enum FileFormat {
    Avro,
    Csv,
    Json,
    Parquet,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cmd = Command::from_args();
    let config = SessionConfig::new().with_information_schema(true);
    let ctx = SessionContext::with_config(config);
    match cmd {
        Command::View { filename, limit } => {
            let filename = parse_filename(&filename)?;
            let df = register_table(&ctx, "t", filename).await?;
            let limit = limit.unwrap_or(10);
            if limit > 0 {
                df.show_limit(limit).await?;
                println!(
                    "Limiting to {} rows. Run with --limit 0 to remove limit.",
                    limit
                );
            } else {
                df.show().await?;
            }
        }
        Command::Schema { filename } => {
            let filename = parse_filename(&filename)?;
            let _ = register_table(&ctx, "t", filename).await?;
            let sql = "SELECT column_name, data_type, is_nullable \
                                FROM information_schema.columns WHERE table_name = 't'";
            let df = ctx.sql(sql).await?;
            df.show().await?;
        }
        Command::Convert { input, output } => {
            let input_filename = parse_filename(&input)?;
            let output_filename = parse_filename(&output)?;
            let df = register_table(&ctx, "t", input_filename).await?;
            match file_format(output_filename)? {
                FileFormat::Avro => unimplemented!(),
                FileFormat::Csv => df.write_csv(output_filename).await?,
                FileFormat::Json => df.write_json(output_filename).await?,
                FileFormat::Parquet => df.write_parquet(output_filename, None).await?,
            }
        }
        Command::Query {
            sql,
            table,
            verbose,
        } => {
            for table in &table {
                let file_name = table
                    .file_stem()
                    .unwrap()
                    .to_str()
                    .ok_or_else(|| DataFusionError::Internal("Invalid filename".to_string()))?;
                let table_name = sanitize_table_name(file_name);
                if verbose {
                    println!("Registering table '{}' for {}", table_name, table.display());
                }
                register_table(&ctx, &table_name, parse_filename(table)?).await?;
            }
            let df = ctx.sql(&sql).await?;
            if verbose {
                let explain = df.explain(false, false)?;
                explain.show().await?;
            }
            df.show().await?;
        }
        Command::Count { table } => {
            let table_name = "__t1__";
            register_table(&ctx, &table_name, parse_filename(&table)?).await?;
            let sql = format!("SELECT COUNT(*) FROM {}", table_name);
            let df = ctx.sql(&sql).await?;
            df.show().await?;
        }
        Command::ViewParquetMeta { input } => {
            view_parquet_meta(input)?;
        }
        Command::Compare {
            input1,
            input2,
            epsilon,
        } => {
            compare_files(input1, input2, epsilon).await?;
        }
    }
    Ok(())
}

fn view_parquet_meta(path: PathBuf) -> Result<()> {
    let file = File::open(&path)?;
    let reader = SerializedFileReader::new(file)?;

    let parquet_metadata = reader.metadata();

    let mut table = Table::new();
    table.load_preset("||--+-++|    ++++++");
    table.set_header(vec![Cell::new("Key"), Cell::new("Value")]);
    let file_meta = parquet_metadata.file_metadata();
    table.add_row(vec![
        Cell::new("Version"),
        Cell::new(format!("{}", file_meta.version())),
    ]);
    table.add_row(vec![
        Cell::new("Created By"),
        Cell::new(file_meta.created_by().unwrap_or("N/A")),
    ]);
    table.add_row(vec![
        Cell::new("Rows"),
        Cell::new(format!("{}", file_meta.num_rows())),
    ]);
    table.add_row(vec![
        Cell::new("Row Groups"),
        Cell::new(format!("{}", parquet_metadata.num_row_groups())),
    ]);
    println!("{}", table);

    for i in 0..parquet_metadata.num_row_groups() {
        let row_group_reader = reader.get_row_group(i)?;
        let md = row_group_reader.metadata();
        println!(
            "\nRow Group {} of {} contains {} rows and has {} bytes:\n",
            i,
            parquet_metadata.num_row_groups(),
            md.num_rows(),
            md.total_byte_size()
        );

        let mut table = Table::new();
        table.load_preset("||--+-++|    ++++++");
        let header: Vec<Cell> = vec![
            "Column Name",
            "Logical Type",
            "Physical Type",
            "Distinct Values",
            "Nulls",
            "Min",
            "Max",
        ]
        .iter()
        .map(|str| Cell::new(str))
        .collect();
        table.set_header(header);

        let not_available = "N/A".to_string();
        for column in md.columns() {
            let mut row: Vec<String> = vec![];
            row.push(column.column_descr().name().to_owned());
            if let Some(t) = column.column_descr().logical_type() {
                row.push(format!("{:?}", t));
            } else {
                row.push(not_available.clone());
            }
            match column.statistics() {
                Some(stats) => {
                    row.push(format!("{}", stats.physical_type()));
                    if let Some(dc) = stats.distinct_count() {
                        row.push(format!("{}", dc));
                    } else {
                        row.push(not_available.clone());
                    }
                    row.push(format!("{}", stats.null_count()));

                    if stats.has_min_max_set() {
                        match &stats {
                            Statistics::Boolean(v) => {
                                row.push(format!("{}", v.min()));
                                row.push(format!("{}", v.max()));
                            }
                            Statistics::Int32(v) => {
                                row.push(format!("{}", v.min()));
                                row.push(format!("{}", v.max()));
                            }
                            Statistics::Int64(v) => {
                                row.push(format!("{}", v.min()));
                                row.push(format!("{}", v.max()));
                            }
                            Statistics::Float(v) => {
                                row.push(format!("{}", v.min()));
                                row.push(format!("{}", v.max()));
                            }
                            Statistics::Double(v) => {
                                row.push(format!("{}", v.min()));
                                row.push(format!("{}", v.max()));
                            }
                            Statistics::ByteArray(v) => {
                                match column.column_descr().logical_type() {
                                    Some(LogicalType::String) => {
                                        let min = v.min().as_utf8().unwrap();
                                        let max = v.min().as_utf8().unwrap();
                                        row.push(format!("{}", min));
                                        row.push(format!("{}", max));
                                    }
                                    _ => {
                                        row.push(format!("{}", v.min()));
                                        row.push(format!("{}", v.max()));
                                    }
                                }
                            }
                            _ => {
                                row.push("unsupported".to_owned());
                                row.push("unsupported".to_owned());
                            }
                        }
                    } else {
                        row.push(not_available.clone());
                        row.push(not_available.clone());
                    }
                }
                _ => {
                    for _ in 0..5 {
                        row.push(not_available.clone());
                    }
                }
            }
            table.add_row(row);
        }

        println!("{}", table);
    }
    Ok(())
}

fn sanitize_table_name(name: &str) -> String {
    let mut str = String::new();
    for ch in name.chars() {
        if ch.is_ascii_alphanumeric() || ch == '_' {
            str.push(ch);
        } else {
            str.push('_')
        }
    }
    str
}

fn parse_filename(filename: &Path) -> Result<&str> {
    filename
        .to_str()
        .ok_or_else(|| DataFusionError::Internal("Invalid filename".to_string()))
}

async fn register_table(
    ctx: &SessionContext,
    table_name: &str,
    filename: &str,
) -> Result<Arc<DataFrame>> {
    match file_format(filename)? {
        FileFormat::Avro => {
            ctx.register_avro(table_name, filename, AvroReadOptions::default())
                .await?
        }
        FileFormat::Csv => {
            ctx.register_csv(table_name, filename, CsvReadOptions::default())
                .await?
        }
        FileFormat::Json => {
            ctx.register_json(table_name, filename, NdJsonReadOptions::default())
                .await?
        }
        FileFormat::Parquet => {
            ctx.register_parquet(table_name, filename, ParquetReadOptions::default())
                .await?
        }
    }
    ctx.table(table_name)
}

fn file_format(filename: &str) -> Result<FileFormat> {
    match filename.rfind('.') {
        Some(i) => match &filename[i + 1..] {
            "avro" => Ok(FileFormat::Avro),
            "csv" => Ok(FileFormat::Csv),
            "json" => Ok(FileFormat::Json),
            "parquet" => Ok(FileFormat::Parquet),
            other => Err(DataFusionError::Internal(format!(
                "unsupported file extension '{}'",
                other
            ))),
        },
        _ => Err(DataFusionError::Internal(format!(
            "Could not determine file extension for '{}'",
            filename
        ))),
    }
}

async fn compare_files(path1: PathBuf, path2: PathBuf, epsilon: Option<f64>) -> Result<bool> {
    let ctx = SessionContext::new();
    // TODO assumes csv for now
    let df = ctx
        .read_csv(path1.to_str().unwrap(), CsvReadOptions::default())
        .await?;
    // TODO reads results into memory .. could stream this instead
    let batches1 = df.collect().await?;

    let df = ctx
        .read_csv(path2.to_str().unwrap(), CsvReadOptions::default())
        .await?;
    let batches2 = df.collect().await?;

    let count1: usize = batches1.iter().map(|b| b.num_rows()).sum();
    let count2: usize = batches2.iter().map(|b| b.num_rows()).sum();
    if count1 == count2 {
        let it1 = RowIter::new(batches1);
        let it2 = RowIter::new(batches2);
        for (i, (a, b)) in it1.zip(it2).enumerate() {
            if a.len() == b.len() {
                for (j, (v1, v2)) in a.iter().zip(b.iter()).enumerate() {
                    if v1 != v2 {
                        let ok = if let Some(epsilon) = epsilon {
                            match (v1, v2) {
                                (
                                    ScalarValue::Float32(Some(ll)),
                                    ScalarValue::Float32(Some(rr)),
                                ) => ((ll - rr) as f64) < epsilon,
                                (
                                    ScalarValue::Float64(Some(ll)),
                                    ScalarValue::Float64(Some(rr)),
                                ) => ll - rr < epsilon,
                                _ => false,
                            }
                        } else {
                            false
                        };
                        if !ok {
                            println!(
                                "data does not match at row {} column {}: {:?} != {:?}",
                                i, j, v1, v2
                            );
                            return Ok(false);
                        }
                    }
                }
            } else {
                println!(
                    "row lengths do not match at index {}: {} != {}",
                    i,
                    a.len(),
                    b.len()
                );
                return Ok(false);
            }
        }
    } else {
        println!("row counts do not match: {} != {}", count1, count2);
        return Ok(false);
    }
    Ok(true)
}

struct RowIter {
    batches: Vec<RecordBatch>,
    current_batch: usize,
    current_batch_offset: usize,
}

impl RowIter {
    fn new(batches: Vec<RecordBatch>) -> Self {
        Self {
            batches,
            current_batch: 0,
            current_batch_offset: 0,
        }
    }
}

impl Iterator for RowIter {
    type Item = Vec<ScalarValue>;

    fn next(&mut self) -> Option<Self::Item> {
        while self.current_batch < self.batches.len() {
            let b = &self.batches[self.current_batch];
            if self.current_batch_offset < b.num_rows() {
                let mut row: Vec<ScalarValue> = Vec::with_capacity(b.num_columns());
                let row_index = self.current_batch_offset;
                self.current_batch_offset += 1;
                for col_index in 0..b.num_columns() {
                    let array = b.column(col_index);
                    if array.is_null(row_index) {
                        row.push(ScalarValue::Null)
                    } else {
                        match array.data_type() {
                            DataType::Utf8 => {
                                let array =
                                    array.as_any().downcast_ref::<array::StringArray>().unwrap();
                                row.push(ScalarValue::Utf8(Some(
                                    array.value(row_index).to_string(),
                                )));
                            }
                            // TODO introduce macros to make this concise
                            DataType::Int8 => {
                                let array =
                                    array.as_any().downcast_ref::<array::Int8Array>().unwrap();
                                row.push(ScalarValue::Int8(Some(array.value(row_index))));
                            }
                            DataType::Int16 => {
                                let array =
                                    array.as_any().downcast_ref::<array::Int16Array>().unwrap();
                                row.push(ScalarValue::Int16(Some(array.value(row_index))));
                            }
                            DataType::Int32 => {
                                let array =
                                    array.as_any().downcast_ref::<array::Int32Array>().unwrap();
                                row.push(ScalarValue::Int32(Some(array.value(row_index))));
                            }
                            DataType::Int64 => {
                                let array =
                                    array.as_any().downcast_ref::<array::Int64Array>().unwrap();
                                row.push(ScalarValue::Int64(Some(array.value(row_index))));
                            }
                            DataType::UInt8 => {
                                let array =
                                    array.as_any().downcast_ref::<array::UInt8Array>().unwrap();
                                row.push(ScalarValue::UInt8(Some(array.value(row_index))));
                            }
                            DataType::UInt16 => {
                                let array =
                                    array.as_any().downcast_ref::<array::UInt16Array>().unwrap();
                                row.push(ScalarValue::UInt16(Some(array.value(row_index))));
                            }
                            DataType::UInt32 => {
                                let array =
                                    array.as_any().downcast_ref::<array::UInt32Array>().unwrap();
                                row.push(ScalarValue::UInt32(Some(array.value(row_index))));
                            }
                            DataType::UInt64 => {
                                let array =
                                    array.as_any().downcast_ref::<array::UInt64Array>().unwrap();
                                row.push(ScalarValue::UInt64(Some(array.value(row_index))));
                            }
                            DataType::Float32 => {
                                let array = array
                                    .as_any()
                                    .downcast_ref::<array::Float32Array>()
                                    .unwrap();
                                row.push(ScalarValue::Float32(Some(array.value(row_index))));
                            }
                            DataType::Float64 => {
                                let array = array
                                    .as_any()
                                    .downcast_ref::<array::Float64Array>()
                                    .unwrap();
                                row.push(ScalarValue::Float64(Some(array.value(row_index))));
                            }
                            DataType::Date32 => {
                                let array =
                                    array.as_any().downcast_ref::<array::Date32Array>().unwrap();
                                row.push(ScalarValue::Date32(Some(array.value(row_index))));
                            }
                            DataType::Date64 => {
                                let array =
                                    array.as_any().downcast_ref::<array::Date64Array>().unwrap();
                                row.push(ScalarValue::Date64(Some(array.value(row_index))));
                            }
                            other => {
                                println!("unsupported type: {}", other);
                                todo!("unsupported data type")
                            }
                        }
                    }
                }
                return Some(row);
            } else {
                // move onto next batch
                self.current_batch += 1;
                self.current_batch_offset = 0;
            }
        }
        None
    }
}
