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

use bdt::compare::ComparisonResult;
use bdt::utils::{file_format, parse_filename};
use bdt::{compare, Error, FileFormat};
use comfy_table::{Cell, Table};
use datafusion::common::DataFusionError;
use datafusion::parquet::basic::LogicalType;
use datafusion::parquet::file::reader::{FileReader, SerializedFileReader};
use datafusion::parquet::file::statistics::Statistics;
use datafusion::prelude::*;
use std::fs::File;
use std::path::PathBuf;
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
        /// List of tables to register
        #[structopt(parse(from_os_str), long)]
        table: Vec<PathBuf>,
        /// SQL Query to execute
        #[structopt(long)]
        sql: String,
        /// Optional output filename to store results. If no path is provided then results
        /// will be written to stdout
        #[structopt(parse(from_os_str), long)]
        output: Option<PathBuf>,
        /// Enable verbose logging
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
        /// Assume there is a header row by default (only applies to CSV)
        #[structopt(short, long)]
        no_header_row: bool,
    },
}

#[tokio::main]
async fn main() -> Result<(), Error> {
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
            table,
            sql,
            output,
            verbose,
        } => {
            for table in &table {
                let file_name = table
                    .file_stem()
                    .unwrap()
                    .to_str()
                    .ok_or_else(|| DataFusionError::Internal("Invalid filename".to_string()))?;
                let table_name = sanitize_table_name(file_name);
                println!("Registering table '{}' for {}", table_name, table.display());
                register_table(&ctx, &table_name, parse_filename(table)?).await?;
            }
            let df = ctx.sql(&sql).await?;
            if verbose {
                let explain = df.explain(false, false)?;
                explain.show().await?;
            }
            if let Some(path) = output {
                match path.extension() {
                    Some(x) => match x.to_str().unwrap() {
                        "csv" => {
                            println!("Writing results in CSV format to {}", path.display());
                            df.write_csv(path.to_str().unwrap()).await?
                        }
                        "parquet" => {
                            println!("Writing results in Parquet format to {}", path.display());
                            df.write_parquet(path.to_str().unwrap(), None).await?
                        }
                        _ => {
                            println!("Unsupported file format for saving query results");
                            std::process::exit(-1);
                        }
                    },
                    _ => {
                        println!("Unsupported file format for saving query results");
                        std::process::exit(-1);
                    }
                }
            } else {
                df.show().await?;
            }
        }
        Command::Count { table } => {
            let table_name = "__t1__";
            register_table(&ctx, table_name, parse_filename(&table)?).await?;
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
            no_header_row,
        } => match compare::compare_files(input1, input2, !no_header_row, epsilon).await? {
            ComparisonResult::Ok => {
                println!("Files match");
            }
            diff => {
                println!("{}", diff);
                std::process::exit(-1);
            }
        },
    }
    Ok(())
}

fn view_parquet_meta(path: PathBuf) -> Result<(), Error> {
    let file = File::open(&path).map_err(Error::from)?;
    let reader = SerializedFileReader::new(file).map_err(Error::from)?;

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
        .map(Cell::new)
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
                                        row.push(min.to_string());
                                        row.push(max.to_string());
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

async fn register_table(
    ctx: &SessionContext,
    table_name: &str,
    filename: &str,
) -> Result<Arc<DataFrame>, Error> {
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
    ctx.table(table_name).map_err(Error::from)
}
