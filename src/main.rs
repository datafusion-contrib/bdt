// Copyright 2022 Andy Grove
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://wwwApache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use datafusion::common::{DataFusionError, Result};
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
    View {
        #[structopt(parse(from_os_str))]
        filename: PathBuf,
        #[structopt(short, long)]
        limit: Option<usize>,
    },
    Schema {
        #[structopt(parse(from_os_str))]
        filename: PathBuf,
    },
    Convert {
        #[structopt(parse(from_os_str))]
        input: PathBuf,
        #[structopt(parse(from_os_str))]
        output: PathBuf,
    },
    Query {
        #[structopt(parse(from_os_str), long)]
        table: Vec<PathBuf>,
        #[structopt(long)]
        sql: String,
        #[structopt(short, long)]
        verbose: bool,
    },
    ViewParquetMeta {
        #[structopt(parse(from_os_str))]
        input: PathBuf,
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
        Command::ViewParquetMeta { input } => {
            view_parquet_meta(input)?;
        }
    }
    Ok(())
}

fn view_parquet_meta(path: PathBuf) -> Result<()> {
    let file = File::open(&path)?;
    let reader = SerializedFileReader::new(file)?;

    let parquet_metadata = reader.metadata();
    println!("File metadata");
    println!("=============");
    println!("Row Groups: {}", parquet_metadata.num_row_groups());

    for i in 0..parquet_metadata.num_row_groups() {
        let row_group_reader = reader.get_row_group(i)?;
        println!("\nRow Group {} metadata", i);
        println!("=====================");
        let md = row_group_reader.metadata();
        println!("Rows: {}", md.num_rows());
        println!("Bytes: {}", md.total_byte_size());

        for column in md.columns() {
            println!("\nColumn {} metadata", column.column_descr().name());
            println!("=====================");
            if let Some(stats) = column.statistics() {
                println!("Physical Type: {:?}", stats.physical_type());
                println!("Null Count: {}", stats.null_count());
                if let Some(dc) = stats.distinct_count() {
                    println!("Distinct Count: {}", dc);
                }
                if stats.has_min_max_set() {
                    match &stats {
                        Statistics::Boolean(v) => {
                            if stats.distinct_count().is_none() {
                                let dc = if v.min() == v.max() { 1 } else { 2 };
                                println!("Distinct Count: {}", dc);
                            }
                            println!("Min: {}", v.min());
                            println!("Max: {}", v.max());
                        }
                        Statistics::Int32(v) => {
                            if stats.distinct_count().is_none() && md.num_rows() > 0 {
                                let range = v.max() - v.min();
                                let dc = range.min(md.num_rows() as i32);
                                println!("Distinct Count (Estimated): {}", dc);
                            }
                            println!("Min: {}", v.min());
                            println!("Max: {}", v.max());
                        }
                        Statistics::Int64(v) => {
                            if stats.distinct_count().is_none() && md.num_rows() > 0 {
                                let range = v.max() - v.min();
                                let dc = range.min(md.num_rows());
                                println!("Distinct Count (Estimated): {}", dc);
                            }
                            println!("Min: {}", v.min());
                            println!("Max: {}", v.max());
                        }
                        Statistics::Float(v) => {
                            if stats.distinct_count().is_none() {
                                println!("Distinct Count: N/A");
                            }
                            println!("Min: {}", v.min());
                            println!("Max: {}", v.max());
                        }
                        Statistics::Double(v) => {
                            if stats.distinct_count().is_none() {
                                println!("Distinct Count: N/A");
                            }
                            println!("Min: {}", v.min());
                            println!("Max: {}", v.max());
                        }
                        _ => {
                            println!("Distinct Count: N/A");
                            println!("Min: UNSUPPORTED TYPE");
                            println!("Max: UNSUPPORTED TYPE");
                        }
                    }
                } else {
                    println!("Distinct Count: N/A");
                    println!("Min: N/A");
                    println!("Max: N/A");
                }
            } else {
                println!("No statistics available");
            }
        }
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
