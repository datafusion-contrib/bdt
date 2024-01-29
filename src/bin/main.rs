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
use bdt::convert::convert_files;
use bdt::parquet::view_parquet_meta;
use bdt::utils::{parse_filename, register_table, sanitize_table_name};
use bdt::{compare, Error};
use datafusion::common::DataFusionError;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::prelude::*;
use std::fs;
use std::path::PathBuf;
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
        #[structopt(short, long)]
        one_file: bool,
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
        /// Directory containing tables to register
        #[structopt(parse(from_os_str), long)]
        tables: Option<PathBuf>,
        /// SQL query to execute
        #[structopt(long)]
        sql: Option<String>,
        /// File containing SQL query to execute
        #[structopt(parse(from_os_str), long)]
        sql_file: Option<PathBuf>,
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
async fn main() {
    let cmd = Command::from_args();
    if let Err(e) = execute_command(cmd).await {
        println!("{:?}", e);
        std::process::exit(-1);
    }
}

async fn execute_command(cmd: Command) -> Result<(), Error> {
    let config = SessionConfig::new().with_information_schema(true);
    let ctx = SessionContext::new_with_config(config);
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
        Command::Convert { one_file, input, output } => {
            let input_filename = parse_filename(&input)?;
            let output_filename = parse_filename(&output)?;
            convert_files(&ctx, input_filename, output_filename, one_file).await?;
        }
        Command::Query {
            table,
            tables,
            sql,
            sql_file,
            output,
            verbose,
        } => {
            if let Some(dir) = tables {
                let paths = fs::read_dir(&dir)?;
                for path in paths {
                    let path = path?.path();
                    let file_name =
                        path.file_stem().unwrap().to_str().ok_or_else(|| {
                            DataFusionError::Internal("Invalid filename".to_string())
                        })?;
                    let table_name = sanitize_table_name(file_name);
                    println!("Registering table '{}' for {}", table_name, path.display());
                    register_table(&ctx, &table_name, parse_filename(&path)?).await?;
                }
            }
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
            let sql = match (sql, sql_file) {
                (Some(text), None) => text,
                (None, Some(file)) => fs::read_to_string(file)?,
                _ => panic!("Must specify either sql or sql_file, but not both"),
            };
            let df = ctx.sql(&sql).await?;
            if verbose {
                let explain = df.clone().explain(false, false)?;
                explain.show().await?;
            }
            if let Some(path) = output {
                match path.extension() {
                    Some(x) => match x.to_str().unwrap() {
                        "csv" => {
                            println!("Writing results in CSV format to {}", path.display());
                            let _ = df
                                .write_csv(
                                    path.to_str().unwrap(),
                                    DataFrameWriteOptions::default(),
                                    None,
                                )
                                .await?;
                        }
                        "parquet" => {
                            println!("Writing results in Parquet format to {}", path.display());
                            let _ = df
                                .write_parquet(
                                    path.to_str().unwrap(),
                                    DataFrameWriteOptions::default(),
                                    None,
                                )
                                .await?;
                        }
                        _ => {
                            return Err(Error::General(
                                "Unsupported file format for saving query results".to_string(),
                            ))
                        }
                    },
                    _ => {
                        return Err(Error::General(
                            "Unsupported file format for saving query results".to_string(),
                        ))
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
            diff => return Err(Error::General(format!("{}", diff))),
        },
    }
    Ok(())
}
