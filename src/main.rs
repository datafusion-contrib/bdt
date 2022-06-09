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

use datafusion::common::{DataFusionError, Result};
use datafusion::prelude::*;
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
                FileFormat::Csv => df.write_parquet(output_filename, None).await?,
                FileFormat::Json => df.write_json(output_filename).await?,
                FileFormat::Parquet => df.write_csv(output_filename).await?,
            }
        }
    }
    Ok(())
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
