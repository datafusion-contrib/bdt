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
use std::path::PathBuf;
use std::sync::Arc;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "bdt", about = "Boring Data Tool")]
enum Command {
    View {
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
    Csv,
    Json,
    Parquet,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cmd = Command::from_args();
    let ctx = SessionContext::new();
    match cmd {
        Command::View { filename } => {
            let filename = parse_filename(&filename)?;
            let df = read(&ctx, filename).await?;
            df.show_limit(10).await?;
        }
        Command::Convert { input, output } => {
            let input_filename = parse_filename(&input)?;
            let output_filename = parse_filename(&output)?;
            let df = read(&ctx, input_filename).await?;
            match file_format(output_filename)? {
                FileFormat::Csv => df.write_parquet(output_filename, None).await?,
                FileFormat::Json => df.write_json(output_filename).await?,
                FileFormat::Parquet => df.write_csv(output_filename).await?,
            }
        }
    }
    Ok(())
}

fn parse_filename(filename: &PathBuf) -> Result<&str> {
    filename
        .to_str()
        .ok_or(DataFusionError::Internal("Invalid filename".to_string()))
}

async fn read(ctx: &SessionContext, filename: &str) -> Result<Arc<DataFrame>> {
    match file_format(filename)? {
        FileFormat::Parquet => {
            ctx.register_parquet("t", filename, ParquetReadOptions::default())
                .await?
        }
        FileFormat::Csv => {
            ctx.register_csv("t", filename, CsvReadOptions::default())
                .await?
        }
        FileFormat::Json => {
            ctx.register_json("t", filename, NdJsonReadOptions::default())
                .await?
        }
    }
    Ok(ctx.sql("SELECT * FROM t").await?)
}

fn file_format(filename: &str) -> Result<FileFormat> {
    match filename.rfind('.') {
        Some(i) => match &filename[i + 1..] {
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
