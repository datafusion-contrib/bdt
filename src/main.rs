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

use datafusion::prelude::*;
use std::io::{Error, ErrorKind, Result};
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
async fn main() {
    let cmd = Command::from_args();

    let ctx = SessionContext::new();

    match cmd {
        Command::View { filename } => {
            let filename = filename.to_str().unwrap();
            let df = read(&ctx, filename).await.unwrap();
            df.show_limit(10).await.unwrap();
        }
        Command::Convert { input, output } => {
            let input_filename = input.to_str().unwrap();
            let output_filename = output.to_str().unwrap();
            let df = read(&ctx, input_filename).await.unwrap();
            match file_format(output_filename).unwrap() {
                FileFormat::Csv => df.write_parquet(output_filename, None).await.unwrap(),
                FileFormat::Json => df.write_json(output_filename).await.unwrap(),
                FileFormat::Parquet => df.write_csv(output_filename).await.unwrap(),
            }
        }
    }
}

async fn read(ctx: &SessionContext, filename: &str) -> Result<Arc<DataFrame>> {
    match file_format(filename).unwrap() {
        FileFormat::Parquet => ctx
            .register_parquet("t", filename, ParquetReadOptions::default())
            .await
            .unwrap(),
        FileFormat::Csv => ctx
            .register_csv("t", filename, CsvReadOptions::default())
            .await
            .unwrap(),
        FileFormat::Json => ctx
            .register_json("t", filename, NdJsonReadOptions::default())
            .await
            .unwrap(),
    }
    Ok(ctx.sql("SELECT * FROM t").await.unwrap())
}

fn file_format(filename: &str) -> Result<FileFormat> {
    match filename.rfind('.') {
        Some(i) => match &filename[i + 1..] {
            "csv" => Ok(FileFormat::Csv),
            "json" => Ok(FileFormat::Json),
            "parquet" => Ok(FileFormat::Parquet),
            _ => Err(Error::new(ErrorKind::Other, "")),
        },
        _ => Err(Error::new(ErrorKind::Other, "")),
    }
}
