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
use std::path::PathBuf;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "bdt", about = "Boring Data Tool")]
enum Command {
    View {
        #[structopt(parse(from_os_str))]
        filename: PathBuf,
    },
}

#[tokio::main]
async fn main() {
    let cmd = Command::from_args();

    match cmd {
        Command::View { filename } => {
            let filename = filename.to_str().unwrap();
            let ctx = SessionContext::new();
            if filename.ends_with(".parquet") {
                ctx.register_parquet("t", filename, ParquetReadOptions::default())
                    .await
                    .unwrap();
            } else {
                todo!("unsupported file extension")
            }
            let df = ctx.sql("SELECT * FROM t").await.unwrap();
            df.show_limit(10).await.unwrap();
        }
    }
}
