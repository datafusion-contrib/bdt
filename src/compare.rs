use crate::utils::RowIter;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{Result, ScalarValue};
use datafusion::prelude::*;
use std::path::PathBuf;

pub async fn compare_files(
    path1: PathBuf,
    path2: PathBuf,
    has_header: bool,
    epsilon: Option<f64>,
) -> Result<bool> {
    let ctx = SessionContext::new();
    let batches1 = read_file(&ctx, path1.to_str().unwrap(), has_header).await?;
    let batches2 = read_file(&ctx, path2.to_str().unwrap(), has_header).await?;
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
                                ) => (ll - rr) < epsilon,
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
                            println!(" left: {:?}", a);
                            println!("right: {:?}", b);
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
                println!(" left: {:?}", a);
                println!("right: {:?}", b);
                return Ok(false);
            }
        }
    } else {
        println!("row counts do not match: {} != {}", count1, count2);
        return Ok(false);
    }
    Ok(true)
}

async fn read_file(
    ctx: &SessionContext,
    filename: &str,
    has_header: bool,
) -> Result<Vec<RecordBatch>> {
    let df = if filename.ends_with(".csv") {
        let read_options = CsvReadOptions::new().has_header(has_header);
        ctx.read_csv(filename, read_options).await?
    } else if filename.ends_with(".parquet") {
        ctx.read_parquet(filename, ParquetReadOptions::default())
            .await?
    } else {
        todo!()
    };
    df.collect().await
}
