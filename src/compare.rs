use crate::utils::RowIter;
use crate::Error;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::ScalarValue;
use datafusion::prelude::*;
use std::fmt::{Display, Formatter};
use std::path::PathBuf;
use std::result::Result;

pub async fn compare_files(
    path1: PathBuf,
    path2: PathBuf,
    has_header: bool,
    epsilon: Option<f64>,
) -> Result<ComparisonResult, Error> {
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
                            let message = format!(
                                "data does not match at row {} column {}: {:?} != {:?}",
                                i, j, v1, v2
                            );
                            return Ok(ComparisonResult::row_diff(a, b, message));
                        }
                    }
                }
            } else {
                let message = format!(
                    "row lengths do not match at index {}: {} != {}",
                    i,
                    a.len(),
                    b.len()
                );
                return Ok(ComparisonResult::row_diff(a, b, message));
            }
        }
    } else {
        let message = format!("row counts do not match: {} != {}", count1, count2);
        return Ok(ComparisonResult::FileDiff(message));
    }
    Ok(ComparisonResult::Ok)
}

pub enum ComparisonResult {
    Ok,
    FileDiff(String),
    RowDiff {
        left: Vec<ScalarValue>,
        right: Vec<ScalarValue>,
        message: String,
    },
}

impl ComparisonResult {
    fn row_diff(left: Vec<ScalarValue>, right: Vec<ScalarValue>, message: String) -> Self {
        Self::RowDiff {
            left,
            right,
            message,
        }
    }
}

impl Display for ComparisonResult {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::RowDiff {
                left,
                right,
                message,
            } => {
                write!(
                    f,
                    "Row mismatch: {}\n left: {:?}\nright: {:?}",
                    message, left, right
                )
            }
            Self::FileDiff(message) => {
                write!(f, "Files are different: {}", message)
            }
            _ => {
                write!(f, "Files match")
            }
        }
    }
}

async fn read_file(
    ctx: &SessionContext,
    filename: &str,
    has_header: bool,
) -> Result<Vec<RecordBatch>, Error> {
    if let Some(i) = filename.rfind('.') {
        match &filename[i + 1..] {
            "csv" => {
                let read_options = CsvReadOptions::new().has_header(has_header);
                ctx.read_csv(filename, read_options)
                    .await
                    .map_err(Error::from)?
                    .collect()
                    .await
                    .map_err(Error::from)
            }
            "parquet" => ctx
                .read_parquet(filename, ParquetReadOptions::default())
                .await
                .map_err(Error::from)?
                .collect()
                .await
                .map_err(Error::from),
            other => Err(Error::General(format!(
                "Unsupported file extension: {}",
                other
            ))),
        }
    } else {
        Err(Error::General(format!(
            "Could not determine file extension"
        )))
    }
}
