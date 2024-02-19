use datafusion::error::DataFusionError;
use datafusion::parquet::errors::ParquetError;

pub mod compare;
pub mod convert;
pub mod parquet;
pub mod utils;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{0}")]
    General(String),
    #[error("Data Fusion error: {0}")]
    DataFusion(#[from] DataFusionError),
    #[error("Parquet error: {0}")]
    Parquet(#[from] ParquetError),
    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),
}

#[derive(Debug)]
pub enum FileFormat {
    Arrow,
    Avro,
    Csv,
    Json,
    Parquet,
}
