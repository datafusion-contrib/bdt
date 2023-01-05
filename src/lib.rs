use datafusion::error::DataFusionError;
use datafusion::parquet::errors::ParquetError;

pub mod compare;
pub mod utils;

#[derive(Debug)]
pub enum Error {
    General(String),
    DataFusion(DataFusionError),
    Parquet(ParquetError),
    IoError(std::io::Error),
}

impl From<DataFusionError> for Error {
    fn from(e: DataFusionError) -> Self {
        Self::DataFusion(e)
    }
}

impl From<ParquetError> for Error {
    fn from(e: ParquetError) -> Self {
        Self::Parquet(e)
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Self::IoError(e)
    }
}

pub enum FileFormat {
    Avro,
    Csv,
    Json,
    Parquet,
}
