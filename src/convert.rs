use crate::utils::{file_format, register_table};
use crate::{Error, FileFormat};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::prelude::SessionContext;

pub async fn convert_files(
    ctx: &SessionContext,
    input_filename: &str,
    output_filename: &str,
) -> Result<Vec<RecordBatch>, Error> {
    let df = register_table(ctx, "t", input_filename).await?;
    let write_options = DataFrameWriteOptions::default();
    match file_format(output_filename)? {
        FileFormat::Avro => Err(Error::General(
            "Conversion to Avro is not supported".to_string(),
        )),
        FileFormat::Csv => df
            .write_csv(output_filename, write_options, None)
            .await
            .map_err(|e| e.into()),
        FileFormat::Json => df
            .write_json(output_filename, write_options)
            .await
            .map_err(|e| e.into()),
        FileFormat::Parquet => df
            .write_parquet(output_filename, write_options, None)
            .await
            .map_err(|e| e.into()),
        FileFormat::Arrow => unimplemented!(),
    }
}
