use crate::utils::{file_format, register_table};
use crate::{Error, FileFormat};

use datafusion::prelude::SessionContext;
use datafusion::{
    arrow::record_batch::RecordBatch,
    common::{
        config::TableParquetOptions,
        parquet_config::{DFParquetCompression, DFParquetEncoding},
    },
    dataframe::DataFrameWriteOptions,
};

pub async fn convert_files(
    ctx: &SessionContext,
    input_filename: &str,
    output_filename: &str,
    single_file: bool,
    zstd: bool,
) -> Result<Vec<RecordBatch>, Error> {
    let df = register_table(ctx, "t", input_filename).await?;
    let write_options = DataFrameWriteOptions::default().with_single_file_output(single_file);

    match file_format(output_filename)? {
        FileFormat::Avro => Err(Error::General(
            "Conversion to Avro is not supported".to_string(),
        )),
        FileFormat::Csv => df
            .write_csv(output_filename, write_options, None)
            .await
            .map_err(|e| e.into()),
        FileFormat::Json => df
            .write_json(output_filename, write_options, None)
            .await
            .map_err(|e| e.into()),
        FileFormat::Parquet => {
            let mut options = TableParquetOptions::default();
            options.global.created_by = "bdt".to_string();
            options.global.encoding = Some(DFParquetEncoding::Plain);
            options.global.compression = Some(if zstd {
                DFParquetCompression::Zstd(8)
            } else {
                DFParquetCompression::Uncompressed
            });
            df.write_parquet(output_filename, write_options, Some(options))
                .await
                .map_err(Error::from)
        }
        FileFormat::Arrow => unimplemented!(),
    }
}
