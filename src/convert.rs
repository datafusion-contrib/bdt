use crate::utils::{file_format, register_table};
use crate::{Error, FileFormat};

use datafusion::prelude::SessionContext;
use datafusion::{
    arrow::record_batch::RecordBatch,
    dataframe::DataFrameWriteOptions,
    parquet::{
        basic::{Compression, Encoding, ZstdLevel},
        file::properties::WriterProperties,
    },
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
    let props = if zstd {
        WriterProperties::builder()
            .set_created_by("bdt".to_string())
            .set_encoding(Encoding::PLAIN)
            .set_compression(Compression::ZSTD(ZstdLevel::try_new(8)?))
            .build()
    } else {
        WriterProperties::builder()
            .set_created_by("bdt".to_string())
            .set_encoding(Encoding::PLAIN)
            .build()
    };

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
            .write_parquet(output_filename, write_options, Some(props))
            .await
            .map_err(|e| e.into()),
        FileFormat::Arrow => unimplemented!(),
    }
}
