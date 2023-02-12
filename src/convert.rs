use crate::utils::{file_format, register_table};
use crate::{Error, FileFormat};
use datafusion::logical_expr::Partitioning;
use datafusion::prelude::SessionContext;

pub async fn convert_files(
    ctx: &SessionContext,
    input_filename: &str,
    output_filename: &str,
    delimiter: Option<String>,
    partitions: Option<usize>,
) -> Result<(), Error> {
    let df = register_table(ctx, "t", input_filename, delimiter).await?;

    let df = if let Some(p) = partitions {
        df.repartition(Partitioning::RoundRobinBatch(p))?
    } else {
        df
    };
    match file_format(output_filename)? {
        FileFormat::Avro => Err(Error::General(
            "Conversion to Avro is not supported".to_string(),
        )),
        FileFormat::Csv => df.write_csv(output_filename).await.map_err(|e| e.into()),
        FileFormat::Json => df.write_json(output_filename).await.map_err(|e| e.into()),
        FileFormat::Parquet => df
            .write_parquet(output_filename, None)
            .await
            .map_err(|e| e.into()),
    }
}
