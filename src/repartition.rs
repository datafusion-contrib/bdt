use datafusion::prelude::{Partitioning, SessionContext};

use crate::utils::{file_format, register_table};
use crate::{Error, FileFormat};

pub async fn repartition(
    ctx: &SessionContext,
    num: usize,
    input_filename: &str,
    output_filename: &str,
) -> Result<(), Error> {
    let df = register_table(ctx, "t", input_filename).await?;
    let parted_df = df.repartition(Partitioning::RoundRobinBatch(num))?;
    match file_format(input_filename)? {
        FileFormat::Avro => Err(Error::General("Avro format is not supported".to_string())),
        FileFormat::Csv => parted_df
            .write_csv(output_filename)
            .await
            .map_err(|e| e.into()),
        FileFormat::Json => parted_df
            .write_json(output_filename)
            .await
            .map_err(|e| e.into()),
        FileFormat::Parquet => parted_df
            .write_parquet(output_filename, None)
            .await
            .map_err(|e| e.into()),
        FileFormat::Arrow => Err(Error::General("Arrow format is not supported".to_string())),
    }
}
