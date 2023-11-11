use crate::Error;
use comfy_table::{Cell, Table};
use datafusion::parquet::basic::LogicalType;
use datafusion::parquet::file::reader::{FileReader, SerializedFileReader};
use datafusion::parquet::file::statistics::Statistics;
use std::fs::File;
use std::path::PathBuf;

pub fn view_parquet_meta(path: PathBuf) -> Result<(), Error> {
    let file = File::open(path).map_err(Error::from)?;
    let reader = SerializedFileReader::new(file).map_err(Error::from)?;

    let parquet_metadata = reader.metadata();

    let mut table = Table::new();
    table.load_preset("||--+-++|    ++++++");
    table.set_header(vec![Cell::new("Key"), Cell::new("Value")]);
    let file_meta = parquet_metadata.file_metadata();
    table.add_row(vec![
        Cell::new("Version"),
        Cell::new(format!("{}", file_meta.version())),
    ]);
    table.add_row(vec![
        Cell::new("Created By"),
        Cell::new(file_meta.created_by().unwrap_or("N/A")),
    ]);
    table.add_row(vec![
        Cell::new("Rows"),
        Cell::new(format!("{}", file_meta.num_rows())),
    ]);
    table.add_row(vec![
        Cell::new("Row Groups"),
        Cell::new(format!("{}", parquet_metadata.num_row_groups())),
    ]);
    println!("{}", table);

    for i in 0..parquet_metadata.num_row_groups() {
        let row_group_reader = reader.get_row_group(i)?;
        let md = row_group_reader.metadata();
        println!(
            "\nRow Group {} of {} contains {} rows and has {} bytes:\n",
            i,
            parquet_metadata.num_row_groups(),
            md.num_rows(),
            md.total_byte_size()
        );

        let mut table = Table::new();
        table.load_preset("||--+-++|    ++++++");
        let header: Vec<Cell> = [
            "Column Name",
            "Logical Type",
            "Physical Type",
            "Distinct Values",
            "Nulls",
            "Min",
            "Max",
        ]
        .iter()
        .map(Cell::new)
        .collect();
        table.set_header(header);

        let not_available = "N/A".to_string();
        for column in md.columns() {
            let mut row: Vec<String> = vec![];
            row.push(column.column_descr().name().to_owned());
            if let Some(t) = column.column_descr().logical_type() {
                row.push(format!("{:?}", t));
            } else {
                row.push(not_available.clone());
            }
            match column.statistics() {
                Some(stats) => {
                    row.push(format!("{}", stats.physical_type()));
                    if let Some(dc) = stats.distinct_count() {
                        row.push(format!("{}", dc));
                    } else {
                        row.push(not_available.clone());
                    }
                    row.push(format!("{}", stats.null_count()));

                    if stats.has_min_max_set() {
                        match &stats {
                            Statistics::Boolean(v) => {
                                row.push(format!("{}", v.min()));
                                row.push(format!("{}", v.max()));
                            }
                            Statistics::Int32(v) => {
                                row.push(format!("{}", v.min()));
                                row.push(format!("{}", v.max()));
                            }
                            Statistics::Int64(v) => {
                                row.push(format!("{}", v.min()));
                                row.push(format!("{}", v.max()));
                            }
                            Statistics::Float(v) => {
                                row.push(format!("{}", v.min()));
                                row.push(format!("{}", v.max()));
                            }
                            Statistics::Double(v) => {
                                row.push(format!("{}", v.min()));
                                row.push(format!("{}", v.max()));
                            }
                            Statistics::ByteArray(v) => {
                                match column.column_descr().logical_type() {
                                    Some(LogicalType::String) => {
                                        let min = v.min().as_utf8().unwrap();
                                        let max = v.min().as_utf8().unwrap();
                                        row.push(min.to_string());
                                        row.push(max.to_string());
                                    }
                                    _ => {
                                        row.push(format!("{}", v.min()));
                                        row.push(format!("{}", v.max()));
                                    }
                                }
                            }
                            _ => {
                                row.push("unsupported".to_owned());
                                row.push("unsupported".to_owned());
                            }
                        }
                    } else {
                        row.push(not_available.clone());
                        row.push(not_available.clone());
                    }
                }
                _ => {
                    for _ in 0..5 {
                        row.push(not_available.clone());
                    }
                }
            }
            table.add_row(row);
        }

        println!("{}", table);
    }
    Ok(())
}
