use crate::Error;
use comfy_table::{Cell, Table};
use datafusion::parquet::basic::LogicalType;
use datafusion::parquet::file::reader::{FileReader, SerializedFileReader};
use datafusion::parquet::file::statistics::{Statistics, ValueStatistics};
use std::fmt::Display;
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
            let logical_type = column.column_descr().logical_type_ref();
            if let Some(t) = logical_type {
                row.push(format!("{:?}", t));
            } else {
                row.push(not_available.clone());
            }
            match column.statistics() {
                Some(stats) => {
                    row.push(format!("{}", stats.physical_type()));
                    row.push(format_stat(stats.distinct_count_opt()));
                    row.push(format_stat(stats.null_count_opt()));

                    let bounds = match stats {
                        Statistics::Boolean(v) => format_bounds(v),
                        Statistics::Int32(v) => format_bounds(v),
                        Statistics::Int64(v) => format_bounds(v),
                        Statistics::Float(v) => format_bounds(v),
                        Statistics::Double(v) => format_bounds(v),
                        Statistics::ByteArray(v)
                            if matches!(logical_type, Some(LogicalType::String)) =>
                        {
                            [v.min_opt(), v.max_opt()].map(|value| {
                                format_stat(
                                    value.map(|bytes| String::from_utf8_lossy(bytes.data())),
                                )
                            })
                        }
                        Statistics::ByteArray(v) => format_bounds(v),
                        _ => [stats.min_bytes_opt(), stats.max_bytes_opt()]
                            .map(|value| format_stat(value.map(|_| "unsupported"))),
                    };
                    row.extend(bounds);
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

fn format_stat(value: Option<impl Display>) -> String {
    value.map_or_else(|| "N/A".to_owned(), |value| value.to_string())
}

fn format_bounds<T: Display>(stats: &ValueStatistics<T>) -> [String; 2] {
    [stats.min_opt(), stats.max_opt()].map(format_stat)
}
