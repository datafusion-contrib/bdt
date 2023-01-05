use datafusion::arrow::array;
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::ScalarValue;

pub struct RowIter {
    batches: Vec<RecordBatch>,
    current_batch: usize,
    current_batch_offset: usize,
}

impl RowIter {
    pub fn new(batches: Vec<RecordBatch>) -> Self {
        Self {
            batches,
            current_batch: 0,
            current_batch_offset: 0,
        }
    }
}

impl Iterator for RowIter {
    type Item = Vec<ScalarValue>;

    fn next(&mut self) -> Option<Self::Item> {
        while self.current_batch < self.batches.len() {
            let b = &self.batches[self.current_batch];
            if self.current_batch_offset < b.num_rows() {
                let mut row: Vec<ScalarValue> = Vec::with_capacity(b.num_columns());
                let row_index = self.current_batch_offset;
                self.current_batch_offset += 1;
                for col_index in 0..b.num_columns() {
                    let array = b.column(col_index);
                    if array.is_null(row_index) {
                        row.push(ScalarValue::Null)
                    } else {
                        match array.data_type() {
                            DataType::Utf8 => {
                                let array =
                                    array.as_any().downcast_ref::<array::StringArray>().unwrap();
                                row.push(ScalarValue::Utf8(Some(
                                    array.value(row_index).to_string(),
                                )));
                            }
                            // TODO introduce macros to make this concise
                            DataType::Int8 => {
                                let array =
                                    array.as_any().downcast_ref::<array::Int8Array>().unwrap();
                                row.push(ScalarValue::Int8(Some(array.value(row_index))));
                            }
                            DataType::Int16 => {
                                let array =
                                    array.as_any().downcast_ref::<array::Int16Array>().unwrap();
                                row.push(ScalarValue::Int16(Some(array.value(row_index))));
                            }
                            DataType::Int32 => {
                                let array =
                                    array.as_any().downcast_ref::<array::Int32Array>().unwrap();
                                row.push(ScalarValue::Int32(Some(array.value(row_index))));
                            }
                            DataType::Int64 => {
                                let array =
                                    array.as_any().downcast_ref::<array::Int64Array>().unwrap();
                                row.push(ScalarValue::Int64(Some(array.value(row_index))));
                            }
                            DataType::UInt8 => {
                                let array =
                                    array.as_any().downcast_ref::<array::UInt8Array>().unwrap();
                                row.push(ScalarValue::UInt8(Some(array.value(row_index))));
                            }
                            DataType::UInt16 => {
                                let array =
                                    array.as_any().downcast_ref::<array::UInt16Array>().unwrap();
                                row.push(ScalarValue::UInt16(Some(array.value(row_index))));
                            }
                            DataType::UInt32 => {
                                let array =
                                    array.as_any().downcast_ref::<array::UInt32Array>().unwrap();
                                row.push(ScalarValue::UInt32(Some(array.value(row_index))));
                            }
                            DataType::UInt64 => {
                                let array =
                                    array.as_any().downcast_ref::<array::UInt64Array>().unwrap();
                                row.push(ScalarValue::UInt64(Some(array.value(row_index))));
                            }
                            DataType::Float32 => {
                                let array = array
                                    .as_any()
                                    .downcast_ref::<array::Float32Array>()
                                    .unwrap();
                                row.push(ScalarValue::Float32(Some(array.value(row_index))));
                            }
                            DataType::Float64 => {
                                let array = array
                                    .as_any()
                                    .downcast_ref::<array::Float64Array>()
                                    .unwrap();
                                row.push(ScalarValue::Float64(Some(array.value(row_index))));
                            }
                            DataType::Date32 => {
                                let array =
                                    array.as_any().downcast_ref::<array::Date32Array>().unwrap();
                                row.push(ScalarValue::Date32(Some(array.value(row_index))));
                            }
                            DataType::Date64 => {
                                let array =
                                    array.as_any().downcast_ref::<array::Date64Array>().unwrap();
                                row.push(ScalarValue::Date64(Some(array.value(row_index))));
                            }
                            other => {
                                println!("unsupported type: {}", other);
                                todo!("unsupported data type")
                            }
                        }
                    }
                }
                return Some(row);
            } else {
                // move onto next batch
                self.current_batch += 1;
                self.current_batch_offset = 0;
            }
        }
        None
    }
}
