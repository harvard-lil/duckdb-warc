use duckdb::{
    core::{DataChunkHandle, Inserter, LogicalTypeId},
    Result,
};
use jiff;
use std::error::Error;
use warc::{BufferedBody, Error as WarcError, Record, WarcReader};

use crate::schema::WARC_FIELDS;

pub enum Compression {
    None,
    Gzip,
}

pub struct Loader<'a> {
    pub filepath: &'a String,
    pub compression: Compression,
}

impl<'a> Loader<'a> {
    pub fn read(&self) -> Result<Vec<Record<BufferedBody>>, WarcError> {
        match self.compression {
            Compression::None => {
                let reader = WarcReader::from_path(&self.filepath)
                    .map_err(|error| WarcError::ReadData(error))?;
                reader.iter_records().collect()
            }
            Compression::Gzip => {
                let reader = WarcReader::from_path_gzip(&self.filepath)
                    .map_err(|error| WarcError::ReadData(error))?;
                reader.iter_records().collect()
            }
        }
    }

    pub fn insert_records(
        records: Vec<Record<BufferedBody>>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn Error>> {
        for (record_index, record) in records.iter().enumerate() {
            for (field_index, field) in WARC_FIELDS.iter().enumerate() {
                let mut column_vector = output.flat_vector(field_index);
                match record.header(field.header.clone()) {
                    Some(value) => match field.field_type {
                        LogicalTypeId::Varchar => {
                            column_vector.insert(record_index, &value.to_string());
                        }
                        LogicalTypeId::Integer => {
                            let slice = column_vector.as_mut_slice::<u32>();
                            slice[record_index] = value.parse::<u32>()?;
                        }
                        LogicalTypeId::Timestamp => {
                            let slice = column_vector.as_mut_slice::<i64>();
                            let timestamp: jiff::Timestamp = value.parse()?;
                            slice[record_index] = timestamp.as_microsecond();
                        }
                        _ => {
                            column_vector.set_null(record_index);
                        }
                    },
                    None => {
                        column_vector.set_null(record_index);
                    }
                }
            }
            let body_vector = output.flat_vector(WARC_FIELDS.len());
            body_vector.insert(record_index, record.body());
            output.set_len(records.len());
        }

        Ok(())
    }
}
