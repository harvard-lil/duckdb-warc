use std::{error::Error, path::PathBuf};

use duckdb::{
    core::{DataChunkHandle, Inserter, LogicalTypeId},
    Result,
};
use glob::glob;
use jiff;
use warc::{BufferedBody, Error as WarcError, Record, WarcReader};

use crate::schema::WARC_FIELDS;

pub struct Loader {
    pub pattern: String,
}

impl Loader {
    pub fn parse_filepaths(&self) -> Result<Vec<PathBuf>, Box<dyn Error>> {
        let filepaths = match &self.pattern {
            pattern if pattern.contains(|char| "*?[".contains(char)) => glob(&pattern)?
                .filter_map(Result::ok)
                .collect::<Vec<PathBuf>>(),
            _ => vec![PathBuf::from(&self.pattern)],
        };

        Ok(filepaths)
    }

    pub fn read_file(filepath: &PathBuf) -> Result<Vec<Record<BufferedBody>>, WarcError> {
        match filepath.extension() {
            Some(ext) if ext == "gz" => {
                let reader = WarcReader::from_path_gzip(filepath).map_err(WarcError::ReadData)?;
                reader.iter_records().collect()
            }
            _ => {
                let reader = WarcReader::from_path(filepath).map_err(WarcError::ReadData)?;
                reader.iter_records().collect()
            }
        }
    }

    pub fn insert_records(
        filepath: &str,
        records: &[Record<BufferedBody>],
        output: &mut DataChunkHandle,
        offset: usize,
    ) -> Result<(), Box<dyn Error>> {
        for (record_index, record) in records.iter().enumerate() {
            let output_index = offset + record_index;

            let filepath_vector = output.flat_vector(0);
            filepath_vector.insert(output_index, filepath);

            let mut index_vector = output.flat_vector(1);
            let index_slice = index_vector.as_mut_slice::<u32>();
            index_slice[output_index] = record_index as u32;

            for (field_index, field) in WARC_FIELDS.iter().enumerate() {
                let mut column_vector = output.flat_vector(field_index + 2);
                match record.header(field.header.clone()) {
                    Some(value) => match field.field_type {
                        LogicalTypeId::Varchar => {
                            column_vector.insert(output_index, &value.to_string());
                        }
                        LogicalTypeId::Integer => {
                            let slice = column_vector.as_mut_slice::<u32>();
                            slice[output_index] = value.parse::<u32>()?;
                        }
                        LogicalTypeId::Timestamp => {
                            let slice = column_vector.as_mut_slice::<i64>();
                            let timestamp: jiff::Timestamp = value.parse()?;
                            slice[output_index] = timestamp.as_microsecond();
                        }
                        _ => {
                            column_vector.set_null(output_index);
                        }
                    },
                    None => {
                        column_vector.set_null(output_index);
                    }
                }
            }
            let body_vector = output.flat_vector(WARC_FIELDS.len() + 2);
            body_vector.insert(output_index, record.body());
        }

        Ok(())
    }
}
