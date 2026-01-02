#![warn(
    clippy::all,
    nonstandard_style,
    rust_2018_idioms,
    rust_2021_compatibility
)]

pub mod schema;

use duckdb::{
    core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId},
    vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab},
    Connection, Result,
};
use duckdb_loadable_macros::duckdb_entrypoint_c_api;
use jiff;
use libduckdb_sys as ffi;
use std::{
    error::Error,
    sync::atomic::{AtomicBool, Ordering},
};
use warc::{BufferedBody, Error as WarcError, Record, WarcReader};

use crate::schema::WARC_FIELDS;

#[repr(C)]
struct ReadWarcBindData {
    filepath: String,
}

#[repr(C)]
struct ReadWarcInitData {
    done: AtomicBool,
}

struct ReadWarcVTab;

impl VTab for ReadWarcVTab {
    type InitData = ReadWarcInitData;
    type BindData = ReadWarcBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn Error>> {
        WARC_FIELDS.iter().for_each(|field| {
            let field_type = match &field.field_type {
                LogicalTypeId::Varchar => LogicalTypeHandle::from(LogicalTypeId::Varchar),
                LogicalTypeId::Integer => LogicalTypeHandle::from(LogicalTypeId::Integer),
                LogicalTypeId::Timestamp => LogicalTypeHandle::from(LogicalTypeId::Timestamp),
                _ => LogicalTypeHandle::from(LogicalTypeId::Varchar),
            };
            bind.add_result_column(field.name, field_type)
        });
        bind.add_result_column("body", LogicalTypeHandle::from(LogicalTypeId::Blob));

        let filepath = bind.get_parameter(0).to_string();
        Ok(ReadWarcBindData { filepath })
    }

    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn Error>> {
        Ok(ReadWarcInitData {
            done: AtomicBool::new(false),
        })
    }

    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn Error>> {
        let init_data = func.get_init_data();
        let bind_data = func.get_bind_data();

        if init_data.done.swap(true, Ordering::Relaxed) {
            output.set_len(0);
        } else {
            let records = match &bind_data.filepath {
                filepath if filepath.to_lowercase().ends_with(".gz") => {
                    let reader = WarcReader::from_path_gzip(&filepath)?;
                    reader
                        .iter_records()
                        .collect::<Result<Vec<Record<BufferedBody>>, WarcError>>()?
                }
                filepath => {
                    let reader = WarcReader::from_path(&filepath)?;
                    reader
                        .iter_records()
                        .collect::<Result<Vec<Record<BufferedBody>>, WarcError>>()?
                }
            };

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
        }

        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![LogicalTypeHandle::from(LogicalTypeId::Varchar)])
    }
}

#[duckdb_entrypoint_c_api(ext_name = "duckdb_warc")]
pub unsafe fn extension_entrypoint(con: Connection) -> Result<(), Box<dyn Error>> {
    con.register_table_function::<ReadWarcVTab>("read_warc")
        .expect("Failed to register read_warc table function");
    Ok(())
}
