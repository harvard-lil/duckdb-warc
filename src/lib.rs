#![warn(
    clippy::all,
    nonstandard_style,
    rust_2018_idioms,
    rust_2021_compatibility
)]

pub mod loader;
pub mod schema;

use duckdb::{
    core::{DataChunkHandle, LogicalTypeHandle, LogicalTypeId},
    vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab},
    Connection, Result,
};
use duckdb_loadable_macros::duckdb_entrypoint_c_api;
use libduckdb_sys as ffi;
use std::{
    error::Error,
    sync::atomic::{AtomicBool, Ordering},
};
use warc::{BufferedBody, Record};

use crate::loader::Loader;
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
            let field_type_handle = field.get_field_type_handle();
            bind.add_result_column(field.name, field_type_handle)
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
            let loader = Loader {
                pattern: &bind_data.filepath,
            };
            let filepaths = loader.parse_filepaths()?;
            let records = filepaths
                .iter()
                .flat_map(|filepath| Loader::read_file(filepath))
                .flatten()
                .collect::<Vec<Record<BufferedBody>>>();
            Loader::insert_records(records, output)?;
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
