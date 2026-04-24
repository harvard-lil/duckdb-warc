#![warn(
    clippy::all,
    nonstandard_style,
    rust_2018_idioms,
    rust_2021_compatibility
)]

pub mod loader;
pub mod schema;

use std::{
    error::Error,
    sync::atomic::{AtomicBool, Ordering},
};

use duckdb::{
    core::{DataChunkHandle, LogicalTypeHandle, LogicalTypeId},
    duckdb_entrypoint_c_api,
    vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab},
    Connection, Result,
};

use crate::loader::{Loader, Source};
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
        bind.add_result_column(
            "record_source",
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        );
        bind.add_result_column(
            "record_index",
            LogicalTypeHandle::from(LogicalTypeId::Integer),
        );
        for field in WARC_FIELDS.iter() {
            let field_type_handle = field.get_field_type_handle();
            bind.add_result_column(field.name, field_type_handle);
        }
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
                source: Source::parse(&bind_data.filepath),
            };
            let sources = loader.resolve_sources()?;
            let mut row_offset: usize = 0;
            for (label, mut reader) in sources {
                let mut stream = reader.stream_records();
                let mut record_index: usize = 0;
                while let Some(result) = stream.next_item() {
                    let record = result?.into_buffered()?;
                    Loader::insert_record(&label, record_index, &record, output, row_offset)?;
                    record_index += 1;
                    row_offset += 1;
                }
            }
            output.set_len(row_offset);
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
