use std::{
    error::Error,
    fs::File,
    io::{BufRead, BufReader, Read},
    path::{Path, PathBuf},
};

use duckdb::{
    core::{DataChunkHandle, Inserter, LogicalTypeId},
    Result,
};
use flate2::read::MultiGzDecoder;
use glob::glob;
use jiff;
use warc::{BufferedBody, Record, WarcReader};

use crate::schema::WARC_FIELDS;

const BUF_CAPACITY: usize = 1_000_000;

pub enum Source {
    Local(String),
    #[cfg(feature = "http")]
    Remote(String),
}

impl Source {
    pub fn parse(path: &str) -> Self {
        #[cfg(feature = "http")]
        if path.starts_with("http://") || path.starts_with("https://") {
            return Source::Remote(path.to_owned());
        }
        Source::Local(path.to_owned())
    }
}

enum Compression {
    Gzip,
    None,
}

impl Compression {
    fn detect(filename: &str) -> Self {
        match filename.split('.').next_back() {
            Some("gz") => Compression::Gzip,
            _ => Compression::None,
        }
    }

    fn wrap_reader<'a, R: Read + 'a>(self, reader: R) -> Box<dyn BufRead + 'a> {
        match self {
            Compression::Gzip => Box::new(BufReader::with_capacity(
                BUF_CAPACITY,
                MultiGzDecoder::new(BufReader::with_capacity(BUF_CAPACITY, reader)),
            )),
            Compression::None => Box::new(BufReader::with_capacity(BUF_CAPACITY, reader)),
        }
    }
}

pub struct Loader {
    pub source: Source,
}

impl Loader {
    pub fn resolve_sources(
        &self,
    ) -> Result<Vec<(String, WarcReader<Box<dyn BufRead>>)>, Box<dyn Error>> {
        match &self.source {
            Source::Local(pattern) => {
                let paths = Self::expand_glob(pattern)?;
                paths
                    .into_iter()
                    .map(|path| {
                        let label = path.to_string_lossy().to_string();
                        let reader = Self::read_local(&path)?;
                        Ok((label, reader))
                    })
                    .collect()
            }
            #[cfg(feature = "http")]
            Source::Remote(url) => {
                let reader = Self::read_remote(url)?;
                Ok(vec![(url.clone(), reader)])
            }
        }
    }

    fn expand_glob(pattern: &str) -> Result<Vec<PathBuf>, Box<dyn Error>> {
        if pattern.contains(|c| "*?[".contains(c)) {
            Ok(glob(pattern)?.filter_map(Result::ok).collect())
        } else {
            Ok(vec![PathBuf::from(pattern)])
        }
    }

    fn read_local(filepath: &Path) -> Result<WarcReader<Box<dyn BufRead>>, Box<dyn Error>> {
        let file = File::open(filepath)?;
        let buf_read = Compression::detect(&filepath.to_string_lossy()).wrap_reader(file);
        Ok(WarcReader::new(buf_read))
    }

    #[cfg(feature = "http")]
    fn read_remote(url: &str) -> Result<WarcReader<Box<dyn BufRead>>, Box<dyn Error>> {
        let response = ureq::get(url).call().map_err(|e| format!("{url}: {e}"))?;
        let body = response.into_body().into_reader();
        let path = url::Url::parse(url)?.path().to_owned();
        let buf_read = Compression::detect(&path).wrap_reader(body);
        Ok(WarcReader::new(buf_read))
    }

    pub fn insert_record(
        record_source: &str,
        record_index: usize,
        record: &Record<BufferedBody>,
        output: &mut DataChunkHandle,
        output_index: usize,
    ) -> std::result::Result<(), Box<dyn Error>> {
        let record_source_vector = output.flat_vector(0);
        record_source_vector.insert(output_index, record_source);

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

        Ok(())
    }
}
