use warc::{BufferedBody, Error as WarcError, Record, WarcReader};

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
}
