use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Result, Write};
use std::path::Path;
use std::sync::Mutex;

use crate::types::SampleReading;

pub struct WalWriter {
    writer: Mutex<BufWriter<File>>,
}

impl WalWriter {
    pub fn new(path: impl AsRef<Path>) -> Result<Self> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;

        Ok(Self {
            writer: Mutex::new(BufWriter::with_capacity(64 * 1024, file)),
        })
    }

    pub fn append(&self, series_id: u64, reading: &SampleReading) -> Result<()> {
        let mut buf = [0u8; 24];
        buf[0..8].copy_from_slice(&series_id.to_le_bytes());

        // zero-cost slice translation into the fixed-size [u8; 16] required by SampleReading
        let reading_buf = (&mut buf[8..24]).try_into().unwrap();
        reading.write_le_bytes(reading_buf);

        let mut writer = self.writer.lock().unwrap();
        writer.write_all(&buf)
    }

    pub fn append_batch(&self, readings: &[(u64, &SampleReading)]) -> Result<()> {
        let mut writer = self.writer.lock().unwrap();
        let mut buf = [0u8; 24];

        for &(series_id, reading) in readings {
            buf[0..8].copy_from_slice(&series_id.to_le_bytes());

            // re-bind the mutable slice view on each iteration
            let reading_buf = (&mut buf[8..24]).try_into().unwrap();
            reading.write_le_bytes(reading_buf);

            writer.write_all(&buf)?;
        }

        Ok(())
    }

    pub fn flush(&self) -> Result<()> {
        self.writer.lock().unwrap().flush()
    }
}
