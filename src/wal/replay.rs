use std::fs::File;
use std::io::{BufReader, ErrorKind, Read, Result};
use std::path::Path;

use crate::types::SampleReading;

pub struct WalReplay;

impl WalReplay {
    pub fn replay(path: impl AsRef<Path>) -> Result<()> {
        let file = File::open(path)?;
        let mut reader = BufReader::with_capacity(64 * 1024, file);
        let mut buf = [0u8; 24];

        // read chunks of 24 bytes until EOF
        loop {
            match reader.read_exact(&mut buf) {
                Ok(_) => {
                    // extract the series_id
                    let series_id = u64::from_le_bytes(buf[0..8].try_into().unwrap());

                    // use the helper method from SampleReading for the remaining 16 bytes
                    let reading_buf = buf[8..24].try_into().unwrap();
                    let reading = SampleReading::from_le_bytes(reading_buf);

                    // send this reading to your in-memory engine during startup
                    println!(
                        "replaying - series_id: {}, timestamp: {}, value: {}",
                        series_id, reading.timestamp, reading.value
                    );
                }
                Err(e) if e.kind() == ErrorKind::UnexpectedEof => {
                    break;
                }
                Err(e) => return Err(e),
            }
        }

        Ok(())
    }
}
