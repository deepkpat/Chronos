use crate::types::Tags;

#[derive(Debug, Clone)]
pub struct Sample {
    pub key: SeriesKey,
    pub reading: SampleReading,
}

#[derive(Debug, Clone)]
pub struct SeriesKey {
    pub metric: String,
    pub tags: Tags,
}

#[derive(Debug, Clone)]
pub struct SampleReading {
    pub timestamp: i64,
    pub value: f64,
}

impl SampleReading {
    #[inline]
    pub fn write_le_bytes(&self, buf: &mut [u8; 16]) {
        buf[0..8].copy_from_slice(&self.timestamp.to_le_bytes());
        buf[8..16].copy_from_slice(&self.value.to_le_bytes());
    }

    #[inline]
    pub fn from_le_bytes(buf: &[u8; 16]) -> Self {
        let timestamp = i64::from_le_bytes(buf[0..8].try_into().unwrap());
        let value = f64::from_le_bytes(buf[8..16].try_into().unwrap());

        Self { timestamp, value }
    }
}
