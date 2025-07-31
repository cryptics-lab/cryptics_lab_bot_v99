// Models for Thalex API responses
use serde::Deserialize;
use crate::domain::model::exchange::Instrument;

#[derive(Debug, Deserialize)]
pub struct InstrumentResponse {
    pub id: u64,
    pub result: Vec<Instrument>,
}
