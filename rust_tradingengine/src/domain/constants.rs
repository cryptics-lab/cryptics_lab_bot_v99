// Constants used used to match responses from the exchange to corresponding request
// The numbers are arbitrary, but they need to be unique per CALL_ID.
// We'll use 100+ for identifying trade requests for orders.
pub const CALL_ID_INSTRUMENTS: u64 = 0;
pub const CALL_ID_INSTRUMENT: u64 = 1;
pub const CALL_ID_SUBSCRIBE: u64 = 2;
pub const CALL_ID_LOGIN: u64 = 3;
pub const CALL_ID_CANCEL_SESSION: u64 = 4;
pub const CALL_ID_SET_COD: u64 = 5;