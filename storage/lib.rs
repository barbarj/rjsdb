use std::rc::Rc;

pub struct NumericCfg {
    // TODO: Both of these values should probably be a smaller type.
    // Figure out what that type should be.
    max_precision: usize,
    max_scale: usize,
}

pub struct NumericValue {
    cfg: Rc<NumericCfg>,
    val: usize,
}

pub struct Char {
    v: String,
}

// TODO: Make it so:
// both date and time (no time zone)
// Low value: 4713 BC
// High value: 294276 AD
// Resolution: 1 microsecond
pub struct Timestamp {
    v: u64,
}

pub enum DbValue {
    Numeric(NumericValue),
    Integer(i32),
    Varchar(String),
    Char(Char),
    Double(f64),
    Timestamp(Timestamp),
}
