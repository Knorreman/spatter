use std::fmt;

#[derive(Debug, Clone)]
pub enum Error {
    InvalidMaster(String),
    InvalidParallelism(usize),
    EmptyRdd,
    CountOverflow,
    MissingPartition(usize),
    Cancelled,
    ExecutorUnimplemented,
    Io(String),
    Cluster(String),
    PartitionPanic { partition: usize, message: String },
    Panic { message: String },
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::InvalidMaster(m) => write!(f, "unsupported master URL: {m}"),
            Error::InvalidParallelism(n) => write!(f, "parallelism must be >= 1, got {n}"),
            Error::EmptyRdd => write!(f, "RDD is empty"),
            Error::CountOverflow => write!(f, "count overflowed usize"),
            Error::MissingPartition(p) => write!(f, "missing partition {p}"),
            Error::Cancelled => write!(f, "task cancelled"),
            Error::ExecutorUnimplemented => {
                write!(f, "networked executor not implemented; use local[*]")
            }
            Error::Io(msg) => write!(f, "io error: {msg}"),
            Error::Cluster(msg) => write!(f, "cluster error: {msg}"),
            Error::PartitionPanic { partition, message } => {
                write!(f, "partition {partition} panicked: {message}")
            }
            Error::Panic { message } => write!(f, "task panicked: {message}"),
        }
    }
}

impl std::error::Error for Error {}

pub type Result<T> = std::result::Result<T, Error>;
