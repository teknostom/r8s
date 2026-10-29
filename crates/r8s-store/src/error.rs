use thiserror::Error;

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("{gvr} '{name}' already exists")]
    AlreadyExists { gvr: String, name: String },

    #[error("{gvr} '{name}' not found")]
    NotFound { gvr: String, name: String },

    #[error("conflict on {gvr} '{name}': {message}")]
    Conflict {
        gvr: String,
        name: String,
        message: String,
    },

    #[error("resource version {requested} is too old; oldest available is {oldest_available}")]
    TooOld {
        requested: u64,
        oldest_available: u64,
    },

    #[error("metadata must be a JSON object")]
    InvalidMetadata,

    #[error("invalid continue token")]
    InvalidContinueToken,

    /// The continue token's revision has been compacted away; the carried
    /// token (rv = -1) resumes the list inconsistently at the latest revision.
    #[error("the provided continue parameter is too old to display a consistent list result")]
    ContinueExpired { inconsistent_token: String },

    #[error("invalid selector: {0}")]
    InvalidSelector(String),

    #[error(transparent)]
    Storage(Box<redb::Error>),

    #[error(transparent)]
    Serialization(#[from] serde_json::Error),
}

macro_rules! impl_from_redb {
    ($($err:ty),+ $(,)?) => {$(
        impl From<$err> for StoreError {
            fn from(err: $err) -> Self {
                StoreError::Storage(Box::new(redb::Error::from(err)))
            }
        }
    )+};
}

impl_from_redb!(
    redb::DatabaseError,
    redb::TransactionError,
    redb::TableError,
    redb::StorageError,
    redb::CommitError,
);

pub type Result<T> = std::result::Result<T, StoreError>;
