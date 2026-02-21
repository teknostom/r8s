#[derive(Debug, thiserror::Error)]
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

    #[error("internal storage error: {0}")]
    Internal(#[from] anyhow::Error),
}
