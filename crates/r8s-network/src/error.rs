/// Errors from configuring the host network: a shelled-out command exited
/// non-zero, or an I/O operation failed. `r8sd` bridges these into `anyhow` at
/// the binary edge.
#[derive(Debug, thiserror::Error)]
pub enum NetworkError {
    #[error("{cmd} {args}: {stderr}")]
    Command {
        cmd: String,
        args: String,
        stderr: String,
    },
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, NetworkError>;
