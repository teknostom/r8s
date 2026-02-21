use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "r8s", about = "Lightweight single-node Kubernetes in Rust")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Create a new cluster
    Create {
        /// Cluster name
        name: Option<String>,
    },
    /// Start a cluster
    Up {
        /// Cluster name (defaults to last used or only cluster)
        name: Option<String>,
        /// Run in foreground instead of daemonizing
        #[arg(long)]
        foreground: bool,
    },
    /// Stop a cluster (preserves state)
    Down {
        /// Cluster name
        name: Option<String>,
    },
    /// Delete a cluster and all its data
    Delete {
        /// Cluster name
        name: String,
    },
    /// List all clusters
    List,
    /// Show cluster status
    Status {
        /// Cluster name
        name: Option<String>,
    },
    /// Print kubeconfig path for a cluster
    Kubeconfig {
        /// Cluster name
        name: Option<String>,
    },
    /// Tail daemon logs
    Logs {
        /// Cluster name
        name: Option<String>,
        /// Follow log output
        #[arg(short, long)]
        follow: bool,
    },
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Command::Create { name } => {
            let name = name.unwrap_or_else(|| "default".to_string());
            println!("Creating cluster '{name}'...");
            // TODO: implement cluster creation
        }
        Command::Up { name, foreground } => {
            let name = name.unwrap_or_else(|| "default".to_string());
            println!(
                "Starting cluster '{name}'{}...",
                if foreground { " (foreground)" } else { "" }
            );
            // TODO: start r8sd daemon
        }
        Command::Down { name } => {
            let name = name.unwrap_or_else(|| "default".to_string());
            println!("Stopping cluster '{name}'...");
            // TODO: stop r8sd daemon
        }
        Command::Delete { name } => {
            println!("Deleting cluster '{name}'...");
            // TODO: stop daemon + remove data
        }
        Command::List => {
            println!("No clusters configured yet.");
            // TODO: list clusters
        }
        Command::Status { name } => {
            let name = name.unwrap_or_else(|| "default".to_string());
            println!("Cluster '{name}': not running");
            // TODO: show cluster status
        }
        Command::Kubeconfig { name } => {
            let name = name.unwrap_or_else(|| "default".to_string());
            println!("~/.local/share/r8s/{name}/kubeconfig");
            // TODO: actual kubeconfig path
        }
        Command::Logs { name, follow: _ } => {
            let name = name.unwrap_or_else(|| "default".to_string());
            println!("No logs for cluster '{name}' (not running)");
            // TODO: tail daemon logs
        }
    }

    Ok(())
}
