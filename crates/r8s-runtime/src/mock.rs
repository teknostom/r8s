use rustc_hash::FxHashMap;
use std::sync::Mutex;
use std::time::Duration;

use crate::traits::*;

/// A mock container runtime that tracks state in memory without running real containers.
/// Useful for testing controllers and the API server.
pub struct MockRuntime {
    containers: Mutex<FxHashMap<String, MockContainer>>,
    next_id: Mutex<u64>,
}

struct MockContainer {
    _config: ContainerConfig,
    running: bool,
}

impl Default for MockRuntime {
    fn default() -> Self {
        Self {
            containers: Mutex::new(FxHashMap::default()),
            next_id: Mutex::new(0),
        }
    }
}

impl MockRuntime {
    pub fn new() -> Self {
        Self::default()
    }
}

impl ContainerRuntime for MockRuntime {
    async fn pull_image(&self, image: &str, _auth: Option<&RegistryAuth>) -> anyhow::Result<ImageId> {
        tracing::info!(image, "mock: pulling image");
        Ok(ImageId(format!("mock-{image}")))
    }

    async fn create_container(&self, config: &ContainerConfig) -> anyhow::Result<ContainerId> {
        let mut next_id = self.next_id.lock().unwrap();
        let id = format!("mock-container-{next_id}");
        *next_id += 1;

        let mut containers = self.containers.lock().unwrap();
        containers.insert(
            id.clone(),
            MockContainer {
                _config: config.clone(),
                running: false,
            },
        );
        tracing::info!(id, name = config.name, "mock: created container");
        Ok(ContainerId(id))
    }

    async fn start_container(&self, id: &ContainerId) -> anyhow::Result<()> {
        let mut containers = self.containers.lock().unwrap();
        if let Some(c) = containers.get_mut(&id.0) {
            c.running = true;
            tracing::info!(id = id.0, "mock: started container");
        }
        Ok(())
    }

    async fn stop_container(&self, id: &ContainerId, _timeout: Duration) -> anyhow::Result<()> {
        let mut containers = self.containers.lock().unwrap();
        if let Some(c) = containers.get_mut(&id.0) {
            c.running = false;
            tracing::info!(id = id.0, "mock: stopped container");
        }
        Ok(())
    }

    async fn remove_container(&self, id: &ContainerId) -> anyhow::Result<()> {
        let mut containers = self.containers.lock().unwrap();
        containers.remove(&id.0);
        tracing::info!(id = id.0, "mock: removed container");
        Ok(())
    }

    async fn container_status(&self, id: &ContainerId) -> anyhow::Result<ContainerStatus> {
        let containers = self.containers.lock().unwrap();
        match containers.get(&id.0) {
            Some(c) => Ok(ContainerStatus {
                id: id.clone(),
                running: c.running,
                exit_code: if c.running { None } else { Some(0) },
            }),
            None => anyhow::bail!("container not found: {}", id.0),
        }
    }

    async fn container_pid(&self, _id: &ContainerId) -> anyhow::Result<u32> {
        anyhow::bail!("mock runtime has no container PIDs")
    }
}
