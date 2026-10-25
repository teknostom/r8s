use rustc_hash::FxHashMap;
use std::sync::Mutex;
use std::time::Duration;

use crate::error::RuntimeError;
use crate::traits::*;

pub struct MockRuntime {
    containers: Mutex<FxHashMap<String, MockContainer>>,
    next_id: Mutex<u64>,
}

struct MockContainer {
    config: ContainerConfig,
    running: bool,
    exit_code: Option<i32>,
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

    pub fn stop_matching(&self, name_contains: &str) {
        self.stop_matching_with_code(name_contains, 0);
    }

    pub fn stop_matching_with_code(&self, name_contains: &str, code: i32) {
        let mut containers = self.containers.lock().expect("mock lock poisoned");
        for c in containers.values_mut() {
            if c.config.name.contains(name_contains) && c.running {
                c.running = false;
                c.exit_code = Some(code);
            }
        }
    }
}

impl ContainerRuntime for MockRuntime {
    async fn has_image(&self, _image: &str) -> bool {
        true
    }

    async fn pull_image(
        &self,
        image: &str,
        _auth: Option<&RegistryAuth>,
    ) -> Result<ImageId, RuntimeError> {
        tracing::info!(image, "mock: pulling image");
        Ok(ImageId(format!("mock-{image}")))
    }

    async fn create_container(
        &self,
        config: &ContainerConfig,
    ) -> Result<ContainerId, RuntimeError> {
        let mut next_id = self.next_id.lock().expect("mock lock poisoned");
        let id = format!("mock-container-{next_id}");
        *next_id += 1;

        let mut containers = self.containers.lock().expect("mock lock poisoned");
        containers.insert(
            id.clone(),
            MockContainer {
                config: config.clone(),
                running: false,
                exit_code: None,
            },
        );
        tracing::info!(id, name = config.name, "mock: created container");
        Ok(ContainerId(id))
    }

    async fn prepare_task(&self, _id: &ContainerId) -> Result<(), RuntimeError> {
        Ok(())
    }

    async fn start_container(&self, id: &ContainerId) -> Result<(), RuntimeError> {
        let mut containers = self.containers.lock().expect("mock lock poisoned");
        if let Some(c) = containers.get_mut(&id.0) {
            c.running = true;
            c.exit_code = None;
            tracing::info!(id = id.0, "mock: started container");
        }
        Ok(())
    }

    async fn stop_container(&self, id: &ContainerId, _timeout: Duration) -> Result<(), RuntimeError> {
        let mut containers = self.containers.lock().expect("mock lock poisoned");
        if let Some(c) = containers.get_mut(&id.0) {
            c.running = false;
            if c.exit_code.is_none() {
                c.exit_code = Some(0);
            }
            tracing::info!(id = id.0, "mock: stopped container");
        }
        Ok(())
    }

    async fn remove_container(&self, id: &ContainerId) -> Result<(), RuntimeError> {
        let mut containers = self.containers.lock().expect("mock lock poisoned");
        containers.remove(&id.0);
        tracing::info!(id = id.0, "mock: removed container");
        Ok(())
    }

    async fn container_status(&self, id: &ContainerId) -> Result<ContainerStatus, RuntimeError> {
        let containers = self.containers.lock().expect("mock lock poisoned");
        match containers.get(&id.0) {
            Some(c) => Ok(ContainerStatus {
                id: id.clone(),
                running: c.running,
                exit_code: if c.running { None } else { c.exit_code },
            }),
            None => Err(RuntimeError::NotFound(id.0.clone())),
        }
    }

    async fn container_pid(&self, _id: &ContainerId) -> Result<u32, RuntimeError> {
        Err(anyhow::anyhow!("mock runtime has no container PIDs").into())
    }

    async fn list_owned_containers(&self) -> Result<Vec<OwnedContainer>, RuntimeError> {
        let containers = self.containers.lock().expect("mock lock poisoned");
        Ok(containers
            .iter()
            .map(|(id, c)| OwnedContainer {
                id: ContainerId(id.clone()),
                pod_uid: c.config.pod_uid.clone(),
                pod_name: c.config.pod_name.clone(),
            })
            .collect())
    }

    async fn exec_sync(
        &self,
        _id: &ContainerId,
        _command: &[String],
        _timeout: Duration,
    ) -> Result<i32, RuntimeError> {
        // No real process to exec into; treat probes as passing.
        Ok(0)
    }
}
