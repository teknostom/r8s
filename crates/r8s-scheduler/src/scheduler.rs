/// Single-node scheduler: watches for unscheduled pods and binds them to the node.
pub struct Scheduler {
    // TODO: store handle, node name
}

impl Scheduler {
    /// Run the scheduling loop.
    pub async fn run(&self) -> anyhow::Result<()> {
        todo!("Phase 5: implement single-node scheduler")
    }
}
