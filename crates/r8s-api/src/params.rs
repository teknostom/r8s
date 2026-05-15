use serde::Deserialize;

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListParams {
    pub watch: Option<String>,
    pub label_selector: Option<String>,
    pub field_selector: Option<String>,
    pub limit: Option<u64>,
    #[serde(rename = "continue")]
    pub continue_token: Option<String>,
    pub resource_version: Option<String>,
    pub timeout_seconds: Option<u64>,
    pub send_initial_events: Option<String>,
}

impl ListParams {
    pub fn is_watch(&self) -> bool {
        matches!(self.watch.as_deref(), Some("true") | Some("1") | Some(""))
    }

    pub fn wants_initial_events(&self) -> bool {
        matches!(
            self.send_initial_events.as_deref(),
            Some("true") | Some("1")
        )
    }
}
