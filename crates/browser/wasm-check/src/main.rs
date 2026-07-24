use deltalake_browser::{BrowserDeltaTable, BrowserQueryResult};

fn main() {
    let _ = BrowserDeltaTable::open;
    let _ = BrowserDeltaTable::snapshot_version;
    let _ = BrowserDeltaTable::query_ipc;
    let _: Option<BrowserQueryResult> = None;
}
