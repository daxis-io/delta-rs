//! Read-only, checkpoint-free Delta Lake queries for browser WASM.

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow_ipc::writer::StreamWriter;
use arrow_schema::{ArrowError, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{FileGroup, FileScanConfigBuilder, ParquetSource};
use datafusion::datasource::source::DataSourceExec;
use datafusion::error::DataFusionError;
use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_kernel::Snapshot;
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
use delta_kernel::engine::sync::SyncEngine;
use delta_kernel::scan::state::ScanFile;
use futures::stream::{BoxStream, StreamExt};
use object_store::memory::InMemory;
use object_store::path::{Error as ObjectPathError, Path};
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use thiserror::Error;
use url::Url;

/// SQL name assigned to the active Delta snapshot.
pub const BROWSER_TABLE_NAME: &str = "delta";

/// Maximum Arrow IPC result accepted by the POC boundary.
pub const MAX_IPC_RESULT_BYTES: usize = 8 * 1024 * 1024;

/// The query result returned across the browser boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrowserQueryResult {
    /// Arrow IPC stream bytes.
    pub ipc_stream: Vec<u8>,
    /// Number of rows in the result.
    pub row_count: usize,
    /// Object bytes transferred while executing the query.
    pub bytes_fetched: u64,
    /// Object requests issued while executing the query.
    pub request_count: u64,
}

/// Errors from the browser-only Delta reader.
#[derive(Debug, Error)]
pub enum BrowserDeltaError {
    /// Delta Kernel could not replay the snapshot.
    #[error("Delta snapshot replay failed: {0}")]
    Kernel(#[from] delta_kernel::Error),
    /// The caller-provided object store failed.
    #[error("browser object store failed: {0}")]
    ObjectStore(#[from] object_store::Error),
    /// A URL path could not be represented as an object-store path.
    #[error("invalid browser object path: {0}")]
    ObjectPath(#[from] ObjectPathError),
    /// A table or data-file URL was invalid.
    #[error("invalid browser table URL: {0}")]
    Url(#[from] url::ParseError),
    /// DataFusion could not plan or execute the SQL.
    #[error("browser query failed: {0}")]
    DataFusion(#[from] DataFusionError),
    /// Arrow schema conversion or IPC serialization failed.
    #[error("Arrow IPC conversion failed: {0}")]
    Arrow(#[from] ArrowError),
    /// The table root is not a directory URL.
    #[error("invalid browser table root: {0}")]
    InvalidTableRoot(String),
    /// Delta metadata contained an invalid file size.
    #[error("active Delta file {path} has invalid size {size}")]
    InvalidFileSize {
        /// Delta-relative file path.
        path: String,
        /// Size recorded by the Delta add action.
        size: i64,
    },
    /// The serialized query result exceeded the browser boundary.
    #[error(
        "Arrow IPC result is {actual_bytes} bytes, exceeding the {max_bytes}-byte browser POC budget"
    )]
    ResultTooLarge {
        /// Serialized result size.
        actual_bytes: usize,
        /// Configured result budget.
        max_bytes: usize,
    },
}

/// A read-only Delta table backed by a caller-provided object store.
#[derive(Debug)]
pub struct BrowserDeltaTable {
    snapshot_version: i64,
    object_store_url: ObjectStoreUrl,
    store: Arc<dyn ObjectStore>,
    metrics: Arc<TransferMetrics>,
    provider: Arc<ActiveParquetTable>,
}

impl BrowserDeltaTable {
    /// Prefetch and replay the checkpoint-free version-0 Delta log.
    pub async fn open(
        table_root: Url,
        store: Arc<dyn ObjectStore>,
    ) -> Result<Self, BrowserDeltaError> {
        let table_root = normalize_table_root(table_root)?;
        let object_store_url = object_store_url(&table_root)?;
        let metrics = Arc::new(TransferMetrics::default());
        let store: Arc<dyn ObjectStore> =
            Arc::new(MeteredReadStore::new(store, Arc::clone(&metrics)));

        let log_url = table_root.join("_delta_log/00000000000000000000.json")?;
        let log_path = Path::from_url_path(log_url.path())?;
        let log_bytes = store.get(&log_path).await?.bytes().await?;

        let cache = Arc::new(InMemory::new());
        cache.put(&log_path, log_bytes.into()).await?;
        let cache_store: Arc<dyn ObjectStore> = cache;
        let engine = Arc::new(SyncEngine::new_with_store(cache_store));

        let snapshot = Snapshot::builder_for(table_root.as_str())
            .at_version(0)
            .build(engine.as_ref())?;
        let scan = Arc::clone(&snapshot).scan_builder().build()?;
        let mut active_files = Vec::new();
        for metadata in scan.scan_metadata(engine.as_ref())? {
            active_files = metadata?.visit_scan_files(active_files, push_scan_file)?;
        }

        let schema: Schema = snapshot.schema().as_ref().try_into_arrow()?;
        let provider = Arc::new(ActiveParquetTable::new(
            Arc::new(schema),
            object_store_url.clone(),
            &table_root,
            active_files,
        )?);

        Ok(Self {
            snapshot_version: i64::try_from(snapshot.version()).map_err(|_| {
                BrowserDeltaError::InvalidTableRoot(format!(
                    "snapshot version {} exceeds i64",
                    snapshot.version()
                ))
            })?,
            object_store_url,
            store,
            metrics,
            provider,
        })
    }

    /// Return the replayed Delta snapshot version.
    pub fn snapshot_version(&self) -> i64 {
        self.snapshot_version
    }

    /// Execute SQL against the active Parquet files and return Arrow IPC.
    pub async fn query_ipc(&self, sql: &str) -> Result<BrowserQueryResult, BrowserDeltaError> {
        let before = self.metrics.snapshot();
        let runtime = RuntimeEnvBuilder::new()
            .with_disk_manager_builder(
                DiskManagerBuilder::default().with_mode(DiskManagerMode::Disabled),
            )
            .build_arc()?;
        let config = SessionConfig::new().with_target_partitions(1);
        let context = SessionContext::new_with_config_rt(config, runtime);
        context.register_object_store(self.object_store_url.as_ref(), Arc::clone(&self.store));
        context.register_table(BROWSER_TABLE_NAME, self.provider.clone())?;

        let dataframe = context.sql(sql).await?;
        let result_schema = dataframe.schema().inner().clone();
        let batches = dataframe.collect().await?;
        let row_count = batches.iter().map(|batch| batch.num_rows()).sum();

        let mut ipc_stream = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut ipc_stream, result_schema.as_ref())?;
            for batch in &batches {
                writer.write(batch)?;
            }
            writer.finish()?;
        }
        if ipc_stream.len() > MAX_IPC_RESULT_BYTES {
            return Err(BrowserDeltaError::ResultTooLarge {
                actual_bytes: ipc_stream.len(),
                max_bytes: MAX_IPC_RESULT_BYTES,
            });
        }

        let after = self.metrics.snapshot();
        Ok(BrowserQueryResult {
            ipc_stream,
            row_count,
            bytes_fetched: after.bytes.saturating_sub(before.bytes),
            request_count: after.requests.saturating_sub(before.requests),
        })
    }
}

fn normalize_table_root(mut table_root: Url) -> Result<Url, BrowserDeltaError> {
    if table_root.cannot_be_a_base()
        || table_root.query().is_some()
        || table_root.fragment().is_some()
    {
        return Err(BrowserDeltaError::InvalidTableRoot(table_root.to_string()));
    }
    if !table_root.path().ends_with('/') {
        let path = format!("{}/", table_root.path());
        table_root.set_path(&path);
    }
    Ok(table_root)
}

fn object_store_url(table_root: &Url) -> Result<ObjectStoreUrl, BrowserDeltaError> {
    let mut origin = table_root.clone();
    origin.set_path("/");
    origin.set_query(None);
    origin.set_fragment(None);
    Ok(ObjectStoreUrl::parse(origin.as_str())?)
}

fn push_scan_file(files: &mut Vec<ScanFile>, file: ScanFile) {
    files.push(file);
}

#[derive(Debug)]
struct ActiveParquetTable {
    schema: SchemaRef,
    object_store_url: ObjectStoreUrl,
    files: Vec<PartitionedFile>,
}

impl ActiveParquetTable {
    fn new(
        schema: SchemaRef,
        object_store_url: ObjectStoreUrl,
        table_root: &Url,
        active_files: Vec<ScanFile>,
    ) -> Result<Self, BrowserDeltaError> {
        let files = active_files
            .into_iter()
            .map(|file| {
                let size =
                    u64::try_from(file.size).map_err(|_| BrowserDeltaError::InvalidFileSize {
                        path: file.path.clone(),
                        size: file.size,
                    })?;
                let location = table_root.join(&file.path)?;
                let path = Path::from_url_path(location.path())?;
                Ok(PartitionedFile::new(path.to_string(), size))
            })
            .collect::<Result<Vec<_>, BrowserDeltaError>>()?;
        Ok(Self {
            schema,
            object_store_url,
            files,
        })
    }
}

#[async_trait]
impl TableProvider for ActiveParquetTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let source = Arc::new(ParquetSource::new(Arc::clone(&self.schema)));
        let builder = FileScanConfigBuilder::new(self.object_store_url.clone(), source)
            .with_file_groups(vec![FileGroup::new(self.files.clone())])
            .with_limit(limit)
            .with_projection_indices(projection.cloned())?;
        Ok(DataSourceExec::from_data_source(builder.build()))
    }
}

#[derive(Debug, Default)]
struct TransferMetrics {
    requests: AtomicUsize,
    bytes: AtomicUsize,
}

impl TransferMetrics {
    fn snapshot(&self) -> MetricSnapshot {
        MetricSnapshot {
            requests: self.requests.load(Ordering::Relaxed) as u64,
            bytes: self.bytes.load(Ordering::Relaxed) as u64,
        }
    }

    fn record_request(&self) {
        self.requests.fetch_add(1, Ordering::Relaxed);
    }

    fn record_bytes(&self, bytes: u64) {
        self.bytes.fetch_add(
            usize::try_from(bytes).unwrap_or(usize::MAX),
            Ordering::Relaxed,
        );
    }
}

#[derive(Debug, Clone, Copy)]
struct MetricSnapshot {
    requests: u64,
    bytes: u64,
}

#[derive(Debug)]
struct MeteredReadStore {
    inner: Arc<dyn ObjectStore>,
    metrics: Arc<TransferMetrics>,
}

impl MeteredReadStore {
    fn new(inner: Arc<dyn ObjectStore>, metrics: Arc<TransferMetrics>) -> Self {
        Self { inner, metrics }
    }
}

impl fmt::Display for MeteredReadStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "browser-metered({})", self.inner)
    }
}

fn read_only_error(operation: &str) -> object_store::Error {
    object_store::Error::NotImplemented {
        operation: operation.to_owned(),
        implementer: "deltalake-browser read-only store".to_owned(),
    }
}

#[async_trait]
impl ObjectStore for MeteredReadStore {
    async fn put_opts(
        &self,
        _location: &Path,
        _payload: PutPayload,
        _options: PutOptions,
    ) -> object_store::Result<PutResult> {
        Err(read_only_error("put_opts"))
    }

    async fn put_multipart_opts(
        &self,
        _location: &Path,
        _options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        Err(read_only_error("put_multipart_opts"))
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let is_head = options.head;
        self.metrics.record_request();
        let result = self.inner.get_opts(location, options).await?;
        if !is_head {
            self.metrics
                .record_bytes(result.range.end.saturating_sub(result.range.start));
        }
        Ok(result)
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        locations
            .map(|location| match location {
                Ok(_) => Err(read_only_error("delete_stream")),
                Err(error) => Err(error),
            })
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.metrics.record_request();
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.metrics.record_request();
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        _from: &Path,
        _to: &Path,
        _options: CopyOptions,
    ) -> object_store::Result<()> {
        Err(read_only_error("copy_opts"))
    }
}
