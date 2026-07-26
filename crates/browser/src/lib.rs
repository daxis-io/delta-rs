//! Read-only, checkpoint-free Delta Lake queries for browser WASM.

use std::any::Any;
use std::fmt;
use std::io::{self, Write};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

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
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
use delta_kernel::engine::sync::SyncEngine;
use delta_kernel::scan::state::ScanFile;
use delta_kernel::Snapshot;
use futures::stream::{BoxStream, Stream, StreamExt};
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
    /// Delta metadata referenced an object outside the table root.
    #[error("active Delta file {path} is outside table root {table_root}")]
    ActiveFileOutsideTable {
        /// Delta add-action path.
        path: String,
        /// Normalized table root.
        table_root: String,
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
        let batches = dataframe.execute_stream().await?;
        let (ipc_stream, row_count) =
            write_ipc_stream(result_schema, batches, MAX_IPC_RESULT_BYTES).await?;

        let after = self.metrics.snapshot();
        Ok(BrowserQueryResult {
            ipc_stream,
            row_count,
            bytes_fetched: after.bytes.saturating_sub(before.bytes),
            request_count: after.requests.saturating_sub(before.requests),
        })
    }
}

#[derive(Debug)]
struct CappedIpcBuffer {
    bytes: Vec<u8>,
    max_bytes: usize,
}

impl CappedIpcBuffer {
    fn new(max_bytes: usize) -> Self {
        Self {
            bytes: Vec::with_capacity(max_bytes),
            max_bytes,
        }
    }

    fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }
}

#[derive(Debug, Clone, Copy, Error)]
#[error("Arrow IPC write attempted {actual_bytes} bytes with a {max_bytes}-byte limit")]
struct IpcResultLimitExceeded {
    actual_bytes: usize,
    max_bytes: usize,
}

impl Write for CappedIpcBuffer {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        let actual_bytes = self.bytes.len().saturating_add(buffer.len());
        if actual_bytes > self.max_bytes {
            return Err(io::Error::other(IpcResultLimitExceeded {
                actual_bytes,
                max_bytes: self.max_bytes,
            }));
        }

        self.bytes.extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn map_ipc_error(error: ArrowError) -> BrowserDeltaError {
    let limit = match &error {
        ArrowError::IoError(_, source) => source
            .get_ref()
            .and_then(|source| source.downcast_ref::<IpcResultLimitExceeded>())
            .copied(),
        _ => None,
    };

    match limit {
        Some(IpcResultLimitExceeded {
            actual_bytes,
            max_bytes,
        }) => BrowserDeltaError::ResultTooLarge {
            actual_bytes,
            max_bytes,
        },
        None => BrowserDeltaError::Arrow(error),
    }
}

async fn write_ipc_stream<S>(
    schema: SchemaRef,
    batches: S,
    max_bytes: usize,
) -> Result<(Vec<u8>, usize), BrowserDeltaError>
where
    S: Stream<Item = datafusion::error::Result<arrow_array::RecordBatch>>,
{
    futures::pin_mut!(batches);
    let buffer = CappedIpcBuffer::new(max_bytes);
    let mut writer = StreamWriter::try_new(buffer, schema.as_ref()).map_err(map_ipc_error)?;
    let mut row_count = 0;

    while let Some(batch) = batches.next().await {
        let batch = batch?;
        row_count += batch.num_rows();
        writer.write(&batch).map_err(map_ipc_error)?;
    }

    let buffer = writer.into_inner().map_err(map_ipc_error)?;
    Ok((buffer.into_bytes(), row_count))
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

fn same_origin(left: &Url, right: &Url) -> bool {
    left.scheme() == right.scheme()
        && left.host_str() == right.host_str()
        && left.port_or_known_default() == right.port_or_known_default()
}

fn active_file_path(table_root: &Url, file_path: &str) -> Result<Path, BrowserDeltaError> {
    let location = table_root.join(file_path)?;
    let path = Path::from_url_path(location.path())?;
    let table_prefix = Path::from_url_path(table_root.path())?;

    if !same_origin(table_root, &location)
        || path == table_prefix
        || !path.prefix_matches(&table_prefix)
    {
        return Err(BrowserDeltaError::ActiveFileOutsideTable {
            path: file_path.to_owned(),
            table_root: table_root.to_string(),
        });
    }

    Ok(path)
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
                let path = active_file_path(table_root, &file.path)?;
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
    fn as_any(&self) -> &dyn Any {
        self
    }

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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::Poll;

    use arrow_array::{RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use delta_kernel::scan::state::DvInfo;

    use super::*;

    fn scan_file(path: &str) -> ScanFile {
        ScanFile {
            path: path.to_owned(),
            size: 1,
            modification_time: 0,
            stats: None,
            dv_info: DvInfo::default(),
            transform: None,
            partition_values: HashMap::new(),
        }
    }

    #[test]
    fn active_files_cannot_escape_the_table_origin_or_path_prefix() {
        let table_root = Url::parse("memory://fixture/table/").unwrap();
        let object_store_url = ObjectStoreUrl::parse("memory://fixture/").unwrap();

        for path in [
            "../outside.parquet",
            "/outside.parquet",
            "memory://other/table/part.parquet",
            "https://attacker.invalid/table/part.parquet",
            "%2e%2e/outside.parquet",
        ] {
            let error = ActiveParquetTable::new(
                Arc::new(Schema::empty()),
                object_store_url.clone(),
                &table_root,
                vec![scan_file(path)],
            )
            .unwrap_err();
            assert!(
                matches!(
                    error,
                    BrowserDeltaError::ActiveFileOutsideTable {
                        path: ref rejected,
                        table_root: ref rejected_root
                    } if rejected == path && rejected_root == table_root.as_str()
                ),
                "unexpected rejection for {path}: {error}"
            );
        }

        let encoded_separator = ActiveParquetTable::new(
            Arc::new(Schema::empty()),
            object_store_url,
            &table_root,
            vec![scan_file("..%2Foutside.parquet")],
        );
        assert!(encoded_separator.is_err());
    }

    #[test]
    fn active_files_allow_descendants_of_the_table_root() {
        let table_root = Url::parse("memory://fixture/table/").unwrap();
        let object_store_url = ObjectStoreUrl::parse("memory://fixture/").unwrap();
        let result = ActiveParquetTable::new(
            Arc::new(Schema::empty()),
            object_store_url,
            &table_root,
            vec![scan_file("nested/part.parquet")],
        );

        assert!(result.is_ok());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn ipc_limit_stops_polling_the_query_stream() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Utf8,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec!["x".repeat(4096)]))],
        )
        .unwrap();
        let polls = Arc::new(AtomicUsize::new(0));
        let poll_count = Arc::clone(&polls);
        let mut next_batch = Some(batch);
        let stream = futures::stream::poll_fn(move |_| {
            poll_count.fetch_add(1, Ordering::Relaxed);
            match next_batch.take() {
                Some(batch) => Poll::Ready(Some(Ok(batch))),
                None => panic!("query stream was polled after the IPC budget was exceeded"),
            }
        });

        let error = write_ipc_stream(schema, stream, 512).await.unwrap_err();
        assert!(
            matches!(
                error,
                BrowserDeltaError::ResultTooLarge {
                    actual_bytes,
                    max_bytes: 512
                } if actual_bytes > 512
            ),
            "unexpected error: {error}"
        );
        assert_eq!(polls.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn capped_ipc_buffer_rejects_before_exceeding_its_limit() {
        let mut buffer = CappedIpcBuffer::new(4);
        buffer.write_all(b"1234").unwrap();

        let error = buffer.write_all(b"5").unwrap_err();
        assert_eq!(buffer.bytes, b"1234");
        assert_eq!(buffer.bytes.capacity(), 4);
        assert_eq!(error.kind(), io::ErrorKind::Other);
    }
}
