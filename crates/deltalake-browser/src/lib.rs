//! Finite, opt-in Delta JSON opening through Kernel tasks and caller resources.
//! The caller installs the log capability and Parquet reader factory in its
//! existing session. Opening never registers a replacement store or reader.
use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::{context::SessionContext, object_store::ObjectStoreUrl};
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_datasource::{
    PartitionedFile, file_groups::FileGroup, file_scan_config::FileScanConfigBuilder,
    source::DataSourceExec,
};
use datafusion_datasource_parquet::{
    ParquetFileReaderFactory, ParquetFileReaderFactoryRequired, ParquetFileReaderFactoryResolver,
    source::ParquetSource,
};
use datafusion_executor::JsonTaskHost;
use delta_kernel::tasks::*;
use std::sync::Arc;

pub use datafusion_executor::log_storage::{
    AdmittedJsonLogStorage, JsonLogReadLimits, JsonLogStorageRegistry,
};
mod materialize;

/// Standard DataFusion table at one Kernel-selected immutable snapshot version.
#[derive(Debug)]
pub struct DeltaTableProvider {
    version: u64,
    schema: SchemaRef,
    origin: ObjectStoreUrl,
    files: Vec<PartitionedFile>,
    reader: Arc<dyn ParquetFileReaderFactory>,
    _lease: Arc<datafusion::execution::memory_pool::MemoryReservation>,
}
impl DeltaTableProvider {
    /// Version selected by Kernel, including explicit historical opens.
    pub fn version(&self) -> u64 {
        self.version
    }
}

/// Open a primitive unpartitioned JSON-history table using caller registrations.
/// This local future can be polled directly in a browser; dropping it drops all
/// outstanding provider futures and metadata streams.
///
/// `TaskLimits` apply per Kernel operation: snapshot loading and live-file scanning each
/// use the supplied profile independently. Adapter file/schema materialization consumes
/// the scan operation's remaining work allowance; there is no parent budget spanning both.
/// The retained result lease covers the returned schema and file metadata. Ordinary and
/// concurrent downstream DataFusion query-plan metadata allocations are outside these
/// task limits. The ordinary Parquet adapter may synthesize nulls for nullable missing
/// columns; this opening API does not impose a strict physical-schema match.
///
/// The caller must register `AdmittedJsonLogStorage` bound to the same registered store
/// Arc and a Parquet reader factory. Qualification covers this closed JSON opening path;
/// generic Kernel Read/Head/Footer interfaces retain their caller-provided allocation
/// and identity obligations and are not a separately qualified general asynchronous engine.
pub async fn open(
    table_url: &url::Url,
    version: Option<u64>,
    session: &SessionContext,
    limits: TaskLimits,
) -> Result<Arc<DeltaTableProvider>> {
    let root_bytes = table_url.as_str().len();
    // Two URL clones plus normalization's old/new serialization buffers.
    // A path separator adds one byte. This check precedes either clone.
    let roots = root_bytes
        .checked_add(1)
        .and_then(|n| n.checked_mul(2 + 3))
        .and_then(|n| n.checked_add(materialize::registration_bytes("DeltaOpen")))
        .ok_or_else(|| {
            external(ResourceExhausted {
                resource: Resource::TaskStateBytes,
                limit: limits.limit(Resource::TaskStateBytes),
                observed: usize::MAX,
            })
        })?;
    if roots > limits.limit(Resource::TaskStateBytes) {
        return Err(external(ResourceExhausted {
            resource: Resource::TaskStateBytes,
            limit: limits.limit(Resource::TaskStateBytes),
            observed: roots,
        }));
    }
    if roots > limits.limit(Resource::MetadataAllocatedBytes) {
        return Err(external(ResourceExhausted {
            resource: Resource::MetadataAllocatedBytes,
            limit: limits.limit(Resource::MetadataAllocatedBytes),
            observed: roots,
        }));
    }
    let root_lease = datafusion::execution::memory_pool::MemoryConsumer::new("DeltaOpen")
        .register(&session.runtime_env().memory_pool);
    root_lease.try_grow(roots)?;
    // The task and host share the remaining original authority. Adapter-owned
    // URL backing remains reserved alongside them, including capacity retained
    // when origin_url's serialization is truncated below.
    let task_limits = limits
        .with_limit(
            Resource::TaskStateBytes,
            limits.limit(Resource::TaskStateBytes) - roots,
        )
        .with_limit(
            Resource::MetadataAllocatedBytes,
            limits.limit(Resource::MetadataAllocatedBytes) - roots,
        );
    let mut table_root = table_url.clone();
    if !table_root.path().ends_with('/') {
        table_root
            .path_segments_mut()
            .map_err(|_| external(OperationFailure::malformed_response()))?
            .push("");
    }
    let mut origin_url = table_root.clone();
    origin_url.set_query(None);
    origin_url.set_fragment(None);
    origin_url.set_path("/");
    let origin = ObjectStoreUrl::try_from_url(origin_url)?;
    // Resolve both required capabilities before discovery performs any I/O.
    let host = JsonTaskHost::new(session, &origin, task_limits).map_err(external)?;
    let resolver = {
        let state = session.state_ref();
        let guard = state.read();
        guard
            .config()
            .get_extension::<ParquetFileReaderFactoryResolver>()
    };
    let reader = resolver
        .ok_or_else(|| external(ParquetFileReaderFactoryRequired::new()))?
        .resolve();
    let (id, mut driver) = AsyncOperationDriver::allocate(host, task_limits).map_err(external)?;
    let evaluation = driver.allocate_evaluation().map_err(external)?;
    let mut load = SnapshotLoadTask::try_new(id, evaluation, &table_root, version, task_limits)
        .map_err(external)?;
    let snapshot = drive(&mut load, &mut driver, task_limits).await?;
    drop(driver);
    let host = JsonTaskHost::new(session, &origin, task_limits).map_err(external)?;
    let (id, mut driver) = AsyncOperationDriver::allocate(host, task_limits).map_err(external)?;
    let evaluation = driver.allocate_evaluation().map_err(external)?;
    let selected_version = snapshot.version();
    let schema = snapshot.schema().clone();
    let mut scan =
        ScanMetadataTask::try_new(id, evaluation, snapshot, task_limits).map_err(external)?;
    let metadata = drive(&mut scan, &mut driver, task_limits).await?;
    drop(driver);
    let result = materialize::files_and_schema(
        &metadata,
        schema.as_ref(),
        &table_root,
        session,
        materialize::ConversionAdmission {
            limits,
            prior_work: scan.accounting().usage(Resource::WorkUnits).consumed(),
            retained_input: scan
                .accounting()
                .usage(Resource::TaskStateBytes)
                .peak_live()
                .checked_add(roots)
                .ok_or_else(|| external(OperationFailure::malformed_response()))?,
            origin_backing: roots,
        },
    )?;
    Ok(Arc::new(DeltaTableProvider {
        version: selected_version,
        schema: result.schema,
        origin,
        files: result.files,
        reader,
        _lease: result.lease,
    }))
}

async fn drive<T: OperationTask>(
    task: &mut T,
    driver: &mut AsyncOperationDriver<JsonTaskHost<'_>>,
    limits: TaskLimits,
) -> Result<T::Output> {
    let cpu = CpuSlice::new(
        limits.limit(Resource::TurnRecords),
        limits.limit(Resource::TurnInputBytes),
        limits.limit(Resource::TurnPlanNodes),
    )
    .map_err(external)?;
    let mut step = task.start(cpu).map_err(external)?;
    loop {
        step = match step {
            TaskStep::Yield => {
                // A genuine scheduling boundary, independent of Tokio.
                let mut pending = true;
                futures::future::poll_fn(|cx| {
                    if std::mem::take(&mut pending) {
                        cx.waker().wake_by_ref();
                        std::task::Poll::Pending
                    } else {
                        std::task::Poll::Ready(())
                    }
                })
                .await;
                task.progress(cpu).map_err(external)?
            }
            TaskStep::Execute(request) => {
                let key = driver.dispatch(request).map_err(external)?;
                driver
                    .complete_effect(task.pending_work().map_err(external)?)
                    .await
                    .map_err(external)?;
                task.resume(key, driver.take_response(key).map_err(external)?, cpu)
                    .map_err(external)?
            }
            TaskStep::Complete(value) => return Ok(value),
            TaskStep::Failed(error) => return Err(external(error)),
            TaskStep::Cancelled => {
                return Err(DataFusionError::Execution("Delta opening cancelled".into()));
            }
        };
    }
}

#[async_trait]
impl TableProvider for DeltaTableProvider {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
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
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Default filter support is Unsupported: DataFusion retains the user
        // predicate above this standard scan. No Delta-statistics pruning.
        let source = Arc::new(
            ParquetSource::new(self.schema.clone())
                .with_parquet_file_reader_factory(self.reader.clone())
                .with_pushdown_filters(false)
                .with_enable_page_index(false)
                .with_bloom_filter_on_read(false),
        );
        let config = FileScanConfigBuilder::new(self.origin.clone(), source)
            .with_file_group(FileGroup::new(self.files.clone()))
            .with_projection_indices(projection.cloned())?
            .with_limit(limit)
            .build();
        Ok(DataSourceExec::from_data_source(config))
    }
}
fn external<E: std::error::Error + Send + Sync + 'static>(error: E) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}

#[cfg(all(test, feature = "qualification-fixture", not(target_arch = "wasm32")))]
mod replay_parity;
