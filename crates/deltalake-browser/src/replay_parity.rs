//! Compare admitted async opening with the existing synchronous Kernel engine.
use super::*;
use datafusion::arrow::datatypes::Schema;
use datafusion::execution::{
    config::SessionConfig,
    memory_pool::{GreedyMemoryPool, MemoryPool},
    runtime_env::RuntimeEnvBuilder,
};
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion_executor::qualification_fixture::{FixtureStore, LogVariant, V0, V1};
use delta_kernel::{
    engine::arrow_conversion::TryIntoArrow, scan::state::ScanFile, snapshot::Snapshot,
};
use delta_kernel_default_engine::DefaultEngine;
use object_store::{ObjectStoreExt, path::Path};

#[derive(Debug)]
struct NoDataRead;
impl ParquetFileReaderFactory for NoDataRead {
    fn create_reader(
        &self,
        _: usize,
        _: PartitionedFile,
        _: Option<usize>,
        _: &ExecutionPlanMetricsSet,
    ) -> Result<Box<dyn datafusion::parquet::arrow::async_reader::AsyncFileReader + Send>> {
        panic!("snapshot/active-file parity must not decode data files")
    }
}
fn paths(out: &mut Vec<String>, file: ScanFile) {
    assert!(!file.dv_info.has_vector());
    assert!(file.transform.is_none());
    assert!(file.partition_values.is_empty());
    out.push(file.path);
}
#[test]
fn async_adapter_matches_existing_kernel_snapshot_and_live_add_replay() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let ordinary = Arc::new(object_store::memory::InMemory::new());
    runtime.block_on(async {
        for (path, bytes) in [
            ("table/_delta_log/00000000000000000000.json", V0),
            ("table/_delta_log/00000000000000000001.json", V1),
        ] {
            ordinary
                .put(&Path::from(path), bytes.to_vec().into())
                .await
                .unwrap();
        }
    });
    let engine = DefaultEngine::builder(ordinary).build();
    for requested in [None, Some(0), Some(1)] {
        let mut builder = Snapshot::builder_for("memory:///table/");
        if let Some(version) = requested {
            builder = builder.at_version(version);
        }
        let baseline = builder.build(&engine).unwrap();
        assert_eq!(baseline.version(), requested.unwrap_or(1));
        let schema: Schema = baseline.schema().as_ref().try_into_arrow().unwrap();
        let scan = baseline.scan_builder().build().unwrap();
        let mut expected = Vec::new();
        for batch in scan.scan_metadata(&engine).unwrap() {
            expected = batch.unwrap().visit_scan_files(expected, paths).unwrap();
        }
        expected.sort();
        let actual = runtime.block_on(async {
            let store = Arc::new(FixtureStore::new(LogVariant::Valid));
            let pool = Arc::new(GreedyMemoryPool::new(64 << 20));
            let runtime = RuntimeEnvBuilder::new()
                .with_memory_pool(pool.clone())
                .build_arc()
                .unwrap();
            let config = datafusion_executor::qualification_fixture::register(
                store.clone(),
                SessionConfig::new(),
                runtime.as_ref(),
            )
            .with_extension(Arc::new(ParquetFileReaderFactoryResolver::new(Arc::new(
                NoDataRead,
            ))));
            let caller = SessionContext::new_with_config_rt(config, runtime);
            let provider = open(
                &url::Url::parse("memory:///table/").unwrap(),
                requested,
                &caller,
                TaskLimits::qualification().with_limit(Resource::ListingEntries, 1),
            )
            .await
            .unwrap();
            assert_eq!(provider.version(), requested.unwrap_or(1));
            assert_eq!(provider.schema.as_ref(), &schema);
            let mut files = provider
                .files
                .iter()
                .map(|file| {
                    file.object_meta
                        .location
                        .as_ref()
                        .strip_prefix("table/")
                        .unwrap()
                        .to_owned()
                })
                .collect::<Vec<_>>();
            files.sort();
            assert_eq!(
                store
                    .ordinary_gets
                    .load(std::sync::atomic::Ordering::SeqCst),
                0
            );
            drop(provider);
            assert_eq!(pool.reserved(), 0);
            files
        });
        assert_eq!(actual, expected);
    }
}
