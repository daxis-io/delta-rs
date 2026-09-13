//! Admission before the ordinary ScanFile visitor and portable schema conversion.
use super::external;
use datafusion::{
    arrow::datatypes::{Schema, SchemaRef},
    common::{DataFusionError, Result},
    execution::{
        context::SessionContext,
        memory_pool::{MemoryConsumer, MemoryReservation},
    },
};
use datafusion_datasource::PartitionedFile;
use delta_kernel::{
    engine::arrow_conversion::TryIntoArrow,
    engine_data::{GetData, RowVisitor},
    expressions::ColumnName,
    scan::{ScanMetadata, state::ScanFile},
    schema::{DataType, StructType},
    tasks::*,
};
use std::{mem::size_of, sync::Arc};

pub(super) struct Materialized {
    pub schema: SchemaRef,
    pub files: Vec<PartitionedFile>,
    pub lease: Arc<MemoryReservation>,
}
#[repr(C)]
struct SharedOwner<T> {
    strong: usize,
    weak: usize,
    value: T,
}
pub(super) fn registration_bytes(name: &str) -> usize {
    size_of::<
        SharedOwner<(
            Arc<dyn datafusion::execution::memory_pool::MemoryPool>,
            MemoryConsumer,
        )>,
    >() + name.len()
        + size_of::<SharedOwner<MemoryReservation>>()
}
fn check(resource: Resource, used: usize, limits: TaskLimits) -> Result<()> {
    if used > limits.limit(resource) {
        return Err(external(ResourceExhausted {
            resource,
            limit: limits.limit(resource),
            observed: used,
        }));
    }
    Ok(())
}
fn add(a: usize, b: usize, limits: TaskLimits) -> Result<usize> {
    a.checked_add(b).ok_or_else(|| {
        external(ResourceExhausted {
            resource: Resource::TaskStateBytes,
            limit: limits.limit(Resource::TaskStateBytes),
            observed: usize::MAX,
        })
    })
}

pub(super) struct ConversionAdmission {
    pub limits: TaskLimits,
    pub prior_work: usize,
    pub retained_input: usize,
    pub origin_backing: usize,
}

pub(super) fn files_and_schema(
    metadata: &[ScanMetadata],
    schema: &StructType,
    root: &url::Url,
    session: &SessionContext,
    admission: ConversionAdmission,
) -> Result<Materialized> {
    let ConversionAdmission {
        limits,
        prior_work,
        retained_input,
        origin_backing,
    } = admission;
    let mut preflight = Paths {
        root_bytes: root.as_str().len(),
        owners: 0,
        work: prior_work,
        limits,
        failure: None,
    };
    // Prepay the metadata count walk and the one-column name construction.
    preflight.charge(metadata.len())?;
    preflight.charge("path".len())?;
    let count = metadata
        .iter()
        .try_fold(0usize, |n, m| add(n, m.scan_files.data().len(), limits))?;
    // Snapshot admission guarantees a primitive, empty-metadata table schema.
    // Six per-field visits: this validation, owner preflight, Kernel conversion,
    // Vec<Field> collection, FieldRef conversion and Arc-slice relocation.
    // Name bytes are copied once by Field::new. Prepay before inspecting each
    // field, and before entering either conversion walker.
    preflight.charge(1)?; // root Schema construction also occurs with no fields
    for field in schema.fields() {
        preflight.charge(6)?;
        preflight.charge(field.name.len())?;
        if !matches!(field.data_type, DataType::Primitive(_)) || !field.metadata.is_empty() {
            return Err(external(OperationFailure::malformed_response()));
        }
    }
    let schema_peak =
        datafusion_executor::arrow_schema_owner_bytes(schema, limits).map_err(external)?;
    let visitor = ScanMetadataTask::file_visitor_owner_bytes(limits).map_err(external)?;
    // Reservation registration: SharedRegistration contains a pool Arc and
    // MemoryConsumer. The fixed consumer name is owned by its registration.
    let registration = registration_bytes("DeltaFileConversion")
        + size_of::<SharedOwner<super::DeltaTableProvider>>();
    let slots = count
        .checked_mul(size_of::<PartitionedFile>())
        .ok_or_else(|| {
            external(ResourceExhausted {
                resource: Resource::TaskStateBytes,
                limit: limits.limit(Resource::TaskStateBytes),
                observed: usize::MAX,
            })
        })?;
    let fixed = [schema_peak, visitor, registration, slots, origin_backing]
        .into_iter()
        .try_fold(0usize, |n, bytes| add(n, bytes, limits))?;
    check(
        Resource::TaskStateBytes,
        add(retained_input, fixed, limits)?,
        limits,
    )?;
    check(
        Resource::MetadataAllocatedBytes,
        add(retained_input, fixed, limits)?,
        limits,
    )?;
    let lease =
        MemoryConsumer::new("DeltaFileConversion").register(&session.runtime_env().memory_pool);
    lease.try_grow(fixed)?;
    // A single selected column's owned name/map/getter setup is dominated by
    // the complete fixed scan visitor bound already reserved above.
    let columns = [ColumnName::new(["path"])];
    for batch in metadata {
        // The two visitors' fixed column-map and getter walks, including empty
        // batches, are prepaid before either visitor allocates or visits rows.
        preflight.charge(
            ScanMetadataTask::file_visitor_work(batch.scan_files.data().len(), limits)
                .map_err(external)?,
        )?;
        batch
            .scan_files
            .data()
            .visit_rows(&columns, &mut preflight)
            .map_err(external)?;
        if let Some(error) = preflight.failure.take() {
            return Err(error);
        }
    }
    let peak = add(fixed, preflight.owners, limits)?;
    check(
        Resource::TaskStateBytes,
        add(retained_input, peak, limits)?,
        limits,
    )?;
    check(
        Resource::MetadataAllocatedBytes,
        add(retained_input, peak, limits)?,
        limits,
    )?;
    lease.try_grow(peak - fixed)?;
    let mut context = Files {
        root,
        files: Vec::with_capacity(count),
        capacity: count,
        error: None,
    };
    for batch in metadata {
        context = batch.visit_scan_files(context, convert).map_err(external)?;
    }
    if let Some(error) = context.error {
        return Err(error);
    }
    let converted: Schema = schema.try_into_arrow().map_err(external)?;
    Ok(Materialized {
        schema: Arc::new(converted),
        files: context.files,
        lease: Arc::new(lease),
    })
}
struct Paths {
    root_bytes: usize,
    owners: usize,
    work: usize,
    limits: TaskLimits,
    failure: Option<DataFusionError>,
}
impl Paths {
    fn charge(&mut self, units: usize) -> Result<()> {
        let next = self.work.checked_add(units).ok_or_else(|| {
            external(ResourceExhausted {
                resource: Resource::WorkUnits,
                limit: self.limits.limit(Resource::WorkUnits),
                observed: usize::MAX,
            })
        })?;
        check(Resource::WorkUnits, next, self.limits)?;
        self.work = next;
        Ok(())
    }
}
impl RowVisitor for Paths {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        // Names are supplied explicitly to EngineData::visit_rows; no new
        // process-lifetime LazyLock is introduced for this one-column visitor.
        (&[], &[DataType::STRING])
    }
    fn visit<'a>(
        &mut self,
        rows: usize,
        getters: &[&'a dyn GetData<'a>],
    ) -> delta_kernel::DeltaResult<()> {
        for row in 0..rows {
            let result = (|| -> Result<()> {
                let path = getters[0]
                    .get_str(row, "path")
                    .map_err(external)?
                    .ok_or_else(|| external(OperationFailure::malformed_response()))?;
                self.charge(path.len())?; // before join_peak classification
                let join =
                    ScanMetadataTask::file_path_owner_bytes(self.root_bytes, path, self.limits)
                        .map_err(external)?;
                // Joined serialization, percent-decoded path, and Path::parse
                // destination can coexist. Each fits the URL owner bound.
                let owners = join
                    .checked_mul(3)
                    .and_then(|n| n.checked_add(path.len()))
                    .ok_or_else(|| external(OperationFailure::malformed_response()))?;
                self.owners = add(self.owners, owners, self.limits)?;
                // Conversion repeats the already admitted path clone/parser
                // and adds percent decode plus destination path validation.
                self.charge(path.len())?; // independent work-bound classification
                let work = ScanMetadataTask::file_path_work(self.root_bytes, path, self.limits)
                    .map_err(external)?;
                self.charge(work)?;
                // Selected object_store Path::from_url_path/Path::parse walks:
                // prefix scan, percent decode, UTF-8 validation, segment split,
                // PathPart character validation and final raw-path copy.
                // vector_peak<u8>(N) >= 4N, so two join owners cover >=8N
                // byte-work for these six walks, including empty-path setup.
                self.charge(
                    join.checked_mul(2)
                        .ok_or_else(|| external(OperationFailure::malformed_response()))?,
                )?;
                Ok(())
            })();
            if let Err(error) = result {
                self.failure = Some(error);
                break;
            }
        }
        Ok(())
    }
}
struct Files<'a> {
    root: &'a url::Url,
    files: Vec<PartitionedFile>,
    capacity: usize,
    error: Option<DataFusionError>,
}
fn convert(context: &mut Files<'_>, file: ScanFile) {
    if context.error.is_some() {
        return;
    }
    let result = (|| -> Result<PartitionedFile> {
        if file.dv_info.has_vector()
            || file.transform.is_some()
            || !file.partition_values.is_empty()
            || file.stats.is_some()
        {
            return Err(external(delta_kernel::Error::unsupported(
                "unsupported scan file conversion",
            )));
        }
        if context.files.len() == context.capacity {
            return Err(external(OperationFailure::malformed_response()));
        }
        let path = context.root.join(&file.path).map_err(external)?;
        if !path.as_str().starts_with(context.root.as_str())
            || path.query().is_some()
            || path.fragment().is_some()
        {
            return Err(external(OperationFailure::malformed_response()));
        }
        let location = object_store::path::Path::from_url_path(path.path()).map_err(external)?;
        let last_modified = chrono::DateTime::from_timestamp_millis(file.modification_time)
            .ok_or_else(|| external(OperationFailure::malformed_response()))?;
        let size = u64::try_from(file.size).map_err(external)?;
        Ok(PartitionedFile::new_from_meta(object_store::ObjectMeta {
            location,
            last_modified,
            size,
            e_tag: None,
            version: None,
        }))
    })();
    match result {
        Ok(file) => context.files.push(file),
        Err(error) => context.error = Some(error),
    }
}
