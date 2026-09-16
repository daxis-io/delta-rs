//! This module contains helper functions to create a LazyTableProvider from an ArrowArrayStreamReader

use std::any::Any;
use std::fmt::{self};
use std::sync::{Arc, Mutex};

use arrow_schema::SchemaRef;
use deltalake::arrow::array::RecordBatchReader;
use deltalake::arrow::error::ArrowError;
use deltalake::arrow::error::Result as ArrowResult;
use deltalake::arrow::record_batch::RecordBatch;
use deltalake::datafusion::catalog::TableProvider;
use deltalake::datafusion::physical_plan::memory::LazyBatchGenerator;
use deltalake::kernel::schema::cast_record_batch;
use parking_lot::RwLock;

use crate::DeltaResult;
use crate::datafusion::LazyTableProvider;

/// Convert an [ArrowArrayStreamReader] into a [LazyTableProvider]
pub fn to_lazy_table(
    source: Box<dyn RecordBatchReader + Send + 'static>,
) -> DeltaResult<Arc<dyn TableProvider>> {
    let schema = source.schema();
    let arrow_stream_batch_generator: Arc<RwLock<dyn LazyBatchGenerator>> =
        Arc::new(RwLock::new(ArrowStreamBatchGenerator::new(source)));

    Ok(Arc::new(LazyTableProvider::try_new(
        schema.clone(),
        vec![arrow_stream_batch_generator],
    )?))
}
pub struct ReaderWrapper {
    reader: Mutex<Option<Box<dyn RecordBatchReader + Send + 'static>>>,
}

impl fmt::Debug for ReaderWrapper {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReaderWrapper")
            .field("reader", &"<RecordBatchReader>")
            .finish()
    }
}

#[derive(Debug)]
pub struct ArrowStreamBatchGenerator {
    pub array_stream: ReaderWrapper,
    started: bool,
}

impl fmt::Display for ArrowStreamBatchGenerator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ArrowStreamBatchGenerator {{ array_stream: {:?} }}",
            self.array_stream
        )
    }
}

impl ArrowStreamBatchGenerator {
    pub fn new(array_stream: Box<dyn RecordBatchReader + Send + 'static>) -> Self {
        Self {
            array_stream: ReaderWrapper {
                reader: Mutex::new(Some(array_stream)),
            },
            started: false,
        }
    }
}

impl LazyBatchGenerator for ArrowStreamBatchGenerator {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn generate_next_batch(
        &mut self,
    ) -> deltalake::datafusion::error::Result<Option<deltalake::arrow::array::RecordBatch>> {
        self.started = true;
        let mut stream_reader = self.array_stream.reader.lock().map_err(|_| {
            deltalake::datafusion::error::DataFusionError::Execution(STREAM_LOCK_ERROR.to_string())
        })?;
        let stream_reader = stream_reader.as_mut().ok_or_else(|| {
            deltalake::datafusion::error::DataFusionError::Execution(
                STREAM_CONSUMED_ERROR.to_string(),
            )
        })?;

        match stream_reader.next() {
            Some(Ok(record_batch)) => Ok(Some(record_batch)),
            Some(Err(err)) => Err(deltalake::datafusion::error::DataFusionError::ArrowError(
                Box::new(err),
                None,
            )),
            None => Ok(None), // End of stream
        }
    }

    fn reset_state(&self) -> Arc<RwLock<dyn LazyBatchGenerator>> {
        match self.array_stream.reader.lock() {
            Ok(mut reader) => {
                if !self.started
                    && let Some(reader) = reader.take()
                {
                    return Arc::new(RwLock::new(Self::new(reader)));
                }
            }
            Err(_) => {
                return Arc::new(RwLock::new(ExhaustedStreamGenerator(STREAM_LOCK_ERROR)));
            }
        }
        Arc::new(RwLock::new(ExhaustedStreamGenerator(STREAM_CONSUMED_ERROR)))
    }
}

const STREAM_LOCK_ERROR: &str = "Failed to lock the ArrowArrayStreamReader";
const STREAM_CONSUMED_ERROR: &str = "Stream-based generator cannot be reset; the original stream has \
    been consumed. Buffer input data if plan re-execution is required.";

/// Carries a stream admission error through the infallible reset interface.
#[derive(Debug)]
struct ExhaustedStreamGenerator(&'static str);

impl std::fmt::Display for ExhaustedStreamGenerator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ExhaustedStreamGenerator")
    }
}

impl LazyBatchGenerator for ExhaustedStreamGenerator {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn generate_next_batch(
        &mut self,
    ) -> deltalake::datafusion::error::Result<Option<deltalake::arrow::array::RecordBatch>> {
        Err(deltalake::datafusion::error::DataFusionError::Execution(
            self.0.to_string(),
        ))
    }

    fn reset_state(&self) -> Arc<RwLock<dyn LazyBatchGenerator>> {
        Arc::new(RwLock::new(ExhaustedStreamGenerator(self.0)))
    }
}

/// A lazy casting wrapper around a RecordBatchReader
struct LazyCastReader {
    input: Box<dyn RecordBatchReader + Send + 'static>,
    target_schema: SchemaRef,
}

impl RecordBatchReader for LazyCastReader {
    fn schema(&self) -> SchemaRef {
        self.target_schema.clone()
    }
}

impl Iterator for LazyCastReader {
    type Item = ArrowResult<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.input.next() {
            Some(Ok(batch)) => Some(
                cast_record_batch(&batch, self.target_schema.clone(), false, false)
                    .map_err(|e| ArrowError::CastError(e.to_string())),
            ),
            Some(Err(e)) => Some(Err(e)),
            None => None,
        }
    }
}

/// Returns a boxed reader that lazily casts each batch to the provided schema.
pub fn maybe_lazy_cast_reader(
    input: Box<dyn RecordBatchReader + Send + 'static>,
    target_schema: SchemaRef,
) -> Box<dyn RecordBatchReader + Send + 'static> {
    if !input.schema().eq(&target_schema) {
        Box::new(LazyCastReader {
            input,
            target_schema,
        })
    } else {
        input
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use deltalake::arrow::array::Int64Array;
    use deltalake::arrow::datatypes::{DataType, Field, Schema};
    use deltalake::arrow::record_batch::RecordBatchIterator;
    use deltalake::datafusion::execution::TaskContext;
    use deltalake::datafusion::physical_plan::ExecutionPlan;
    use deltalake::datafusion::physical_plan::memory::LazyMemoryExec;
    use futures::TryStreamExt;

    #[tokio::test]
    async fn arrow_stream_first_execution_transfers_reader_and_rejects_replay()
    -> deltalake::datafusion::error::Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let expected = vec![
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1, 2]))])?,
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![3, 4]))])?,
        ];
        let reader = || {
            Box::new(RecordBatchIterator::new(
                expected.clone().into_iter().map(Ok),
                schema.clone(),
            ))
        };
        let generator = Arc::new(RwLock::new(ArrowStreamBatchGenerator::new(reader())));
        let plan = Arc::new(LazyMemoryExec::try_new(schema.clone(), vec![generator])?);
        let reset_plan = plan.clone().reset_state()?;
        let first = reset_plan.execute(0, Arc::new(TaskContext::default()))?;
        let mut competing = reset_plan.execute(0, Arc::new(TaskContext::default()))?;
        assert!(
            competing
                .try_next()
                .await
                .unwrap_err()
                .to_string()
                .contains("original stream has been consumed")
        );
        assert_eq!(first.try_collect::<Vec<_>>().await?, expected);
        let mut replay = reset_plan.execute(0, Arc::new(TaskContext::default()))?;
        assert!(
            replay
                .try_next()
                .await
                .unwrap_err()
                .to_string()
                .contains("original stream has been consumed")
        );

        let mut partial = ArrowStreamBatchGenerator::new(reader());
        assert_eq!(partial.generate_next_batch()?, Some(expected[0].clone()));
        assert!(
            partial
                .reset_state()
                .write()
                .generate_next_batch()
                .unwrap_err()
                .to_string()
                .contains("original stream has been consumed")
        );
        assert_eq!(partial.generate_next_batch()?, Some(expected[1].clone()));
        assert!(partial.generate_next_batch()?.is_none());

        let mut poisoned = ArrowStreamBatchGenerator::new(reader());
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _guard = poisoned.array_stream.reader.lock().unwrap();
                panic!("poison the reader mutex");
            }))
            .is_err()
        );
        assert!(matches!(
            poisoned.generate_next_batch().unwrap_err(),
            deltalake::datafusion::error::DataFusionError::Execution(message)
                if message == STREAM_LOCK_ERROR
        ));
        let error_generator = poisoned.reset_state();
        assert!(matches!(
            error_generator.write().generate_next_batch().unwrap_err(),
            deltalake::datafusion::error::DataFusionError::Execution(message)
                if message == STREAM_LOCK_ERROR
        ));
        let repeated = error_generator.read().reset_state();
        assert!(matches!(
            repeated.write().generate_next_batch().unwrap_err(),
            deltalake::datafusion::error::DataFusionError::Execution(message)
                if message == STREAM_LOCK_ERROR
        ));
        Ok(())
    }
}
