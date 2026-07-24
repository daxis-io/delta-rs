use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_ipc::reader::StreamReader;
use arrow_schema::{DataType, Field, Schema};
use deltalake_browser::BrowserDeltaTable;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use serde_json::json;
use url::Url;

async fn version_zero_table() -> (Url, Arc<dyn ObjectStore>) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("category", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["alpha", "beta", "alpha", "beta"])),
            Arc::new(Int64Array::from(vec![2, 3, 5, 7])),
        ],
    )
    .unwrap();

    let mut parquet = Vec::new();
    let properties = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    let mut writer = ArrowWriter::try_new(&mut parquet, schema, Some(properties)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let schema_string = json!({
        "type": "struct",
        "fields": [
            {
                "name": "category",
                "type": "string",
                "nullable": false,
                "metadata": {}
            },
            {
                "name": "value",
                "type": "long",
                "nullable": false,
                "metadata": {}
            }
        ]
    })
    .to_string();
    let log = [
        json!({"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}}),
        json!({
            "metaData": {
                "id": "00000000-0000-4000-8000-000000000001",
                "format": {"provider": "parquet", "options": {}},
                "schemaString": schema_string,
                "partitionColumns": [],
                "configuration": {},
                "createdTime": 0
            }
        }),
        json!({
            "add": {
                "path": "part-00000.snappy.parquet",
                "partitionValues": {},
                "size": parquet.len(),
                "modificationTime": 0,
                "dataChange": true,
                "stats": "{\"numRecords\":4}"
            }
        }),
        json!({
            "commitInfo": {
                "timestamp": 0,
                "operation": "WRITE"
            }
        }),
    ]
    .into_iter()
    .map(|action| action.to_string())
    .collect::<Vec<_>>()
    .join("\n");

    let store = Arc::new(InMemory::new());
    store
        .put(
            &Path::from("table/_delta_log/00000000000000000000.json"),
            log.into(),
        )
        .await
        .unwrap();
    store
        .put(
            &Path::from("table/part-00000.snappy.parquet"),
            parquet.into(),
        )
        .await
        .unwrap();

    (Url::parse("memory://fixture/table/").unwrap(), store)
}

#[tokio::test(flavor = "current_thread")]
async fn replays_version_zero_and_queries_active_parquet_as_ipc() {
    let (table_root, store) = version_zero_table().await;
    let table = BrowserDeltaTable::open(table_root, store).await.unwrap();
    assert_eq!(table.snapshot_version(), 0);

    let result = table
        .query_ipc(
            "SELECT category, SUM(value) AS total \
             FROM delta GROUP BY category ORDER BY category",
        )
        .await
        .unwrap();

    assert_eq!(result.row_count, 2);
    assert!(result.request_count > 0);
    assert!(result.bytes_fetched > 0);

    let batches = StreamReader::try_new(result.ipc_stream.as_slice(), None)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 2);
    assert_eq!(
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some("alpha"), Some("beta")]
    );
    assert_eq!(
        batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values(),
        &[7, 10]
    );
}
