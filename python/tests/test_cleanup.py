import pathlib

import pytest
from arro3.core import Array, DataType, Table
from arro3.core import Field as ArrowField

from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import DeltaError


def valid_gc_data(version) -> Table:
    id_col = ArrowField("id", DataType.int32(), nullable=True)
    gc = ArrowField("gc", DataType.int32(), nullable=True).with_metadata(
        {"delta.generationExpression": "10"}
    )
    return Table.from_pydict(
        {"id": Array([version, version], type=id_col), "gc": Array([10, 10], type=gc)},
    )


def test_generated_columns_checkpoint_rejected(tmp_path: pathlib.Path):
    write_deltalake(
        tmp_path, valid_gc_data(0), configuration={"delta.minWriterVersion": "7"}
    )
    table = DeltaTable(tmp_path)
    objects = {
        p.relative_to(tmp_path): p.read_bytes()
        for p in tmp_path.rglob("*")
        if p.is_file()
    }
    with pytest.raises(
        DeltaError, match="Unsupported: Feature 'generatedColumns' is not supported"
    ):
        table.create_checkpoint()
    assert table.version() == DeltaTable(tmp_path).version() == 0
    assert {
        p.relative_to(tmp_path): p.read_bytes()
        for p in tmp_path.rglob("*")
        if p.is_file()
    } == objects


@pytest.mark.pandas
def test_cleanup_from_old_snapshot_preserves_logs(tmp_path: pathlib.Path):
    for i in range(10):
        data = Table.from_pydict(
            {
                "id": Array([i, i], DataType.int32()),
                "gc": Array([10, 10], DataType.int32()),
            }
        )
        write_deltalake(
            tmp_path,
            mode="overwrite",
            data=data,
            configuration={"delta.logRetentionDuration": "interval 0 day"},
        )

    DeltaTable(tmp_path).create_checkpoint()
    log_path = tmp_path / "_delta_log"
    logs = {p.name: p.read_bytes() for p in log_path.iterdir() if p.is_file()}
    # Checkpoint 9 cannot establish a deletion boundary for snapshot 5.
    DeltaTable(tmp_path, version=5).cleanup_metadata()
    assert {p.name: p.read_bytes() for p in log_path.iterdir() if p.is_file()} == logs
    assert DeltaTable(tmp_path, version=5).to_pandas().to_dict("list") == {
        "id": [5, 5],
        "gc": [10, 10],
    }
    assert DeltaTable(tmp_path).to_pandas().to_dict("list") == {
        "id": [9, 9],
        "gc": [10, 10],
    }
