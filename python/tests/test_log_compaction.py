import pathlib

import pytest
from arro3.core import Table

from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import DeltaError


def test_log_compaction_rejected(tmp_path: pathlib.Path, sample_table: Table):
    table_path = tmp_path / "path" / "to" / "table"
    for _ in range(4):
        write_deltalake(table_path, sample_table, mode="append")

    table = DeltaTable(table_path)
    protocol = table.protocol()
    objects = {
        p.relative_to(table_path): p.read_bytes()
        for p in table_path.rglob("*")
        if p.is_file()
    }
    # The pinned Kernel disables compaction until its correctness testing is complete.
    with pytest.raises(
        DeltaError, match="Unsupported: Log compaction is not currently supported"
    ):
        table.compact_logs(starting_version=0, ending_version=3)
    reloaded = DeltaTable(table_path)
    assert table.version() == reloaded.version() == 3
    assert table.protocol() == reloaded.protocol() == protocol
    assert {
        p.relative_to(table_path): p.read_bytes()
        for p in table_path.rglob("*")
        if p.is_file()
    } == objects
