import json
import struct
import pytest
from pathlib import Path

import msgpack

from src.engine.table_storage import TableStorage
from src.models.command import (
    CreateTableCommand,
    AddCommand,
    GetCommand,
    UpdateCommand,
    DeleteCommand,
)

pytestmark = pytest.mark.asyncio


@pytest.fixture
async def storage(tmp_path: Path):
    """Give each test its own isolated directory."""
    store = TableStorage(base_directory=str(tmp_path))
    return store


async def test_create_table(storage: TableStorage):
    cmd = CreateTableCommand(
        table="users",
        columns={
            "id": "INTEGER",
            "name": "TEXT",
            "active": "BOOLEAN",
        }
    )

    await storage.create_table(cmd)

    schema_file = Path(storage._schema_path("users"))
    data_file = Path(storage._data_path("users"))

    assert schema_file.exists()
    assert data_file.exists()

    schema = json.loads(schema_file.read_text())
    assert schema == {
        "columns": {
            "id": "INTEGER",
            "name": "TEXT",
            "active": "BOOLEAN"
        }
    }

    assert data_file.suffix == ".bin"
    assert data_file.name == "users.data.bin"
    assert data_file.read_bytes() == b""


async def test_add_and_get(storage: TableStorage):
    # create table
    await storage.create_table(
        CreateTableCommand(
            table="users",
            columns={"id": "INTEGER", "name": "TEXT"}
        )
    )

    # insert row
    await storage.add(
        AddCommand(
            table="users",
            columns=["id", "name"],
            values=["1", '"Anton"']
        )
    )

    rows = await storage.get(
        GetCommand(
            table="users",
            columns=["id", "name"]
        )
    )

    assert len(rows) == 1
    assert rows[0] == {"id": 1, "name": "Anton"}


async def test_get_with_where(storage: TableStorage):
    await storage.create_table(
        CreateTableCommand(
            table="users",
            columns={"id": "INTEGER", "name": "TEXT"}
        )
    )

    await storage.add(AddCommand("users", ["id", "name"], ["1", '"A"']))
    await storage.add(AddCommand("users", ["id", "name"], ["2", '"B"']))

    rows = await storage.get(
        GetCommand(
            table="users",
            columns=["id", "name"],
            where="id == 2"
        )
    )

    assert len(rows) == 1
    assert rows[0]["id"] == 2


async def test_update(storage: TableStorage):
    await storage.create_table(
        CreateTableCommand("users", {"id": "INTEGER", "name": "TEXT"})
    )

    await storage.add(AddCommand("users", ["id", "name"], ["1", '"Old"']))

    await storage.update(
        UpdateCommand(
            table="users",
            assignments={"name": '"New"'},
            where="id == 1"
        )
    )

    rows = await storage.get(GetCommand("users", ["id", "name"]))

    assert rows[0]["name"] == "New"


async def test_update_only_matching(storage: TableStorage):
    await storage.create_table(
        CreateTableCommand("users", {"id": "INTEGER", "name": "TEXT"})
    )

    await storage.add(AddCommand("users", ["id", "name"], ["1", '"One"']))
    await storage.add(AddCommand("users", ["id", "name"], ["2", '"Two"']))

    await storage.update(
        UpdateCommand(
            table="users",
            assignments={"name": '"Updated"'},
            where="id == 2"
        )
    )

    rows = await storage.get(GetCommand("users", ["id", "name"]))

    assert rows[0]["name"] == "One"      # unchanged
    assert rows[1]["name"] == "Updated"  # updated


async def test_delete(storage: TableStorage):
    await storage.create_table(
        CreateTableCommand("users", {"id": "INTEGER", "name": "TEXT"})
    )

    await storage.add(AddCommand("users", ["id", "name"], ["1", '"Test"']))
    await storage.add(AddCommand("users", ["id", "name"], ["2", '"Remove"']))

    await storage.delete(
        DeleteCommand(
            table="users",
            where="id == 2"
        )
    )

    rows = await storage.get(GetCommand("users", ["id", "name"]))

    assert len(rows) == 1
    assert rows[0]["id"] == 1


async def test_type_validation(storage: TableStorage):
    await storage.create_table(
        CreateTableCommand("users", {"id": "INTEGER", "name": "TEXT"})
    )

    with pytest.raises(ValueError):
        # id cannot be TEXT
        await storage.add(
            AddCommand(
                table="users",
                columns=["id", "name"],
                values=['"abc"', '"John"']
            )
        )


async def test_serialize_deserialize_roundtrip(storage: TableStorage):
    """Row serialized with _serialize_row and deserialized with _deserialize_row matches original."""
    row = {"id": 1, "name": "Test", "flag": True, "score": 3.14}
    packed = storage._serialize_row(row)
    assert isinstance(packed, bytes)
    assert len(packed) >= 4
    length = struct.unpack(">I", packed[:4])[0]
    assert length == len(packed) - 4
    restored = storage._deserialize_row(packed[4:])
    assert restored == row


async def test_data_file_is_length_prefixed_msgpack(storage: TableStorage):
    """After add, data file contains length-prefixed msgpack records (not JSONL)."""
    await storage.create_table(
        CreateTableCommand(table="t", columns={"id": "INTEGER", "name": "TEXT"})
    )
    await storage.add(AddCommand("t", ["id", "name"], ["1", '"A"']))

    data_file = Path(storage._data_path("t"))
    raw = data_file.read_bytes()
    assert raw[:4] != b"[{"  # not JSON array
    assert raw[:1] != b"{"  # not JSON object

    length = struct.unpack(">I", raw[:4])[0]
    assert length <= len(raw) - 4
    payload = raw[4 : 4 + length]
    row = msgpack.unpackb(payload, raw=False)
    assert row["id"] == 1
    assert row["name"] == "A"


async def test_get_returns_native_types(storage: TableStorage):
    """GET returns rows with Python types (int, float, bool, str) after msgpack round-trip."""
    await storage.create_table(
        CreateTableCommand(
            table="typed",
            columns={"id": "INTEGER", "score": "FLOAT", "active": "BOOLEAN", "label": "TEXT"},
        )
    )
    await storage.add(
        AddCommand("typed", ["id", "score", "active", "label"], ["42", "2.5", "true", '"hello"'])
    )

    rows = await storage.get(GetCommand(table="typed", columns=["*"]))
    assert len(rows) == 1
    assert rows[0]["id"] == 42
    assert rows[0]["score"] == 2.5
    assert rows[0]["active"] is True
    assert rows[0]["label"] == "hello"
