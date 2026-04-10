import asyncio
import json
import os
import struct
from pathlib import Path

import aiofiles
import msgpack

from src.models.command import CreateTableCommand, AddCommand, GetCommand, UpdateCommand, DeleteCommand


class TableStorage:
    __TYPE_MAPPING = {
        "INTEGER": int,
        "FLOAT": float,
        "BOOLEAN": bool,
        "TEXT": str,
    }

    def __init__(self, base_directory: str = "../../database") -> None:
        self._base_directory = Path(base_directory)
        self._base_directory.mkdir(exist_ok=True)
        self._table_locks: dict[str, asyncio.Lock] = {}
        self._dict_lock = asyncio.Lock()

    async def _get_table_lock(self, table: str) -> asyncio.Lock:
        async with self._dict_lock:
            if table not in self._table_locks:
                self._table_locks[table] = asyncio.Lock()
            return self._table_locks[table]

    def _schema_path(self, table: str) -> Path:
        return self._base_directory / f"{table}.schema.json"

    def _data_path(self, table: str) -> Path:
        return self._base_directory / f"{table}.data.bin"

    def _serialize_row(self, row: dict) -> bytes:
        payload = msgpack.packb(row, use_bin_type=True)
        return struct.pack(">I", len(payload)) + payload

    def _deserialize_row(self, data: bytes) -> dict:
        return msgpack.unpackb(data, raw=False)

    async def _load_schema(self, table: str) -> dict:
        schema_path = self._schema_path(table)
        if not schema_path.exists():
            raise ValueError(f"Table '{table}' does not exist")

        async with aiofiles.open(schema_path, "r") as f:
            text = await f.read()
            return json.loads(text)

    async def _validate_row(self, table: str, row: dict):
        schema = await self._load_schema(table)

        for column, col_type in schema["columns"].items():
            if column not in row:
                raise ValueError(f"Missing column '{column}' in inserted row")

            caster = self.__TYPE_MAPPING.get(col_type)

            if caster is None:
                raise ValueError(f"Unsupported type: {col_type}")

            try:
                row[column] = caster(row[column])
            except (ValueError, TypeError):
                raise ValueError(f"Invalid value for column '{column}' type '{col_type}'")

        return None

    async def create_table(self, command: CreateTableCommand) -> None:
        lock = await self._get_table_lock(command.table)

        async with lock:
            schema_path = self._schema_path(command.table)
            data_path = self._data_path(command.table)

            if schema_path.exists():
                raise ValueError(f"Table '{command.table}' already exists")

            async with aiofiles.open(schema_path, "w") as f:
                await f.write(json.dumps({"columns": command.columns}, ensure_ascii=False, indent=2))

            async with aiofiles.open(data_path, "wb") as f:
                await f.write(b"")  # create empty file

    async def add(self, command: AddCommand) -> None:
        lock = await self._get_table_lock(command.table)

        async with lock:
            row = dict(zip(command.columns, command.values))

            for key, value in row.items():
                if isinstance(value, str) and value.startswith('"') and value.endswith('"'):
                    row[key] = value[1:-1]

            await self._validate_row(command.table, row)

            async with aiofiles.open(self._data_path(command.table), "ab") as f:
                await f.write(self._serialize_row(row))

    async def get(self, command: GetCommand) -> list[dict]:
        lock = await self._get_table_lock(command.table)

        async with lock:
            results = list()

            data_path = self._data_path(command.table)

            if not data_path.exists():
                raise ValueError(f"Table '{command.table}' does not exist")

            async with aiofiles.open(data_path, "rb") as f:
                while True:
                    header = await f.read(4)
                    if len(header) < 4:
                        break
                    length = struct.unpack(">I", header)[0]
                    payload = await f.read(length)
                    if len(payload) < length:
                        break
                    row = self._deserialize_row(payload)

                    if command.where:
                        if not eval(command.where, {}, row):
                            continue

                    if command.columns == ["*"]:
                        results.append(row)
                    else:
                        results.append({col: row.get(col) for col in command.columns})

            return results

    async def update(self, command: UpdateCommand) -> None:
        lock = await self._get_table_lock(command.table)

        async with lock:
            data_path = self._data_path(command.table)
            temp_path = data_path.with_suffix(".tmp")

            async with aiofiles.open(data_path, "rb") as src, aiofiles.open(temp_path, "wb") as dst:
                while True:
                    header = await src.read(4)
                    if len(header) < 4:
                        break
                    length = struct.unpack(">I", header)[0]
                    payload = await src.read(length)
                    if len(payload) < length:
                        break
                    row = self._deserialize_row(payload)

                    if command.where and not eval(command.where, {}, row):
                        await dst.write(self._serialize_row(row))
                        continue

                    for key, value in command.assignments.items():
                        if isinstance(value, str) and value.startswith('"') and value.endswith('"'):
                            value = value[1:-1]
                        row[key] = value

                    await self._validate_row(command.table, row)
                    await dst.write(self._serialize_row(row))

            os.replace(temp_path, data_path)

    async def delete(self, command: DeleteCommand) -> None:
        lock = await self._get_table_lock(command.table)

        async with lock:
            data_path = self._data_path(command.table)
            temp_path = data_path.with_suffix(".tmp")

            async with aiofiles.open(data_path, "rb") as src, aiofiles.open(temp_path, "wb") as dst:
                while True:
                    header = await src.read(4)
                    if len(header) < 4:
                        break
                    length = struct.unpack(">I", header)[0]
                    payload = await src.read(length)
                    if len(payload) < length:
                        break
                    row = self._deserialize_row(payload)

                    if command.where and eval(command.where, {}, row):
                        continue  # skip → delete

                    await dst.write(self._serialize_row(row))

            os.replace(temp_path, data_path)
