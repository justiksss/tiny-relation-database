import asyncio
from typing import AsyncGenerator, Any

import pytest
from datetime import datetime
from pathlib import Path

from src.engine.wal.writer import WALWriter
from src.models.entry import WALEntry, WALStatus
from src.models.enums.operations import DatabaseOperationsEnum


@pytest.fixture
def sample_entries() -> list[WALEntry]:
    now = datetime.now().isoformat()
    return [
        WALEntry(
            log_sequence_number=i + 1,
            timestamp=now,
            operation=DatabaseOperationsEnum.ADD,
            table="test_table",
            data={"value": i},
            status=WALStatus.COMMITTED,
            query=f"add test_table values ({i})",
        )
        for i in range(5)
    ]


@pytest.fixture
def event_queue() -> asyncio.Queue:
    return asyncio.Queue()


@pytest.fixture
def wal_file(tmp_path: Path) -> Path:
    return tmp_path / "wal_test.log"


@pytest.fixture
async def running_wal_writer(wal_file, event_queue) -> AsyncGenerator[WALWriter, Any]:
    wal_writer = WALWriter(
        wal_path=str(wal_file),
        event_queue=event_queue,
        batch_size=3,
        flush_interval=0.1,
        fsync=False,
    )
    task = asyncio.create_task(wal_writer.start())
    yield wal_writer
    await wal_writer.stop()
    await task
