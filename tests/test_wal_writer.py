import asyncio
import pytest
from datetime import datetime

from src.engine.wal.writer import WALWriter, LSNGenerator
from src.engine.wal.reader import WALReader
from src.models.entry import WALEntry, WALStatus
from src.models.enums.operations import DatabaseOperationsEnum


@pytest.mark.wal
@pytest.mark.asyncio
class TestWALWriter:
    async def test_flush_batch(self, wal_file, event_queue, sample_entries, running_wal_writer):
        for entry in sample_entries:
            await event_queue.put(entry)

        await asyncio.sleep(0.3)

        content = wal_file.read_text()
        lines = content.strip().split("\n")

        assert len(lines) == len(sample_entries)

        for line, entry in zip(lines, sample_entries):
            assert entry.table in line
            assert entry.operation.value in line
            assert str(entry.log_sequence_number) in line

    async def test_flush_after_timeout(self, wal_file, event_queue):
        wal_writer = WALWriter(
            wal_path=str(wal_file),
            event_queue=event_queue,
            batch_size=5,
            flush_interval=0.1,
            fsync=False,
        )

        task = asyncio.create_task(wal_writer.start())

        now = datetime.now().isoformat()
        entries = [
            WALEntry(
                log_sequence_number=i + 1,
                timestamp=now,
                operation=DatabaseOperationsEnum.ADD,
                table="timeout_table",
                data={"value": i},
                status=WALStatus.COMMITTED,
            )
            for i in range(2)
        ]

        for entry in entries:
            await event_queue.put(entry)

        await asyncio.sleep(0.2)

        await wal_writer.stop()
        await task

        content = wal_file.read_text()
        lines = content.strip().split("\n")

        assert len(lines) == 2

        for line in lines:
            assert "timeout_table" in line

    async def test_flush_remaining_on_stop(self, wal_file, event_queue):
        wal_writer = WALWriter(
            wal_path=str(wal_file),
            event_queue=event_queue,
            batch_size=10,
            flush_interval=1.0,
            fsync=False,
        )

        task = asyncio.create_task(wal_writer.start())

        now = datetime.now().isoformat()
        entries = [
            WALEntry(
                log_sequence_number=i + 1,
                timestamp=now,
                operation=DatabaseOperationsEnum.ADD,
                table="remaining_table",
                data={"value": i},
                status=WALStatus.COMMITTED,
            )
            for i in range(3)
        ]

        for entry in entries:
            await event_queue.put(entry)

        await asyncio.sleep(0.1)
        await wal_writer.stop()
        await task

        content = wal_file.read_text()
        lines = content.strip().split("\n")

        assert len(lines) == 3

        for line in lines:
            assert "remaining_table" in line


@pytest.mark.wal
class TestLSNGenerator:
    def test_lsn_starts_from_specified_value(self):
        gen = LSNGenerator(start_log_sequence_number=100)
        assert gen.current == 100
        assert gen.next() == 101
        assert gen.next() == 102

    def test_lsn_starts_from_zero_by_default(self):
        gen = LSNGenerator()
        assert gen.current == 0
        assert gen.next() == 1

    def test_lsn_is_monotonically_increasing(self):
        gen = LSNGenerator()
        prev = 0
        for _ in range(100):
            current = gen.next()
            assert current > prev
            prev = current


@pytest.mark.wal
class TestWALReader:
    @pytest.mark.asyncio
    async def test_read_entries(self, wal_file, event_queue, sample_entries, running_wal_writer):
        for entry in sample_entries:
            await event_queue.put(entry)

        await asyncio.sleep(0.3)
        await running_wal_writer.stop()

        reader = WALReader(str(wal_file))
        entries = reader.read_entries_sync()

        assert len(entries) == len(sample_entries)
        
        for read_entry, original_entry in zip(entries, sample_entries):
            assert read_entry.log_sequence_number == original_entry.log_sequence_number
            assert read_entry.table == original_entry.table
            assert read_entry.operation == original_entry.operation

    @pytest.mark.asyncio
    async def test_get_last_log_sequence_number(self, wal_file, event_queue, sample_entries, running_wal_writer):
        for entry in sample_entries:
            await event_queue.put(entry)

        await asyncio.sleep(0.3)
        await running_wal_writer.stop()

        reader = WALReader(str(wal_file))
        last_lsn = reader.get_last_log_sequence_number()

        assert last_lsn == 5

    def test_empty_wal_file(self, wal_file):
        wal_file.touch()
        
        reader = WALReader(str(wal_file))
        entries = reader.read_entries_sync()
        
        assert entries == []
        assert reader.get_last_log_sequence_number() == 0

    def test_nonexistent_wal_file(self, tmp_path):
        reader = WALReader(str(tmp_path / "nonexistent.log"))
        entries = reader.read_entries_sync()
        
        assert entries == []
        assert reader.get_last_log_sequence_number() == 0
