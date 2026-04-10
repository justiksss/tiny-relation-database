import asyncio
import os
from datetime import datetime
from pathlib import Path
from threading import Lock

import aiofiles

from src.logger import logger
from src.models.entry import WALEntry, WALStatus, CheckpointEntry
from src.engine.wal.reader import WALReader


class LSNGenerator:
    
    def __init__(self, start_log_sequence_number: int = 0) -> None:
        self._current_log_sequence_number = start_log_sequence_number
        self._lock = Lock()
    
    def next(self) -> int:
        with self._lock:
            self._current_log_sequence_number += 1
            return self._current_log_sequence_number
    
    @property
    def current(self) -> int:
        with self._lock:
            return self._current_log_sequence_number


class WALWriter:
    
    def __init__(
        self,
        wal_path: str,
        event_queue: asyncio.Queue[WALEntry],
        batch_size: int = 5,
        flush_interval: float = 0.5,
        fsync: bool = True,
    ) -> None:
        self.wal_path = Path(wal_path)
        self.wal_path.parent.mkdir(parents=True, exist_ok=True)
        
        self.event_queue = event_queue
        
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.fsync = fsync
        
        self._stop_event = asyncio.Event()
        
        reader = WALReader(str(self.wal_path))

        start_log_sequence_number = reader.get_last_log_sequence_number()
        self.lsn_generator = LSNGenerator(start_log_sequence_number)
        
        logger.info(f"[WAL Writer] Initialized with start LSN={start_log_sequence_number}")
    
    def generate_lsn(self) -> int:
        return self.lsn_generator.next()
    
    @property
    def current_lsn(self) -> int:
        return self.lsn_generator.current
    
    async def flush_batch(self, file_handle, batch: list[WALEntry]) -> None:
        lines = [entry.to_log_line() for entry in batch]

        await file_handle.writelines(lines)
        await file_handle.flush()
        
        if self.fsync:
            os.fsync(file_handle.fileno())
    
    async def write_entry(self, entry: WALEntry) -> None:
        async with aiofiles.open(self.wal_path, "a") as file:
            await file.write(entry.to_log_line())
            await file.flush()

            if self.fsync:
                os.fsync(file.fileno())
    
    async def write_checkpoint(self) -> CheckpointEntry:
        checkpoint = CheckpointEntry(
            log_sequence_number=self.lsn_generator.next(),
            timestamp=datetime.now().isoformat(),
            last_committed_log_sequence_number=self.current_lsn - 1,
        )
        
        async with aiofiles.open(self.wal_path, "a") as file:
            await file.write(checkpoint.to_log_line())
            await file.flush()

            if self.fsync:
                os.fsync(file.fileno())
        
        logger.info(f"[WAL Writer] Checkpoint written at LSN={checkpoint.log_sequence_number}")
        return checkpoint
    
    async def truncate_before_checkpoint(self, checkpoint_lsn: int) -> None:
        """
        Remove entries before checkpoint from WAL file.
        
        Creates a new WAL file with only entries after checkpoint.
        """
        reader = WALReader(str(self.wal_path))
        entries = reader.read_entries_sync()
        
        entries_to_keep = [entry for entry in entries if entry.log_sequence_number > checkpoint_lsn]
        
        temp_path = self.wal_path.with_suffix(".tmp")
        
        async with aiofiles.open(temp_path, "w") as f:
            for entry in entries_to_keep:
                await f.write(entry.to_log_line())
        
        os.replace(temp_path, self.wal_path)
        
        logger.info(f"[WAL Writer] Truncated WAL, kept {len(entries_to_keep)} entries")
    
    async def start(self) -> None:
        """
        Start the WAL writer background task.
        
        Batches entries and flushes periodically or when batch is full.
        """
        logger.success("[WAL Writer] Started")
        batch: list[WALEntry] = []
        
        async with aiofiles.open(self.wal_path, "a") as write_ahead_log:
            while not self._stop_event.is_set():
                try:
                    entry = await asyncio.wait_for(
                        self.event_queue.get(),
                        timeout=self.flush_interval
                    )
                    
                    batch.append(entry)
                    self.event_queue.task_done()
                    
                    if len(batch) >= self.batch_size:
                        await self.flush_batch(write_ahead_log, batch)
                        logger.debug(f"[WAL Writer] Flushed {len(batch)} entries (batch full)")
                        batch.clear()
                
                except asyncio.TimeoutError:
                    if batch:
                        await self.flush_batch(write_ahead_log, batch)
                        logger.debug(f"[WAL Writer] Flushed {len(batch)} entries (timeout)")
                        batch.clear()
            
            # flush remaining entries on stop
            if batch:
                await self.flush_batch(write_ahead_log, batch)
                logger.debug(f"[WAL Writer] Flushed remaining {len(batch)} entries on stop")
                batch.clear()
        
        logger.info("[WAL Writer] Stopped")
    
    async def stop(self) -> None:
        """Stop the WAL writer."""
        self._stop_event.set()


class WALManager:
    
    def __init__(
        self,
        wal_path: str,
        batch_size: int = 5,
        flush_interval: float = 0.5,
    ) -> None:
        self.wal_path = wal_path
        self.event_queue: asyncio.Queue[WALEntry] = asyncio.Queue()
        
        self.writer = WALWriter(
            wal_path=wal_path,
            event_queue=self.event_queue,
            batch_size=batch_size,
            flush_interval=flush_interval,
        )
    
    async def log_operation(
        self,
        operation,
        table: str,
        data: dict,
        query: str = "",
    ) -> WALEntry:
        entry = WALEntry(
            log_sequence_number=self.writer.generate_lsn(),
            timestamp=datetime.now().isoformat(),
            operation=operation,
            table=table,
            data=data,
            query=query,
            status=WALStatus.PENDING,
        )
        
        await self.event_queue.put(entry)
        return entry
    
    async def log_committed(
        self,
        original_entry: WALEntry,
    ) -> WALEntry:
        committed_entry = WALEntry(
            log_sequence_number=original_entry.log_sequence_number,
            timestamp=datetime.now().isoformat(),
            operation=original_entry.operation,
            table=original_entry.table,
            data=original_entry.data,
            query=original_entry.query,
            status=WALStatus.COMMITTED,
        )
        
        await self.event_queue.put(committed_entry)
        return committed_entry
    
    async def log_failed(
        self,
        original_entry: WALEntry,
    ) -> WALEntry:
        failed_entry = WALEntry(
            log_sequence_number=original_entry.log_sequence_number,
            timestamp=datetime.now().isoformat(),
            operation=original_entry.operation,
            table=original_entry.table,
            data=original_entry.data,
            query=original_entry.query,
            status=WALStatus.FAILED,
        )
        
        await self.event_queue.put(failed_entry)
        return failed_entry
    
    async def checkpoint(self) -> CheckpointEntry:
        return await self.writer.write_checkpoint()
    
    async def start(self) -> None:
        await self.writer.start()
    
    async def stop(self) -> None:
        await self.writer.stop()
