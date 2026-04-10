from pathlib import Path

import aiofiles

from src.logger import logger
from src.models.entry import WALEntry, WALStatus, CheckpointEntry


class WALReader:
    def __init__(self, wal_path: str) -> None:
        self._wal_path = Path(wal_path)
    
    def read_entries_sync(self) -> list[WALEntry]:
        entries = list()
        
        if not self._wal_path.exists():
            logger.info(f"[WAL Reader] No WAL file found at {self._wal_path}")
            return entries
        
        with open(self._wal_path, "r", encoding="utf-8") as file:
            for line_num, line in enumerate(file, 1):
                line = line.strip()

                if not line:
                    continue
                
                try:
                    if line.startswith("CHECKPOINT|"):
                        # Skip checkpoint entries for now
                        continue
                    
                    entry = WALEntry.from_log_line(line)
                    entries.append(entry)
                    
                except Exception as e:
                    logger.warning(f"[WAL Reader] Failed to parse line {line_num}: {e}")
                    continue
        
        entries.sort(key=lambda e: e.log_sequence_number)
        
        logger.info(f"[WAL Reader] Read {len(entries)} entries from WAL")
        return entries
    
    async def read_entries(self) -> list[WALEntry]:
        entries = list()
        
        if not self._wal_path.exists():
            logger.info(f"[WAL Reader] No WAL file found at {self._wal_path}")
            return entries
        
        async with aiofiles.open(self._wal_path, "r", encoding="utf-8") as file:
            line_num = 0

            async for line in file:
                line_num += 1
                line = line.strip()

                if not line:
                    continue
                
                try:
                    if line.startswith("CHECKPOINT|"):
                        continue
                    
                    entry = WALEntry.from_log_line(line)
                    entries.append(entry)
                    
                except Exception as e:
                    logger.warning(f"[WAL Reader] Failed to parse line {line_num}: {e}")
                    continue
        
        entries.sort(key=lambda x: x.log_sequence_number)
        
        logger.info(f"[WAL Reader] Read {len(entries)} entries from WAL")
        return entries
    
    def get_last_checkpoint(self) -> CheckpointEntry | None:
        if not self._wal_path.exists():
            return None
        
        last_checkpoint: CheckpointEntry | None = None
        
        with open(self._wal_path, "r", encoding="utf-8") as file:
            for line in file:
                line = line.strip()

                if line.startswith("CHECKPOINT|"):

                    try:
                        last_checkpoint = CheckpointEntry.from_log_line(line)

                    except Exception:
                        continue
        
        return last_checkpoint
    
    def get_last_log_sequence_number(self) -> int:
        if not self._wal_path.exists():
            return 0
        
        last_log_sequence_number = 0
        
        with open(self._wal_path, "r", encoding="utf-8") as file:
            for line in file:
                line = line.strip()

                if not line:
                    continue
                
                try:
                    if line.startswith("CHECKPOINT|"):
                        parts = line.split("|")
                        log_sequence_number = int(parts[1])

                    else:
                        parts = line.split("|", 1)
                        log_sequence_number = int(parts[0])
                    
                    last_log_sequence_number = max(last_log_sequence_number, log_sequence_number)

                except Exception:
                    continue
        
        return last_log_sequence_number

    def get_entries_after_checkpoint(self) -> list[WALEntry]:
        last_checkpoint = self.get_last_checkpoint()
        entries = self.read_entries_sync()
        
        if last_checkpoint is None:
            return entries
        
        return [entry for entry in entries if entry.log_sequence_number > last_checkpoint.last_committed_log_sequence_number]

    def get_entries(self, wal_status: WALStatus) -> list[WALEntry]:
        entries = self.read_entries_sync()
        return [entry for entry in entries if entry.status == wal_status]

