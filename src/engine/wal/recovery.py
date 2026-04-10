from pathlib import Path

from src.logger import logger

from src.models.entry import WALEntry, WALStatus
from src.models.enums.operations import DatabaseOperationsEnum

from src.engine.wal.reader import WALReader
from src.engine.table_storage import TableStorage

from src.models.command import (
    CreateTableCommand,
    AddCommand,
    UpdateCommand,
    DeleteCommand,
)

class WALRecovery:
    MODIFYING_OPERATIONS = {
        DatabaseOperationsEnum.CREATE_TABLE,
        DatabaseOperationsEnum.ADD,
        DatabaseOperationsEnum.UPDATE,
        DatabaseOperationsEnum.DELETE,
    }
    
    def __init__(self, wal_path: str, storage: TableStorage) -> None:
        self.wal_reader = WALReader(wal_path)
        self.storage = storage
        self._recovered_log_sequence_number = 0

        self._operation_handlers: dict[DatabaseOperationsEnum, callable] = {
            DatabaseOperationsEnum.ADD: self._replay_add_operation,
            DatabaseOperationsEnum.UPDATE: self._replay_update_operation,
            DatabaseOperationsEnum.DELETE: self._replay_delete_operation,
            DatabaseOperationsEnum.CREATE_TABLE: self._replay_create_table_operation,
        }
    
    @property
    def last_recovered_log_sequence_number(self) -> int:
        return self._recovered_log_sequence_number

    async def _replay_add_operation(self, entry: WALEntry) -> None:
        command = AddCommand(
            table=entry.table,
            columns=entry.data.get("columns", []),
            values=entry.data.get("values", []),
        )
        await self.storage.add(command)

    async def _replay_update_operation(self, entry: WALEntry) -> None:
        command = UpdateCommand(
            table=entry.table,
            assignments=entry.data.get("assignments", {}),
            where=entry.data.get("where"),
        )
        await self.storage.update(command)

    async def _replay_delete_operation(self, entry: WALEntry) -> None:
        command = DeleteCommand(
            table=entry.table,
            where=entry.data.get("where"),
        )
        await self.storage.delete(command)

    async def _replay_create_table_operation(self, entry: WALEntry) -> None:
        command = CreateTableCommand(
                table=entry.table,
                columns=entry.data.get("columns", {}),
            )
        try:
            await self.storage.create_table(command)
        except ValueError as e:
            if "already exists" in str(e):
                logger.debug(f"[Recovery] Table '{entry.table}' already exists, skipping")
            else:
                raise

    async def _replay_entry(self, entry: WALEntry) -> None:
        handler = self._operation_handlers.get(entry.operation)

        if handler is None:
            raise ValueError(f"Unknown operation type: {entry.operation}")

        await handler(entry)
    
    async def recover(self, from_checkpoint: bool = True) -> int:
        if from_checkpoint:
            entries = self.wal_reader.get_entries_after_checkpoint()
        else:
            entries = self.wal_reader.get_entries(WALStatus.COMMITTED)
        
        if not entries:
            logger.info("[Recovery] No entries to recover")
            return 0
        
        logger.info(f"[Recovery] Starting recovery of {len(entries)} entries...")
        
        recovered_count = 0
        
        for entry in entries:

            if entry.operation not in self.MODIFYING_OPERATIONS:
                continue
            
            if entry.status != WALStatus.COMMITTED:
                logger.debug(f"[Recovery] Skipping non-committed entry LSN={entry.log_sequence_number}")
                continue
            
            try:
                await self._replay_entry(entry)
                self._recovered_log_sequence_number = entry.log_sequence_number

                recovered_count += 1
                logger.debug(f"[Recovery] Replayed LSN={entry.log_sequence_number} {entry.operation}")
                
            except Exception as e:
                logger.error(f"[Recovery] Failed to replay LSN={entry.log_sequence_number}: {e}")
                continue
        
        logger.success(f"[Recovery] Completed. Replayed {recovered_count} entries")
        return recovered_count
    
    def verify_integrity(self) -> tuple[bool, list[str]]:
        issues = list()
        
        try:
            entries = self.wal_reader.read_entries_sync()
        except Exception as e:
            return False, [f"Failed to read WAL: {e}"]
        
        if not entries:
            return True, []
        
        previous_log_sequence_number = 0

        for entry in entries:

            if entry.log_sequence_number <= previous_log_sequence_number:
                issues.append(f"Log sequence number out of order: {entry.log_sequence_number} <= {previous_log_sequence_number}")

            previous_log_sequence_number = entry.log_sequence_number
        
        pending = [entry for entry in entries if entry.status == WALStatus.PENDING]

        if pending:
            issues.append(f"Found {len(pending)} pending entries (incomplete transactions)")
        
        is_valid = len(issues) == 0
        return is_valid, issues

