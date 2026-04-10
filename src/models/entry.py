from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any
import json

from src.models.enums.operations import DatabaseOperationsEnum


class WALStatus(StrEnum):
    """Status of WAL entry for transaction support."""
    PENDING = "PENDING"
    COMMITTED = "COMMITTED"
    FAILED = "FAILED"

@dataclass
class WALEntry:
    log_sequence_number: int  # monotonically increasing

    timestamp: str
    operation: DatabaseOperationsEnum

    table: str
    
    data: dict[str, Any] = field(default_factory=dict)
    
    status: WALStatus = WALStatus.PENDING
    
    query: str = ""
    
    def to_log_line(self) -> str:
        return (
            f"{self.log_sequence_number}|{self.timestamp}|{self.operation.value}|"
            f"{self.status.value}|{self.table}|{json.dumps(self.data, ensure_ascii=False)}|"
            f"{self.query}\n"
        )
    
    @classmethod
    def from_log_line(cls, line: str) -> "WALEntry":
        parts = line.strip().split("|", 6)
        
        if len(parts) < 6:
            raise ValueError(f"Invalid WAL entry format: {line}")
        
        log_sequence_number = int(parts[0])
        timestamp = parts[1]

        operation = DatabaseOperationsEnum(parts[2])
        status = WALStatus(parts[3])

        table = parts[4]

        data = json.loads(parts[5]) if parts[5] else {}
        query = parts[6] if len(parts) > 6 else ""
        
        return cls(
            log_sequence_number=log_sequence_number,
            timestamp=timestamp,
            operation=operation,
            status=status,
            table=table,
            data=data,
            query=query,
        )


@dataclass
class CheckpointEntry:
    log_sequence_number: int
    timestamp: str
    
    last_committed_log_sequence_number: int
    
    def to_log_line(self) -> str:
        return f"CHECKPOINT|{self.log_sequence_number}|{self.timestamp}|{self.last_committed_log_sequence_number}\n"
    
    @classmethod
    def from_log_line(cls, line: str) -> "CheckpointEntry":
        parts = line.strip().split("|")
        if parts[0] != "CHECKPOINT":
            raise ValueError(f"Not a checkpoint entry: {line}")
        
        return cls(
            log_sequence_number=int(parts[1]),
            timestamp=parts[2],
            last_committed_log_sequence_number=int(parts[3]),
        )
