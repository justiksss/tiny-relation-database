import asyncio
from pathlib import Path

from src.authorization import DatabaseAuthorizer
from src.engine.executor import QueryExecutor
from src.engine.table_storage import TableStorage
from src.engine.wal.recovery import WALRecovery
from src.engine.wal.writer import WALManager
from src.logger import logger
from src.server import DatabaseServer

BASE_DIR = Path(__file__).resolve().parent.parent


async def run_recovery(wal_path: str, storage: TableStorage) -> None:
    """
    Run WAL recovery to restore database state.
    
    Called on server startup to ensure consistency.
    """
    logger.info("[Startup] Checking for WAL recovery...")
    
    recovery = WALRecovery(wal_path=wal_path, storage=storage)
    
    is_valid, issues = recovery.verify_integrity()
    
    if not is_valid:
        logger.warning(f"[Startup] WAL integrity issues found:")
        for issue in issues:
            logger.warning(f"  - {issue}")
    
    # Run recovery
    recovered_count = await recovery.recover(from_checkpoint=True)
    
    if recovered_count > 0:
        logger.success(f"[Startup] Recovered {recovered_count} operations from WAL")
    else:
        logger.info("[Startup] No recovery needed")


async def database_server(
    host: str = "127.0.0.1",
    port: int = 54321,
    recovery_enabled: bool = True,
) -> None:
    wal_path = str(BASE_DIR / "data" / "wal.log")
    db_path = str(BASE_DIR / "database")
    
    storage = TableStorage(base_directory=db_path)
    
    if recovery_enabled:
        await run_recovery(wal_path=wal_path, storage=storage)
    
    wal_manager = WALManager(
        wal_path=wal_path,
        batch_size=5,
        flush_interval=0.5,
    )
    
    executor = QueryExecutor(storage=storage)
    
    server = DatabaseServer(
        host=host,
        port=port,
        authorizer=DatabaseAuthorizer(),
        wal_manager=wal_manager,
        executor=executor,
    )
    
    logger.info(f"[Startup] Starting database server on {host}:{port}")
    logger.info(f"[Startup] WAL path: {wal_path}")
    logger.info(f"[Startup] Database path: {db_path}")
    
    await asyncio.gather(
        wal_manager.start(),
        server.start(),
    )


if __name__ == "__main__":
    asyncio.run(database_server())
