import asyncio
from asyncio import StreamReader, StreamWriter

from src.authorization import DatabaseAuthorizer
from src.engine.executor import QueryExecutor
from src.engine.wal.writer import WALManager
from src.logger import logger
from src.models.command import (
    CreateTableCommand,
    AddCommand,
    UpdateCommand,
    DeleteCommand,
    GetCommand,
)


class DatabaseServer:
    def __init__(
        self,
        host: str,
        port: int,
        wal_manager: WALManager,
        authorizer: DatabaseAuthorizer,
        executor: QueryExecutor,
    ) -> None:
        self.host = host
        self.port = port

        self.authorizer = authorizer
        self.executor = executor
        self.wal_manager = wal_manager

    async def authorize_client(self, reader: StreamReader, writer: StreamWriter) -> bool:
        addr = writer.get_extra_info("peername")
        logger.info(f"[+] Client {addr} connected")

        writer.write(b"Enter connection string (fsdb://user:pass@host:port):\n")
        await writer.drain()

        connection_line = await reader.readline()

        if not connection_line:
            writer.close()
            await writer.wait_closed()
            logger.warning(f"[-] Client {addr} disconnected (no input)")
            return False

        connection_string = connection_line.decode().strip()

        authorized = self.authorizer.authorize_client(connection_string=connection_string)

        if not authorized:
            writer.write(b"[ERROR] Invalid credentials. Disconnecting...\n")
            await writer.drain()

            writer.close()
            await writer.wait_closed()

            logger.warning(f"[-] Unauthorized client {addr} disconnected")
            return False

        return True

    async def handle_client(self, reader: StreamReader, writer: StreamWriter):
        authorized_result = await self.authorize_client(reader=reader, writer=writer)

        if not authorized_result:
            return

        writer.write(b"[OK] Authorized successfully!\n")
        await writer.drain()

        addr = writer.get_extra_info("peername")

        while True:
            writer.write(b"fsdb> ")
            await writer.drain()

            query = await reader.readline()

            if not query:
                break

            query_text = query.decode().strip()

            if not query_text:
                continue

            if query_text.lower() in ("вихід", "exit", "quit"):
                writer.write(b"[OK] Goodbye!\n")
                await writer.drain()
                break

            result = await self._execute_query(query_text)

            writer.write(f"{result}\n".encode())
            await writer.drain()

        logger.info(f"[-] Client {addr} disconnected")

        writer.close()
        await writer.wait_closed()

    def _extract_wal_data(self, command) -> dict:
        if isinstance(command, CreateTableCommand):
            return {"columns": command.columns}
        
        elif isinstance(command, AddCommand):
            return {
                "columns": command.columns,
                "values": command.values,
            }
        
        elif isinstance(command, UpdateCommand):
            return {
                "assignments": command.assignments,
                "where": command.where,
            }
        
        elif isinstance(command, DeleteCommand):
            return {"where": command.where}
        
        elif isinstance(command, GetCommand):
            return {
                "columns": command.columns,
                "where": command.where,
            }
        
        return {}

    async def _execute_query(self, query_text: str) -> str:
        try:
            command = self.executor.parse(query_text)
            operation = self.executor.get_operation_type(command)
            
            wal_data = self._extract_wal_data(command)
            
            wal_entry = await self.wal_manager.log_operation(
                operation=operation,
                table=command.table,
                data=wal_data,
                query=query_text,
            )

            try:
                result = await self.executor.execute(command)
                
                await self.wal_manager.log_committed(wal_entry)
                
                return f"[OK] {result}"
            
            except Exception as exec_error:
                await self.wal_manager.log_failed(wal_entry)
                raise exec_error

        except ValueError as e:
            logger.error(f"Query error: {e}")
            return f"[ERROR] {e}"
        except Exception as e:
            logger.exception(f"Unexpected error: {e}")
            return f"[ERROR] Internal error: {e}"

    async def start(self) -> None:
        server = await asyncio.start_server(
            client_connected_cb=self.handle_client,
            host=self.host,
            port=self.port
        )

        addr = server.sockets[0].getsockname()
        logger.success(f"[*] Asyncio TCP server running on {addr}")

        async with server:
            await server.serve_forever()
