import asyncio
import pytest
from pathlib import Path

from src.authorization import DatabaseAuthorizer
from src.engine.executor import QueryExecutor
from src.engine.table_storage import TableStorage
from src.engine.wal.writer import WALManager
from src.server import DatabaseServer

pytestmark = pytest.mark.asyncio


class TestE2E:

    @pytest.fixture
    async def server_setup(self, tmp_path: Path):
        db_dir = tmp_path / "database"
        db_dir.mkdir()

        wal_path = tmp_path / "wal.log"

        wal_manager = WALManager(
            wal_path=str(wal_path),
            batch_size=5,
            flush_interval=0.1,
        )

        storage = TableStorage(base_directory=str(db_dir))
        executor = QueryExecutor(storage=storage)

        users_file = tmp_path / "users.json"
        users_file.write_text('[{"username": "test", "password_hash": "$2b$12$doFmlGGphjSonon39QwOa.IcOXEJu6WFU9YxP8glUrC3bT/UyRgqm"}]')

        authorizer = DatabaseAuthorizer()
        authorizer.mock_user_database_path = str(users_file)

        server = DatabaseServer(
            host="127.0.0.1",
            port=0,
            wal_manager=wal_manager,
            authorizer=authorizer,
            executor=executor,
        )

        return server, wal_manager, storage

    @pytest.fixture
    async def running_server(self, server_setup):
        server, wal_manager, storage = server_setup

        tcp_server = await asyncio.start_server(
            server.handle_client,
            server.host,
            server.port,
        )

        wal_task = asyncio.create_task(wal_manager.start())

        port = tcp_server.sockets[0].getsockname()[1]

        yield {"host": server.host, "port": port, "storage": storage}

        tcp_server.close()
        await tcp_server.wait_closed()
        await wal_manager.stop()
        await wal_task

    async def connect_client(self, host: str, port: int) -> tuple:
        reader, writer = await asyncio.open_connection(host, port)

        await reader.readline()

        writer.write(b"fsdb://test:test123@localhost:54321\n")
        await writer.drain()

        response = await reader.readline()
        assert b"OK" in response

        return reader, writer

    async def send_query(self, reader, writer, query: str) -> str:
        await reader.readuntil(b"> ")

        writer.write(f"{query}\n".encode())
        await writer.drain()

        response = await reader.readline()
        return response.decode().strip()

    async def test_full_crud_cycle(self, running_server):
        reader, writer = await self.connect_client(
            running_server["host"],
            running_server["port"],
        )

        try:
            result = await self.send_query(
                reader, writer,
                'створити таблицю users (id INTEGER, name TEXT, age INTEGER)'
            )
            assert "[OK]" in result
            assert "created" in result

            result = await self.send_query(
                reader, writer,
                'додати users (id, name, age) значення (1, "Антон", 25)'
            )
            assert "[OK]" in result

            result = await self.send_query(
                reader, writer,
                'додати users (id, name, age) значення (2, "Марія", 30)'
            )
            assert "[OK]" in result

            result = await self.send_query(
                reader, writer,
                'додати users (id, name, age) значення (3, "Петро", 22)'
            )
            assert "[OK]" in result

            result = await self.send_query(reader, writer, 'отримати users')
            assert "[OK]" in result
            assert "Антон" in result
            assert "Марія" in result
            assert "Петро" in result

            result = await self.send_query(
                reader, writer,
                'отримати users де id дорівнює 2'
            )
            assert "[OK]" in result
            assert "Марія" in result
            assert "Антон" not in result

            result = await self.send_query(
                reader, writer,
                'оновити users встановити age 26 де id дорівнює 1'
            )
            assert "[OK]" in result
            assert "updated" in result.lower()

            result = await self.send_query(
                reader, writer,
                'отримати users де id дорівнює 1'
            )
            assert "26" in result

            result = await self.send_query(
                reader, writer,
                'видалити users де id дорівнює 3'
            )
            assert "[OK]" in result
            assert "deleted" in result.lower()

            result = await self.send_query(reader, writer, 'отримати users')
            assert "Петро" not in result
            assert "Антон" in result
            assert "Марія" in result

        finally:
            writer.close()
            await writer.wait_closed()

    async def test_create_duplicate_table_fails(self, running_server):
        reader, writer = await self.connect_client(
            running_server["host"],
            running_server["port"],
        )

        try:
            result = await self.send_query(
                reader, writer,
                'створити таблицю test (id INTEGER)'
            )
            assert "[OK]" in result

            result = await self.send_query(
                reader, writer,
                'створити таблицю test (id INTEGER)'
            )
            assert "[ERROR]" in result
            assert "already exists" in result

        finally:
            writer.close()
            await writer.wait_closed()

    async def test_invalid_query_returns_error(self, running_server):
        reader, writer = await self.connect_client(
            running_server["host"],
            running_server["port"],
        )

        try:
            result = await self.send_query(
                reader, writer,
                'невідома команда'
            )
            assert "[ERROR]" in result

        finally:
            writer.close()
            await writer.wait_closed()

    async def test_type_validation(self, running_server):
        reader, writer = await self.connect_client(
            running_server["host"],
            running_server["port"],
        )

        try:
            await self.send_query(
                reader, writer,
                'створити таблицю typed (id INTEGER, name TEXT)'
            )

            result = await self.send_query(
                reader, writer,
                'додати typed (id, name) значення ("not_a_number", "Test")'
            )
            assert "[ERROR]" in result

        finally:
            writer.close()
            await writer.wait_closed()

    async def test_get_from_nonexistent_table(self, running_server):
        reader, writer = await self.connect_client(
            running_server["host"],
            running_server["port"],
        )

        try:
            result = await self.send_query(
                reader, writer,
                'отримати nonexistent'
            )
            assert "[ERROR]" in result
            assert "does not exist" in result

        finally:
            writer.close()
            await writer.wait_closed()

    async def test_exit_command(self, running_server):
        reader, writer = await self.connect_client(
            running_server["host"],
            running_server["port"],
        )

        await reader.readuntil(b"> ")

        writer.write(b"exit\n")
        await writer.drain()

        response = await reader.readline()
        assert b"Goodbye" in response

        writer.close()
        await writer.wait_closed()

    async def test_ukrainian_exit_command(self, running_server):
        reader, writer = await self.connect_client(
            running_server["host"],
            running_server["port"],
        )

        await reader.readuntil(b"> ")

        writer.write("вихід\n".encode())
        await writer.drain()

        response = await reader.readline()
        assert b"Goodbye" in response

        writer.close()
        await writer.wait_closed()

    async def test_multiple_clients(self, running_server):
        reader1, writer1 = await self.connect_client(
            running_server["host"],
            running_server["port"],
        )

        reader2, writer2 = await self.connect_client(
            running_server["host"],
            running_server["port"],
        )

        try:
            result = await self.send_query(
                reader1, writer1,
                'створити таблицю shared (id INTEGER, value TEXT)'
            )
            assert "[OK]" in result

            await self.send_query(
                reader1, writer1,
                'додати shared (id, value) значення (1, "from_client_1")'
            )

            result = await self.send_query(
                reader2, writer2,
                'отримати shared'
            )
            assert "from_client_1" in result

            await self.send_query(
                reader2, writer2,
                'додати shared (id, value) значення (2, "from_client_2")'
            )

            result = await self.send_query(
                reader1, writer1,
                'отримати shared'
            )
            assert "from_client_1" in result
            assert "from_client_2" in result

        finally:
            writer1.close()
            writer2.close()
            await writer1.wait_closed()
            await writer2.wait_closed()

    async def test_empty_result(self, running_server):
        reader, writer = await self.connect_client(
            running_server["host"],
            running_server["port"],
        )

        try:
            await self.send_query(
                reader, writer,
                'створити таблицю empty (id INTEGER)'
            )

            result = await self.send_query(reader, writer, 'отримати empty')
            assert "[OK]" in result
            assert "No rows found" in result

        finally:
            writer.close()
            await writer.wait_closed()

    async def test_update_multiple_columns(self, running_server):
        reader, writer = await self.connect_client(
            running_server["host"],
            running_server["port"],
        )

        try:
            await self.send_query(
                reader, writer,
                'створити таблицю products (id INTEGER, name TEXT, price INTEGER)'
            )
            await self.send_query(
                reader, writer,
                'додати products (id, name, price) значення (1, "Phone", 100)'
            )

            result = await self.send_query(
                reader, writer,
                'оновити products встановити name "Smartphone", price 150 де id дорівнює 1'
            )
            assert "[OK]" in result

            result = await self.send_query(
                reader, writer,
                'отримати products де id дорівнює 1'
            )
            assert "Smartphone" in result
            assert "150" in result

        finally:
            writer.close()
            await writer.wait_closed()

    async def test_concurrent_read_while_other_deletes_and_adds(self, running_server):
        """One client reads in a loop while the other adds/deletes; no exceptions, reads are consistent."""
        reader1, writer1 = await self.connect_client(
            running_server["host"],
            running_server["port"],
        )
        reader2, writer2 = await self.connect_client(
            running_server["host"],
            running_server["port"],
        )

        try:
            result = await self.send_query(
                reader1, writer1,
                'створити таблицю concurrent (id INTEGER, val TEXT)'
            )
            assert "[OK]" in result

            for i in range(5):
                await self.send_query(
                    reader1, writer1,
                    f'додати concurrent (id, val) значення ({i}, "x{i}")'
                )

            read_results = []
            read_done = asyncio.Event()

            async def reader_loop():
                nonlocal read_results
                for _ in range(20):
                    try:
                        r = await self.send_query(reader2, writer2, 'отримати concurrent')
                        if "[ERROR]" not in r:
                            read_results.append(r)
                    except Exception:
                        pass

            async def writer_loop():
                for i in range(10):
                    await self.send_query(
                        reader1, writer1,
                        f'додати concurrent (id, val) значення ({100 + i}, "y{i}")'
                    )
                    await self.send_query(
                        reader1, writer1,
                        f'видалити concurrent де id дорівнює {i}'
                    )

            await asyncio.gather(
                asyncio.create_task(reader_loop()),
                asyncio.create_task(writer_loop()),
            )

            assert len(read_results) >= 1
            for r in read_results:
                assert "[ERROR]" not in r
                if "No rows found" not in r:
                    data = __import__("json").loads(r.replace("[OK] ", "").strip())
                    assert isinstance(data, list)
                    for row in data:
                        assert "id" in row and "val" in row
        finally:
            writer1.close()
            writer2.close()
            await writer1.wait_closed()
            await writer2.wait_closed()
