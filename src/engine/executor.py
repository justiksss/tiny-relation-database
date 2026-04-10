import json

from src.engine.table_storage import TableStorage
from src.models.command import (
    BaseCommand,
    GetCommand,
    AddCommand,
    UpdateCommand,
    DeleteCommand,
    CreateTableCommand,
)
from src.models.enums.operations import DatabaseOperationsEnum
from src.sql.parser import Parser


class QueryExecutor:
    def __init__(self, storage: TableStorage) -> None:
        self.storage = storage

        self._command_handlers: dict[type, callable] = {
            CreateTableCommand: self._handle_create,
            AddCommand: self._handle_add,
            GetCommand: self._handle_get,
            UpdateCommand: self._handle_update,
            DeleteCommand: self._handle_delete,
        }

        self._operation_mapping: dict[type, DatabaseOperationsEnum] = {
            GetCommand: DatabaseOperationsEnum.GET,
            AddCommand: DatabaseOperationsEnum.ADD,
            UpdateCommand: DatabaseOperationsEnum.UPDATE,
            DeleteCommand: DatabaseOperationsEnum.DELETE,
            CreateTableCommand: DatabaseOperationsEnum.CREATE_TABLE,
        }

    def parse(self, query_text: str) -> BaseCommand:
        parser = Parser(query_text)
        return parser.parse()

    def get_operation_type(self, command: BaseCommand) -> DatabaseOperationsEnum:
        operation = self._operation_mapping.get(type(command))

        if operation is None:
            raise ValueError(f"Unknown command type: {type(command)}")

        return operation

    async def execute(self, command: BaseCommand) -> str:
        handler = self._command_handlers.get(type(command))

        if handler is None:
            raise ValueError(f"Unknown command: {type(command)}")

        return await handler(command)

    async def _handle_create(self, command: CreateTableCommand) -> str:
        await self.storage.create_table(command)
        return f"Table '{command.table}' created successfully"

    async def _handle_add(self, command: AddCommand) -> str:
        await self.storage.add(command)
        return f"Row added to '{command.table}'"

    async def _handle_get(self, command: GetCommand) -> str:
        rows = await self.storage.get(command)

        if not rows:
            return "No rows found"

        return json.dumps(rows, ensure_ascii=False)

    async def _handle_update(self, command: UpdateCommand) -> str:
        await self.storage.update(command)
        return f"Rows updated in '{command.table}'"

    async def _handle_delete(self, command: DeleteCommand) -> str:
        await self.storage.delete(command)
        return f"Rows deleted from '{command.table}'"

