from collections.abc import Callable
from typing import TypeVar

from src.models.command import BaseCommand, AddCommand, GetCommand, DeleteCommand, UpdateCommand, CreateTableCommand
from src.models.enums.operations import DatabaseOperationsEnum
from src.models.sql.token import Token
from src.sql.lexer import Lexer

CommandT = TypeVar("CommandT", bound=BaseCommand)


class Parser:
    def __init__(self, query: str) -> None:
        self._query = query

        self.__lexer = Lexer(query)
        self._tokens: list[Token] = self.__lexer.lex_analysis()

        self._parse_callback_operations: dict[str, Callable[..., CommandT]] = {
            DatabaseOperationsEnum.GET: self._parse_get,
            DatabaseOperationsEnum.ADD: self._parse_add,
            DatabaseOperationsEnum.UPDATE: self._parse_update,
            DatabaseOperationsEnum.DELETE: self._parse_delete,
            DatabaseOperationsEnum.CREATE_TABLE: self._parse_create_table,
        }

        self._position = 0

    def _consume_type(self, types: list[str]) -> Token:
        token = self._tokens[self._position]

        if token.type.name not in types:
            raise ValueError(f"Expected type {types}, got {token.type.name}")

        self._position += 1
        return token

    def _check_type(self, type_name: str) -> bool:
        return self._position < len(self._tokens) and self._tokens[self._position].type.name == type_name

    def _parse_get(self) -> GetCommand:
        """
        Example

        "отримати коритсувачі де id дорівнює 5"

        GetCommand(table='користувачі', columns=['*'], where='id==5')
        """
        table = self._consume_type(["VARIABLE"]).text

        columns, where = ["*"], None

        if self._check_type("WHERE"):
            self._consume_type(["WHERE"])
            column = self._consume_type(["VARIABLE"]).text

            self._consume_type(["ASSIGN"])
            value = self._consume_type(["VARIABLE", "NUMBER", "STRING"]).text
            where = f"{column}=={value}"

        return GetCommand(table=table, where=where, columns=columns)

    def _parse_add(self) -> AddCommand:
        """
        Example

        `додати devices (ід, імя) значення (5, "Антон")`

         AddCommand(table='devices', columns=['ід', 'імя'], values=['5', '"Антон"'])
        """
        table = self._consume_type(["VARIABLE"]).text

        self._consume_type(["LPAR"])
        columns = []

        while not self._check_type("RPAR"):
            columns.append(self._consume_type(["VARIABLE"]).text)

            if self._check_type("COMMA"):
                self._consume_type(["COMMA"])

        self._consume_type(["RPAR"])
        self._consume_type(["VALUE"])
        self._consume_type(["LPAR"])

        values = []

        while not self._check_type("RPAR"):
            values.append(self._consume_type(["VARIABLE", "NUMBER", "STRING"]).text)

            if self._check_type("COMMA"):
                self._consume_type(["COMMA"])

        self._consume_type(["RPAR"])

        return AddCommand(table=table, columns=columns, values=values)

    def _parse_update(self) -> UpdateCommand:
        """
        Example:
            'оновити users встановити name "Антон" де id дорівнює 5'

        UpdateCommand(
            table='users',
            set_clause={'name': '"Антон"'},
            where='id==5'
        )
        """
        table = self._consume_type(["VARIABLE"]).text

        self._consume_type(["SET"])

        assignments = dict()

        while True:
            column = self._consume_type(["VARIABLE"]).text
            value = self._consume_type(["VARIABLE", "NUMBER", "STRING"]).text

            assignments[column] = value

            if self._check_type("COMMA"):
                self._consume_type(["COMMA"])
            else:
                break

        where = None

        if self._check_type("WHERE"):
            self._consume_type(["WHERE"])
            column = self._consume_type(["VARIABLE"]).text
            self._consume_type(["ASSIGN"])
            value = self._consume_type(["VARIABLE", "NUMBER", "STRING"]).text
            where = f"{column}=={value}"

        return UpdateCommand(table=table, assignments=assignments, where=where)

    def _parse_delete(self) -> DeleteCommand:
        """
        Example:
            'видалити users де id дорівнює 10'

        DeleteCommand(
            table='users',
            where='id==10'
        )
        """
        table = self._consume_type(["VARIABLE"]).text

        where = None
        if self._check_type("WHERE"):
            self._consume_type(["WHERE"])
            column = self._consume_type(["VARIABLE"]).text
            self._consume_type(["ASSIGN"])
            value = self._consume_type(["VARIABLE", "NUMBER", "STRING"]).text
            where = f"{column}=={value}"

        return DeleteCommand(table=table, where=where)

    def _parse_create_table(self) -> CreateTableCommand:
        """
        Example:
        'створити таблицю users (id число, name текст)'

        CreateTableCommand(
            table="users",
            columns={"id": "NUMBER", "name": "STRING"}
        )
        """
        self._consume_type(["TABLE"])

        table = self._consume_type(["VARIABLE"]).text
        self._consume_type(["LPAR"])

        columns: dict[str, str] = {}

        while not self._check_type("RPAR"):
            col_name = self._consume_type(["VARIABLE"]).text
            col_type = self._consume_type(["VARIABLE"]).text

            columns[col_name] = col_type

            if self._check_type("COMMA"):
                self._consume_type(["COMMA"])

        self._consume_type(["RPAR"])

        return CreateTableCommand(table=table, columns=columns)

    def parse(self) -> CommandT:
        operation = self._consume_type(
            types=[
                DatabaseOperationsEnum.GET,
                DatabaseOperationsEnum.UPDATE,
                DatabaseOperationsEnum.DELETE,
                DatabaseOperationsEnum.ADD,
                DatabaseOperationsEnum.CREATE_TABLE,
            ]
        )

        if not (callback := self._parse_callback_operations.get(operation.type.name)):
            raise ValueError(f"Unknown command: {operation.text}")

        return callback()
