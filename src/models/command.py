from dataclasses import dataclass
import re

SNAKE_CASE = re.compile(r"^[A-Za-zА-Яа-яІіЇїЄєҐґ_][A-Za-zА-Яа-яІіЇїЄєҐґ0-9_]*$")


@dataclass
class BaseCommand:
    table: str

    def __post_init__(self):
        if not SNAKE_CASE.match(self.table):
            raise ValueError(f"Table name must be snake_case, got: {self.table}")


@dataclass
class CreateTableCommand(BaseCommand):
    columns: dict[str, str]


@dataclass
class AddCommand(BaseCommand):
    columns: list[str]

    values: list[str]


@dataclass
class GetCommand(BaseCommand):
    columns: list[str]
    where: str | None = None


@dataclass
class UpdateCommand(BaseCommand):
    assignments: dict[str, str]

    where: str | None = None


@dataclass
class DeleteCommand(BaseCommand):
    where: str | None = None
