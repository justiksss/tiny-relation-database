from dataclasses import dataclass


@dataclass
class TokenType:
    name: str
    regex: str


@dataclass
class Token:
    type: TokenType
    text: str

    position: int


TOKEN_TYPES_LIST: dict[str, TokenType] = {
    "ASSIGN": TokenType(name="ASSIGN", regex="дорівнює"),
    "GET": TokenType(name="GET", regex="отримати"),
    "ADD": TokenType(name="ADD", regex="додати"),
    "UPDATE": TokenType(name="UPDATE", regex="оновити"),
    "DELETE": TokenType(name="DELETE", regex="видалити"),
    "CREATE": TokenType(name="CREATE_TABLE", regex="створити"),
    "TABLE": TokenType(name="TABLE", regex="таблицю"),
    "WHERE": TokenType(name="WHERE", regex="де"),
    "SET": TokenType(name="SET", regex="встановити"),
    "VALUE": TokenType(name="VALUE", regex="значення"),
    "FROM": TokenType(name="FROM", regex="з"),
    "VARIABLE": TokenType(name="VARIABLE", regex=r"[A-Za-zА-Яа-яІіЇїЄєҐґ_][A-Za-zА-Яа-яІіЇїЄєҐґ0-9_]*"),
    "NUMBER": TokenType(name="NUMBER", regex="[0-9]+"),
    "STRING": TokenType(name="STRING", regex='"[^"]*"'),
    "SPACE": TokenType(name="SPACE", regex="[ \n\t\r]+"),
    "COMMA": TokenType(name="COMMA", regex=","),
    "LPAR": TokenType(name="LPAR", regex="\\("),
    "RPAR": TokenType(name="RPAR", regex="\\)"),
}
