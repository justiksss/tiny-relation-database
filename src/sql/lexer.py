import re

from src.models.sql.token import Token, TOKEN_TYPES_LIST


class Lexer:
    def __init__(self, query: str) -> None:
        self._query = query

        self._token_list: list[Token] = list()
        self._position: int = 0

    def lex_analysis(self) -> list[Token]:
        while self._next_token():
            pass

        return [token for token in self._token_list if token.type.name != "SPACE"]

    def _next_token(self) -> bool:
        if self._position >= len(self._query):
            return False

        substring = self._query[self._position:]

        for token_type in TOKEN_TYPES_LIST.values():
            pattern = "^" + token_type.regex

            match = re.match(pattern, substring)

            if match:
                value = match.group(0)

                self._token_list.append(Token(token_type, value, self._position))
                self._position += len(value)
                return True

        raise ValueError(f"Error at position - {self._position}: unexpected character '{self._query[self._position]}'")
