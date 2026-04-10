import pytest
from src.sql.lexer import Lexer


def test_lexer_single_tokens():
    lexer = Lexer("5")
    tokens = lexer.lex_analysis()

    assert len(tokens) == 1
    assert tokens[0].type.name == "NUMBER"
    assert tokens[0].text == "5"


def test_lexer_word_variable():
    lexer = Lexer("ідентифікатор")
    tokens = lexer.lex_analysis()

    assert len(tokens) == 1
    assert tokens[0].type.name == "VARIABLE"
    assert tokens[0].text == "ідентифікатор"


def test_lexer_assignment():
    lexer = Lexer("дорівнює")
    tokens = lexer.lex_analysis()

    assert len(tokens) == 1
    assert tokens[0].type.name == "ASSIGN"
    assert tokens[0].text == "дорівнює"


def test_full_query_tokenization():
    query = "отримати з devices де id дорівнює 5"
    lexer = Lexer(query)
    tokens = lexer.lex_analysis()

    result = [(t.type.name, t.text) for t in tokens]

    expected = [
        ("GET", "отримати"),
        ("FROM", "з"),
        ("VARIABLE", "devices"),
        ("WHERE", "де"),
        ("VARIABLE", "id"),
        ("ASSIGN", "дорівнює"),
        ("NUMBER", "5"),
    ]

    assert result == expected


def test_lexer_spaces_are_ignored():
    lexer = Lexer("   5   ")
    tokens = lexer.lex_analysis()

    assert len(tokens) == 1
    assert tokens[0].type.name == "NUMBER"
    assert tokens[0].text == "5"


def test_unexpected_character_raises():
    lexer = Lexer("@")

    with pytest.raises(ValueError) as exc:
        lexer.lex_analysis()

    assert "unexpected character '@'" in str(exc.value)
