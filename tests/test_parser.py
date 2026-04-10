from src.sql.parser import Parser


def test_create_table():
    query = 'створити таблицю users (id число, name текст)'
    parser = Parser(query)
    cmd = parser.parse()

    assert cmd.table == "users"
    assert cmd.columns == {"id": "число", "name": "текст"}


def test_create_table_multiple_columns():
    query = 'створити таблицю products (id число, name текст, price число)'
    parser = Parser(query)
    cmd = parser.parse()

    assert cmd.table == "products"
    assert cmd.columns == {"id": "число", "name": "текст", "price": "число"}


def test_add():
    query = 'додати users (id, name) значення (5, "Антон")'
    parser = Parser(query)
    cmd = parser.parse()

    assert cmd.table == "users"
    assert cmd.columns == ["id", "name"]
    assert cmd.values == ["5", '"Антон"']


def test_add_multiple_columns_and_values():
    query = 'додати products (id, name, price) значення (1, "Телефон", 499)'
    parser = Parser(query)
    cmd = parser.parse()

    assert cmd.table == "products"
    assert cmd.columns == ["id", "name", "price"]
    assert cmd.values == ["1", '"Телефон"', "499"]


def test_get_with_where():
    query = 'отримати users де id дорівнює 3'
    parser = Parser(query)
    cmd = parser.parse()

    assert cmd.table == "users"
    assert cmd.where == "id==3"
    assert cmd.columns == ["*"]


def test_get_without_where():
    query = "отримати користувачі"
    parser = Parser(query)
    cmd = parser.parse()

    assert cmd.table == "користувачі"
    assert cmd.columns == ["*"]
    assert cmd.where is None

def test_get_with_where_string_value():
    query = 'отримати користувачі де name дорівнює "Антон"'
    parser = Parser(query)
    cmd = parser.parse()

    assert cmd.table == "користувачі"
    assert cmd.columns == ["*"]
    assert cmd.where == 'name=="Антон"'


def test_update():
    query = 'оновити users встановити name "Богдан" де id дорівнює 7'
    parser = Parser(query)
    cmd = parser.parse()

    assert cmd.table == "users"
    assert cmd.assignments == {"name": '"Богдан"'}
    assert cmd.where == "id==7"


def test_update_simple():
    query = 'оновити users встановити name "Богдан" де id дорівнює 7'
    parser = Parser(query)
    cmd = parser.parse()

    assert cmd.table == "users"
    assert cmd.assignments == {"name": '"Богдан"'}
    assert cmd.where == "id==7"

def test_update_multiple_columns():
    query = 'оновити products встановити name "Телефон", price 599 де id дорівнює 2'
    parser = Parser(query)
    cmd = parser.parse()

    assert cmd.assignments == {"name": '"Телефон"', "price": "599"}
    assert cmd.where == "id==2"

def test_delete():
    query = 'видалити users де id дорівнює 10'
    parser = Parser(query)
    cmd = parser.parse()

    assert cmd.table == "users"
    assert cmd.where == "id==10"

def test_delete_with_where():
    query = 'видалити users де id дорівнює 5'
    parser = Parser(query)
    cmd = parser.parse()

    assert cmd.table == "users"
    assert cmd.where == "id==5"
