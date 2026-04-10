from dataclasses import dataclass


@dataclass
class Client:
    connection_string: str

    username: str
    password: str

    host: str
    port: int
