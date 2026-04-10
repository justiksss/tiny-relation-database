import json
from urllib.parse import urlparse

import bcrypt

from src.exceptions.connection import ConnectionStringException
from src.logger import logger
from src.models.client import Client


class DatabaseAuthorizer:
    def __init__(self) -> None:
        self.schema_prefix = "fsdb://"

        self.mock_user_database_path = "/Users/justiksss/Projects/PET_PROJECTS/tiny_relation_database/mocks/users.json"

        self.default_port = 54321

    def load_users(self) -> dict[str, str]:
        try:
            with open(self.mock_user_database_path, "r") as file:
                data = json.load(file)

                return {user["username"]: user["password_hash"] for user in data}

        except FileNotFoundError:
            logger.error("No users.json found")
            return {}

    def add_user(self, username: str, password: str):
        password_hash = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()
        users = []
        try:
            with open(self.mock_user_database_path, "r") as f:
                users = json.load(f)
        except FileNotFoundError:
            pass

        users.append({"username": username, "password_hash": password_hash})

        with open(self.mock_user_database_path, "w") as f:
            json.dump(users, f, indent=2)

        logger.info(f"User '{username}' added.")

    def parse_connection_string(self, connection_string: str) -> Client:
        if not connection_string.startswith(self.schema_prefix):
            logger.error(f"Invalid connection string (missing prefix): {connection_string}")
            raise ConnectionStringException("Connection string must start with fsdb://")

        parsed = urlparse(connection_string)

        if not all([parsed.hostname, parsed.username, parsed.password]):
            logger.error(f"Incomplete connection string: {connection_string}")
            raise ConnectionStringException("Connection string missing username, password, or host")

        client = Client(
            host=parsed.hostname,
            port=parsed.port or self.default_port,
            username=parsed.username,
            password=parsed.password,
            connection_string=connection_string,
        )

        logger.debug(f"Parsed connection string: {client}")
        return client

    def authorize_client(self, connection_string: str) -> bool:
        client = self.parse_connection_string(connection_string=connection_string)

        users = self.load_users()
        user_hash = users.get(client.username)

        if user_hash and bcrypt.checkpw(client.password.encode(), user_hash.encode()):
            logger.info(f"User '{client.username}' authorized successfully")
            return True

        logger.warning(f"Failed authorization attempt for user '{client.username}'")
        return False
