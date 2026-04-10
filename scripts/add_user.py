import sys

from src.authorization import DatabaseAuthorizer

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python scripts/add_user.py <username> <password>")
        sys.exit(1)

    username, password = sys.argv[1], sys.argv[2]

    authorizer = DatabaseAuthorizer()
    authorizer.add_user(username, password)
