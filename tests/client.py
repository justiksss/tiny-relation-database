import asyncio
import sys
from src.logger import logger


async def send_query(reader, writer, query: str) -> str:
    await reader.readuntil(b"> ")

    writer.write(f"{query}\n".encode())
    await writer.drain()

    response = await reader.readline()
    return response.decode().strip()


async def tcp_client(connection_string: str):
    reader, writer = await asyncio.open_connection("127.0.0.1", 54321)
    logger.success("[*] Connected to server")

    prompt = await reader.readline()
    logger.info(prompt.decode().strip())

    writer.write(f"{connection_string}\n".encode())
    await writer.drain()

    response = await reader.readline()
    logger.info(response.decode().strip())

    if b"ERROR" in response:
        logger.error("Authorization failed, closing connection.")
        writer.close()
        await writer.wait_closed()
        return

    test_queries = [
        'створити таблицю products (id INTEGER, name TEXT, price INTEGER)',
        'додати products (id, name, price) значення (1, "Телефон", 999)',
        'додати products (id, name, price) значення (2, "Ноутбук", 25000)',
        'додати products (id, name, price) значення (3, "Навушники", 500)',
        'отримати products',
        'отримати products де id дорівнює 2',
        'оновити products встановити price 899 де id дорівнює 1',
        'отримати products де id дорівнює 1',
        'видалити products де id дорівнює 3',
        'отримати products',
    ]

    for query in test_queries:
        logger.info(f"\n>>> {query}")
        result = await send_query(reader, writer, query)
        logger.info(f"<<< {result}")

    await reader.readuntil(b"> ")
    writer.write(b"exit\n")
    await writer.drain()

    response = await reader.readline()
    logger.info(response.decode().strip())

    writer.close()
    await writer.wait_closed()
    logger.success("[*] Connection closed.")


def print_help():
    print("""
╔══════════════════════════════════════════════════════════════════════════════╗
║                         FSDB - Команди                                       ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  CREATE TABLE:                                                               ║
║    створити таблицю <name> (<col1> <TYPE>, <col2> <TYPE>, ...)              ║
║    Типи: INTEGER, TEXT, FLOAT, BOOLEAN                                       ║
║                                                                              ║
║  INSERT:                                                                     ║
║    додати <table> (<cols>) значення (<values>)                              ║
║                                                                              ║
║  SELECT:                                                                     ║
║    отримати <table>                                                         ║
║    отримати <table> де <col> дорівнює <value>                               ║
║                                                                              ║
║  UPDATE:                                                                     ║
║    оновити <table> встановити <col> <value> де <col> дорівнює <value>       ║
║                                                                              ║
║  DELETE:                                                                     ║
║    видалити <table> де <col> дорівнює <value>                               ║
║                                                                              ║
║  COMMANDS:                                                                   ║
║    help, ?         - показати цю довідку                                    ║
║    clear, cls      - очистити екран                                         ║
║    exit, quit, вихід - вийти                                                ║
╚══════════════════════════════════════════════════════════════════════════════╝
""")

    


async def sandbox_client(
    host: str = "127.0.0.1",
    port: int = 54321,
    connection_string: str = "fsdb://admin:123123@127.0.0.1:54321"
):
    """
    Interactive sandbox mode - write queries and see results in real-time.
    """
    
    try:
        reader, writer = await asyncio.open_connection(host, port)
        print(f"✓ Connected to {host}:{port}")
    except ConnectionRefusedError:
        print(f"✗ Cannot connect to {host}:{port}")
        print("  Make sure the server is running: python -m src.main")
        return
    except Exception as e:
        print(f"✗ Connection error: {e}")
        return

    # Authorization
    prompt = await reader.readline()
    print(f"← {prompt.decode().strip()}")

    writer.write(f"{connection_string}\n".encode())
    await writer.drain()
    print(f"→ {connection_string}")

    response = await reader.readline()
    response_text = response.decode().strip()
    
    if b"ERROR" in response:
        print(f"✗ {response_text}")
        writer.close()
        await writer.wait_closed()
        return
    
    print(f"✓ {response_text}")
    print()
    print("─" * 60)
    print()

    history = []

    try:
        while True:
            # Read server prompt
            await reader.readuntil(b"> ")
            
            # Get user input
            try:
                query = input("fsdb> ").strip()
            except EOFError:
                query = "exit"
            
            if not query:
                continue
            
            # Local commands
            if query.lower() in ("help", "?"):
                print_help()
                continue
            
            if query.lower() in ("clear", "cls"):
                print("\033[2J\033[H", end="")  # ANSI clear screen
                continue
            
            if query.lower() == "history":
                print("\n📜 History:")
                for i, cmd in enumerate(history[-20:], 1):
                    print(f"  {i}. {cmd}")
                print()
                continue
            
            # Add to history
            history.append(query)
            
            # Send query to server
            writer.write(f"{query}\n".encode())
            await writer.drain()
            
            # Read response
            response = await reader.readline()
            response_text = response.decode().strip()
            
            # Format output
            if "[OK]" in response_text:
                print(f"✓ {response_text}")
            elif "[ERROR]" in response_text:
                print(f"✗ {response_text}")
            else:
                print(f"← {response_text}")
            
            print()
            
            # Check for exit
            if query.lower() in ("exit", "quit", "вихід"):
                break

    except KeyboardInterrupt:
        print("\n\nInterrupted. Closing connection...")
        writer.write(b"exit\n")
        await writer.drain()
        try:
            await asyncio.wait_for(reader.readline(), timeout=1.0)
        except asyncio.TimeoutError:
            pass

    writer.close()
    await writer.wait_closed()
    print("✓ Connection closed. Goodbye!")


async def interactive_client(connection_string: str):
    """Legacy interactive mode."""
    await sandbox_client(connection_string=connection_string)


if __name__ == "__main__":
    connection_string = "fsdb://admin:123123@127.0.0.1:54321"
    host = "127.0.0.1"
    port = 54321

    # Parse arguments
    args = sys.argv[1:]
    
    if "-h" in args or "--help" in args:
        print("Usage: python -m tests.client [OPTIONS]")
        print()
        print("Options:")
        print("  -i, --interactive    Interactive sandbox mode (default)")
        print("  -t, --test           Run test queries")
        print("  -h, --help           Show this help")
        print("  --host HOST          Server host (default: 127.0.0.1)")
        print("  --port PORT          Server port (default: 54321)")
        print("  --connection STRING  Connection string")
        sys.exit(0)
    
    # Parse host/port
    for i, arg in enumerate(args):
        if arg == "--host" and i + 1 < len(args):
            host = args[i + 1]
        elif arg == "--port" and i + 1 < len(args):
            port = int(args[i + 1])
        elif arg == "--connection" and i + 1 < len(args):
            connection_string = args[i + 1]
    
    if "-t" in args or "--test" in args:
        # Test mode
        asyncio.run(tcp_client(connection_string))
    else:
        # Interactive sandbox mode (default)
        asyncio.run(sandbox_client(host, port, connection_string))
