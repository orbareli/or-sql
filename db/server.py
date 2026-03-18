# server.py — runs your DB as a server
import socket
import json
import sys
sys.path.insert(0, r"C:\Users\or\Desktop\or\or-sql\db")

from catalog import Catalog
from executor import Executor

catalog  = Catalog(r"C:\Users\or\Desktop\or\or-sql\db\db_files")
executor = Executor(catalog)

server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.bind(("localhost", 5555))
server.listen(5)
print("OrSQL server listening on port 5555...")

while True:
    conn, addr = server.accept()
    data = conn.recv(4096).decode()
    
    try:
        result = executor.run(data)
        response = json.dumps({"result": result, "error": None})
    except Exception as e:
        response = json.dumps({"result": None, "error": str(e)})
    
    conn.sendall(response.encode())
    conn.close()